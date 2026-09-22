"""FastAPI 应用入口。"""
from __future__ import annotations

import threading
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from .config import Settings
from .db import init_db, read_only_session, session
from .schemas import (
    AcknowledgementIn,
    AcknowledgementOut,
    EventIn,
    EventMetricsResponse,
    EventsPage,
    IngestResponse,
    MaintenanceWindowEventsResponse,
    MaintenanceWindowIn,
    MaintenanceWindowOut,
    ResegmentationPreviewResponse,
    SegmentChangesPage,
    SegmentDurationReportResponse,
    SegmentOut,
    SegmentPatternsResponse,
    SegmentSnapshotPage,
    WithdrawResponse,
)
from . import service
from . import severity as sev

# SQLite 单写者：进程内串行化归段写入（含撤回），配合事务保证合并/拆段一致性
_ingest_lock = threading.Lock()


def create_app(settings: Settings) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        init_db(settings.database_path)  # 启动即完成数据库初始化（幂等）
        yield

    app = FastAPI(
        title="设备告警事件段聚合服务",
        version="1.8.0",
        lifespan=lifespan,
    )

    @app.get("/healthz")
    def healthz() -> dict:
        return {"status": "ok"}

    @app.get("/events", response_model=EventsPage)
    def list_events(
        source: str = Query(min_length=1, max_length=256),
        start_at: datetime | None = Query(default=None),
        end_at: datetime | None = Query(default=None),
        severity: list[str] | None = Query(default=None),
        after: str | None = Query(default=None, min_length=1, max_length=512),
        limit: int = Query(default=100, ge=1, le=1000),
    ) -> dict:
        """事件审计查询：在**单一只读快照**内按 (occurred_at, event_id) 稳定升序分页。

        - ``source`` 必填；``start_at`` / ``end_at`` 为可选的发生时间闭区间
          （ISO 8601，无时区按 UTC 处理，边界包含）；
        - ``severity`` 可重复传入做多值过滤（``?severity=error&severity=critical``），
          大小写不敏感并接受 ``warn``/``fatal`` 别名，非法值返回 422；
        - ``after`` 为上一页返回的 ``next_after`` 不透明游标（首页不传）；
          ``limit`` 默认 100、最大 1000；
        - 每条事件返回 source、event_id、occurred_at、severity 与**当前**
          segment_id（撤回/合并重算后的实时归属）；
        - 空页时 ``next_after`` 与入参 ``after`` 相同，游标保持不变，可直接继续轮询。
        """
        if start_at is not None and start_at.tzinfo is None:
            start_at = start_at.replace(tzinfo=timezone.utc)
        if end_at is not None and end_at.tzinfo is None:
            end_at = end_at.replace(tzinfo=timezone.utc)
        if start_at is not None and end_at is not None and start_at > end_at:
            raise HTTPException(
                status_code=422, detail="start_at 不能晚于 end_at"
            )

        severities: list[str] | None = None
        if severity:
            try:
                severities = sorted({sev.normalize(s) for s in severity})
            except ValueError as exc:
                raise HTTPException(status_code=422, detail=str(exc)) from exc

        try:
            with read_only_session(settings.database_path) as conn:
                return service.list_events(
                    conn,
                    source=source,
                    start_ts=start_at.timestamp() if start_at else None,
                    end_ts=end_at.timestamp() if end_at else None,
                    severities=severities,
                    after=after,
                    limit=limit,
                )
        except ValueError as exc:  # 游标解码失败等输入错误 → 422
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.get("/events/metrics", response_model=EventMetricsResponse)
    def event_metrics(
        source: str = Query(min_length=1, max_length=256),
        start_at: datetime = Query(...),
        end_at: datetime = Query(...),
        bucket_seconds: float = Query(..., gt=0),
    ) -> dict:
        """事件审计时间桶统计：在**单一只读快照**内按 occurred_at 聚合。

        - ``source`` 必填；时间窗口为 ``[start_at, end_at)`` **左闭右开**
          （ISO 8601，无时区按 UTC 处理，``end_at`` 不包含），桶从
          ``start_at`` 起按 ``bucket_seconds`` 等宽划分（边界统一按 UTC）；
        - 每桶返回五个 canonical severity 各自计数、事件总数与桶内事件
          **当前**涉及的不同 segment_id 数；无事件桶同样输出，末桶可短于桶宽；
        - 桶按时间**升序**输出；接口纯读（query_only 连接），不改写任何表；
        - ``start_at`` 不晚于 ``end_at``、时间窗口非空、桶数不超过
          10000，否则返回 422。
        """
        if start_at.tzinfo is None:
            start_at = start_at.replace(tzinfo=timezone.utc)
        if end_at.tzinfo is None:
            end_at = end_at.replace(tzinfo=timezone.utc)
        try:
            with read_only_session(settings.database_path) as conn:
                return service.event_metrics(
                    conn,
                    source=source,
                    start_ts=start_at.timestamp(),
                    end_ts=end_at.timestamp(),
                    bucket_seconds=bucket_seconds,
                )
        except ValueError as exc:  # 空时间范围/桶数超限等输入错误 → 422
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.post("/events", response_model=IngestResponse)
    def submit_event(event: EventIn) -> dict:
        """提交告警事件；按 source + 静默间隔归段，event_id 幂等去重。"""
        with _ingest_lock, session(settings.database_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            return service.ingest_event(
                conn,
                source=event.source,
                event_id=event.event_id,
                occurred_ts=event.occurred_at.timestamp(),
                severity=event.severity,
                silence=settings.silence_interval_seconds,
            )

    @app.delete("/events/{source}/{event_id}", response_model=WithdrawResponse)
    def withdraw_event(source: str, event_id: str) -> dict:
        """撤回误报事件：单事务内删除事件并按静默间隔重算其原段。

        - 未知 (source, event_id) 返回 404；
        - 唯一事件移除 → 删除原段并写 deleted 墓碑；
        - 桥接事件移除导致拆段 → 最早子段沿用原 id（revision+1，upsert），
          其余子段新建（revision=1，upsert）；
        - 未拆段 → 保留原 id，边界/最高级别重算，revision+1，upsert；
        - 任一步失败整体回滚，事件、段与变更流均保持撤回前状态。
        """
        with _ingest_lock, session(settings.database_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            result = service.withdraw_event(
                conn,
                source=source,
                event_id=event_id,
                silence=settings.silence_interval_seconds,
            )
        if result is None:
            raise HTTPException(status_code=404, detail="event not found")
        return result

    @app.get(
        "/segments/resegmentation-preview",
        response_model=ResegmentationPreviewResponse,
    )
    def preview_resegmentation(
        source: str = Query(min_length=1, max_length=256),
        silence_interval_seconds: float = Query(ge=0),
    ) -> dict:
        """静默间隔重分段预演：用候选间隔在只读事务内重算该 source 的分段。

        全程纯读（query_only 连接 + 无任何写 SQL），不改写事件、现有段或
        变更流；返回模拟段（带临时编号）、每个现有段到模拟段的
        unchanged/split/merged 映射及受影响段汇总。
        """
        with read_only_session(settings.database_path) as conn:
            return service.preview_resegmentation(
                conn, source=source, silence=silence_interval_seconds
            )

    @app.get("/segments/snapshot", response_model=SegmentSnapshotPage)
    def get_segment_snapshot(
        at_change_id: int = Query(ge=0),
        source: str | None = Query(default=None, min_length=1, max_length=256),
        after: str | None = Query(default=None, min_length=1, max_length=512),
        limit: int = Query(default=100, ge=1, le=1000),
    ) -> dict:
        """按变更水位重建事件段历史快照（单一只读事务，纯读）。

        - ``at_change_id`` 必填：以变更流为准重建该水位时刻的段状态——
          每个 segment_id 取不晚于水位的最后一条变更，upsert 保留其快照，
          merged/deleted 墓碑移除该段；结果按 segment_id 升序；
        - 请求水位超过当前最大 change_id 时按当前最大水位截断，响应的
          ``at_change_id`` 为实际生效水位，后续页须固定该水位；
        - ``source`` 可选过滤；``after`` 为上一页 ``next_after`` 不透明游标，
          绑定水位与 source，伪造或跨条件复用返回 422；
        - 空页时 ``next_after`` 与入参 ``after`` 相同，游标保持不变；
        - 全程纯读，不改写任何表或变更流。
        """
        try:
            with read_only_session(settings.database_path) as conn:
                return service.get_segment_snapshot(
                    conn,
                    at_change_id=at_change_id,
                    source=source,
                    after=after,
                    limit=limit,
                )
        except ValueError as exc:  # 游标解码失败/条件不匹配 → 422
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.get("/segments/patterns", response_model=SegmentPatternsResponse)
    def segment_patterns(
        source: str | None = Query(default=None, min_length=1, max_length=256),
        start_at: datetime | None = Query(default=None),
        end_at: datetime | None = Query(default=None),
        window_length: int = Query(..., ge=1, le=1000),
    ) -> dict:
        """严重性序列模式指纹：扫描**当前**事件段并按连续窗口指纹分组（纯读）。

        - ``source`` 可选（精确匹配；不传则扫描全部来源），``start_at`` /
          ``end_at`` 为可选发生时间**闭区间**（两端均含，ISO 8601，无时区
          按 UTC 处理）；``window_length`` 必填，连续窗口长度 1–1000；
        - 按事件当前 ``segment_id`` 归属分组，段内按 ``(occurred_at,
          event_id)`` 稳定升序提取 canonical severity 序列（同刻事件次序
          确定），对每个长度为 ``window_length`` 的连续子序列生成规范化
          指纹（重叠窗口分别计数）；
        - 按指纹分组返回出现次数、涉及的 segment_id/source 及首次/最近
          发生时间（窗口首/末事件时间的极值）；patterns 按指纹升序；
        - 全部读取在**同一只读快照**（query_only 连接）内完成，不改任何表；
          ``start_at`` 晚于 ``end_at``、窗口长度越界或时间非法返回 422。
        """
        if start_at is not None and start_at.tzinfo is None:
            start_at = start_at.replace(tzinfo=timezone.utc)
        if end_at is not None and end_at.tzinfo is None:
            end_at = end_at.replace(tzinfo=timezone.utc)
        if start_at is not None and end_at is not None and start_at > end_at:
            raise HTTPException(
                status_code=422, detail="start_at 不能晚于 end_at"
            )
        try:
            with read_only_session(settings.database_path) as conn:
                return service.segment_patterns(
                    conn,
                    source=source,
                    start_ts=start_at.timestamp() if start_at else None,
                    end_ts=end_at.timestamp() if end_at else None,
                    window_length=window_length,
                )
        except ValueError as exc:  # 窗口长度/时间范围非法 → 422
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.get(
        "/segments/duration-report",
        response_model=SegmentDurationReportResponse,
    )
    def segment_duration_report(
        source: str | None = Query(default=None, min_length=1, max_length=256),
        start_at: datetime | None = Query(default=None),
        end_at: datetime | None = Query(default=None),
        bucket_seconds: float = Query(..., gt=0),
    ) -> dict:
        """当前事件段持续时长分布报告（**单一只读快照**，纯读）。

        - ``source`` 可选（精确匹配；不传则统计全部来源），``start_at`` /
          ``end_at`` 为可选**闭区间**边界（两端均含，ISO 8601，无时区按
          UTC 处理）：段的时间范围 [started_at, ended_at] 与该闭区间
          **有交集**即纳入统计；
        - 每段持续时长 = ended_at - started_at（秒）；返回样本数、最短/
          最长、均值、中位数与 P90（线性插值分位数）；无匹配段时样本数
          为 0，各统计量为 null、buckets 为空；
        - ``bucket_seconds`` 必填：时长桶从 0 起等宽划分（左闭右开），
          覆盖到最长时长所在的桶，空桶同样输出，桶按时长升序稳定输出；
        - 全部读取在同一 query_only 只读快照内完成，不改写任何表；
          ``start_at`` 晚于 ``end_at``、桶长非正/非数字或桶数超过
          10000 时返回 422。
        """
        if start_at is not None and start_at.tzinfo is None:
            start_at = start_at.replace(tzinfo=timezone.utc)
        if end_at is not None and end_at.tzinfo is None:
            end_at = end_at.replace(tzinfo=timezone.utc)
        if start_at is not None and end_at is not None and start_at > end_at:
            raise HTTPException(
                status_code=422, detail="start_at 不能晚于 end_at"
            )
        try:
            with read_only_session(settings.database_path) as conn:
                return service.segment_duration_report(
                    conn,
                    source=source,
                    start_ts=start_at.timestamp() if start_at else None,
                    end_ts=end_at.timestamp() if end_at else None,
                    bucket_seconds=bucket_seconds,
                )
        except ValueError as exc:  # 时间范围/桶长/桶数等输入错误 → 422
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.get("/segments/{segment_id}", response_model=SegmentOut)
    def get_segment(segment_id: int) -> dict:
        """查询事件段详情：起止时间、最高级别、事件数、revision。"""
        with session(settings.database_path) as conn:
            segment = service.get_segment(conn, segment_id)
        if segment is None:
            raise HTTPException(status_code=404, detail="segment not found")
        return segment

    @app.get("/segments", response_model=list[SegmentOut])
    def list_segments(source: str | None = Query(default=None)) -> list[dict]:
        """列出事件段，可按 source 过滤。"""
        with session(settings.database_path) as conn:
            return service.list_segments(conn, source)

    @app.post(
        "/segments/{segment_id}/acknowledgements",
        response_model=AcknowledgementOut,
    )
    def acknowledge_segment(segment_id: int, ack: AcknowledgementIn):
        """人工确认审计：校验 expected_revision 后追加不可变确认记录。

        - 在 BEGIN IMMEDIATE 事务内校验段当前 revision 后落库；
          段不存在返回 404；
        - expected_revision 与段当前 revision 不一致返回 409，
          响应体携带当前段快照（{"detail": ..., "segment": {...}}）；
        - idempotency_key 在段内幂等：相同键 + 相同载荷重放返回原记录
          （即使段此后已变化），相同键 + 不同载荷返回 409；
        - 确认记录仅追加、不可变，不改变段的 revision、不写变更流。
        """
        with _ingest_lock, session(settings.database_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            result = service.acknowledge_segment(
                conn,
                segment_id=segment_id,
                operator=ack.operator,
                note=ack.note,
                expected_revision=ack.expected_revision,
                idempotency_key=ack.idempotency_key,
            )
        outcome = result["outcome"]
        if outcome == "not_found":
            raise HTTPException(status_code=404, detail="segment not found")
        if outcome == "revision_conflict":
            segment = result["segment"]
            return JSONResponse(
                status_code=409,
                content={
                    "detail": (
                        f"expected_revision {ack.expected_revision} 与段当前 "
                        f"revision {segment['revision']} 不一致"
                    ),
                    "segment": segment,
                },
            )
        if outcome == "payload_conflict":
            raise HTTPException(
                status_code=409,
                detail="idempotency_key 已被不同载荷使用",
            )
        return result["acknowledgement"]

    @app.get(
        "/segments/{segment_id}/acknowledgements",
        response_model=list[AcknowledgementOut],
    )
    def list_acknowledgements(segment_id: int) -> list[dict]:
        """按确认序号升序列出该段的人工确认记录；段不存在返回 404。"""
        with read_only_session(settings.database_path) as conn:
            segment = service.get_segment(conn, segment_id)
            records = (
                service.list_acknowledgements(conn, segment_id)
                if segment is not None
                else None
            )
        if segment is None:
            raise HTTPException(status_code=404, detail="segment not found")
        return records

    @app.get("/segment-changes", response_model=SegmentChangesPage)
    def list_segment_changes(
        after: int = Query(default=0, ge=0),
        limit: int = Query(default=100, ge=1, le=1000),
    ) -> dict:
        """增量同步变更流：按 change_id 升序返回 upsert/merged 记录。

        用上一页的 next_after 作为下一页的 after；change_id 单调递增，
        分页期间新写入的变更不会导致漏读或重复。
        """
        with session(settings.database_path) as conn:
            return service.list_segment_changes(conn, after=after, limit=limit)

    @app.post("/maintenance-windows", response_model=MaintenanceWindowOut)
    def create_maintenance_window(window: MaintenanceWindowIn) -> dict:
        """创建维护窗口：source 精确匹配，窗口为 ``[start_at, end_at)`` 左闭右开。

        - 多个窗口允许互相重叠，互不去重、互不影响；
        - 窗口写入在 ``BEGIN IMMEDIATE`` 单事务内完成，失败整体回滚，
          窗口表独立于事件/段，创建窗口不改写任何事件、段或变更流；
        - 无时区的时间按 UTC 处理；``start_at`` 不早于 ``end_at``
          （含相等的空窗口）返回 422。
        """
        start_at = window.start_at
        end_at = window.end_at
        if start_at >= end_at:
            raise HTTPException(
                status_code=422, detail="start_at 必须早于 end_at"
            )
        try:
            with _ingest_lock, session(settings.database_path) as conn:
                conn.execute("BEGIN IMMEDIATE")
                return service.create_maintenance_window(
                    conn,
                    source=window.source,
                    start_ts=start_at.timestamp(),
                    end_ts=end_at.timestamp(),
                    reason=window.reason,
                )
        except ValueError as exc:  # 防御：非法时间范围 → 422
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.get(
        "/maintenance-windows/{window_id}/events",
        response_model=MaintenanceWindowEventsResponse,
    )
    def list_maintenance_window_events(window_id: int) -> dict:
        """查询维护窗口覆盖的事件（**单一只读快照**，纯读）。

        - ``source`` 精确匹配；事件时间落在 ``[start_at, end_at)``
          **左闭右开**窗口内（恰好等于 ``end_at`` 不返回）；
        - 每条事件返回四要素与其**当前** segment_id（撤回/合并重算后的
          实时归属）；窗口不存在返回 404；
        - 按事件当前所在段划分：``fully``（段整体在窗口内）与
          ``partially``（段还包含窗口之外的事件），两类均按事件时间
          ``(occurred_at, event_id)`` 升序；
        - 事件按主键唯一，即使被多个重叠窗口命中，对每个窗口也只返回一次。
        """
        with read_only_session(settings.database_path) as conn:
            result = service.list_maintenance_window_events(conn, window_id)
        if result is None:
            raise HTTPException(
                status_code=404, detail="maintenance window not found"
            )
        return result

    return app


app = create_app(Settings.from_env())
