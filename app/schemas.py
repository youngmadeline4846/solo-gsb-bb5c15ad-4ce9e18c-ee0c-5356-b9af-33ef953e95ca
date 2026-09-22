"""API 请求/响应模型。"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, Field, field_validator

from . import severity as sev


class EventIn(BaseModel):
    source: str = Field(min_length=1, max_length=256, examples=["edge-gateway-01"])
    event_id: str = Field(min_length=1, max_length=256, examples=["evt-0001"])
    occurred_at: datetime = Field(examples=["2026-09-20T08:00:00Z"])
    severity: str = Field(examples=["warning"])

    @field_validator("severity")
    @classmethod
    def _normalize_severity(cls, v: str) -> str:
        try:
            return sev.normalize(v)
        except ValueError as exc:
            raise ValueError(str(exc)) from exc

    @field_validator("occurred_at")
    @classmethod
    def _assume_utc(cls, v: datetime) -> datetime:
        # 无时区的时间按 UTC 处理
        return v.replace(tzinfo=timezone.utc) if v.tzinfo is None else v


class SegmentOut(BaseModel):
    id: int
    source: str
    started_at: datetime
    ended_at: datetime
    max_severity: str
    event_count: int
    revision: int


class IngestResponse(BaseModel):
    deduplicated: bool
    segment: SegmentOut


class EventOut(BaseModel):
    """事件审计视图：事件四要素 + 当前所属 segment_id。

    segment_id 为查询时刻的归属（撤回/合并重算后可能变化），随快照实时返回。
    """

    source: str
    event_id: str
    occurred_at: datetime
    severity: str
    segment_id: int


class EventsPage(BaseModel):
    """事件审计一页；next_after 为不透明游标，作为下一页的 after 继续拉取。

    空页时 next_after 与入参 after 相同（首页无 after 时为 null），游标保持不变。
    """

    events: list[EventOut]
    next_after: str | None = None


class SeverityCounts(BaseModel):
    """一个时间桶内各 canonical 级别的事件计数（级别固定为五个键，缺省为 0）。"""

    debug: int = 0
    info: int = 0
    warning: int = 0
    error: int = 0
    critical: int = 0


class EventMetricsBucket(BaseModel):
    """一个左闭右开时间桶 [bucket_start, bucket_end) 的统计。

    - severity_counts：五个 canonical 级别各自的事件数；
    - event_count：桶内事件总数（等于 severity_counts 五项之和）；
    - distinct_segment_count：桶内事件当前涉及的不同 segment_id 数
      （按查询快照中的实时归属统计）；
    - 无事件的桶也会输出，各计数为 0；末个桶可能短于 bucket_seconds。
    """

    bucket_start: datetime
    bucket_end: datetime
    severity_counts: SeverityCounts
    event_count: int
    distinct_segment_count: int


class EventMetricsResponse(BaseModel):
    """事件审计时间桶统计；buckets 按时间升序，覆盖 [start_at, end_at) 全部桶位。"""

    source: str
    start_at: datetime
    end_at: datetime
    bucket_seconds: float
    buckets: list[EventMetricsBucket]


class SegmentPatternOut(BaseModel):
    """一个严重性序列模式的分组聚合结果。

    - fingerprint：规范化模式指纹（canonical severity 序列的稳定哈希），
      相同序列（含长度与各级别次序）在任意段/来源间指纹一致；
    - pattern：指纹对应的 canonical 级别序列（按发生时间从早到晚）；
    - occurrence_count：该模式作为连续窗口出现的总次数，同一序列中
      重叠重复出现分别计数；
    - segment_ids / sources：出现该模式的不同 segment_id / source（升序）；
    - first_occurred_at / last_occurred_at：各次出现窗口首/末事件
      发生时间的最早/最晚值（窗口跨度由 window_length 决定）。
    """

    fingerprint: str
    pattern: list[str]
    occurrence_count: int
    segment_ids: list[int]
    sources: list[str]
    first_occurred_at: datetime
    last_occurred_at: datetime


class SegmentPatternsResponse(BaseModel):
    """严重性序列模式扫描结果（单一只读快照，纯读，不改任何表）。

    扫描指定 source（可选）与闭区间时间窗 [start_at, end_at]（两端均含）
    内的当前事件，按事件当前 segment_id 分组、段内按 (occurred_at,
    event_id) 稳定升序提取 severity 序列，以长度 window_length 的连续
    滑动窗口生成规范化指纹并分组。patterns 按指纹升序，重复请求确定一致；
    短于 window_length 的段不产生模式；没有匹配事件时 patterns 为空。
    """

    source: str | None
    start_at: datetime | None
    end_at: datetime | None
    window_length: int
    scanned_event_count: int
    scanned_segment_count: int
    patterns: list[SegmentPatternOut]


class WithdrawResponse(BaseModel):
    """误报事件撤回结果。

    - ``deleted_segment``：原段仅含该事件，段随之删除，``segments`` 为空；
    - ``shrunk_segment``：移除后原段未拆开（边界/最高级别可能重算），
      保留段沿用原 id 且 revision 递增，``segments`` 含 1 个段；
    - ``split_segment``：桥接事件移除导致原段拆成多段，最早子段沿用原 id
      （revision 递增），其余子段新建（revision=1），按时间序全部返回。
    """

    source: str
    event_id: str
    result: Literal["deleted_segment", "shrunk_segment", "split_segment"]
    segments: list[SegmentOut]


class SegmentChange(BaseModel):
    """一条段变更记录。

    - upsert：段创建或边界/最高级别变化，segment 为变更时刻的完整快照；
    - merged：墓碑，segment_id 段已被删除并并入 merged_into 指向的保留段；
    - deleted：墓碑，segment_id 段因其唯一事件被撤回而删除（无并入目标）。
    """

    change_id: int
    type: Literal["upsert", "merged", "deleted"]
    segment_id: int
    merged_into: int | None = None
    revision: int | None = None
    segment: SegmentOut | None = None


class SegmentChangesPage(BaseModel):
    """变更流一页；next_after 作为下一页的 after 继续拉取。"""

    changes: list[SegmentChange]
    next_after: int


class SegmentSnapshotPage(BaseModel):
    """按变更水位重建的段历史快照一页。

    - segments：水位时刻仍存在的段（upsert 快照），按 segment_id 升序；
    - at_change_id：实际生效水位——请求水位超过当前最大 change_id 时被截断，
      后续页须以该水位继续；
    - next_after：绑定水位与 source 的不透明游标，作为下一页的 after；
      空页时与入参 after 相同（首页无 after 时为 null），游标保持不变。
    """

    segments: list[SegmentOut]
    at_change_id: int
    next_after: str | None = None


class PreviewSegmentOut(BaseModel):
    """按候选静默间隔重算出的模拟段；temp_id 仅本次预演响应内有效。"""

    temp_id: int
    source: str
    started_at: datetime
    ended_at: datetime
    max_severity: str
    event_count: int


class SegmentMappingOut(BaseModel):
    """单个现有段到模拟段的映射，依据事件成员关系判定：

    - unchanged：该现有段的事件与唯一一个模拟段完全相同（一对一）；
    - split：该现有段的事件分散到多个模拟段（一对多，候选间隔更小）；
    - merged：多个现有段的事件并入同一个模拟段（多对一，候选间隔更大）。
    """

    segment_id: int
    relation: Literal["unchanged", "split", "merged"]
    simulated_temp_ids: list[int]
    event_count: int


class ResegmentationSummary(BaseModel):
    changed_existing_segment_count: int
    split_existing_segment_count: int
    merged_existing_segment_count: int
    unchanged_existing_segment_count: int
    existing_segment_count: int
    simulated_segment_count: int


class ResegmentationPreviewResponse(BaseModel):
    """静默间隔重分段预演结果：纯读，不改变任何事件、现有段与变更流。"""

    source: str
    silence_interval_seconds: float
    event_count: int
    simulated_segments: list[PreviewSegmentOut]
    mappings: list[SegmentMappingOut]
    summary: ResegmentationSummary


class AcknowledgementIn(BaseModel):
    """人工确认请求：校验 expected_revision 后追加不可变审计记录。

    idempotency_key 在段内唯一：相同键 + 相同载荷重放返回原记录，
    相同键 + 不同载荷返回 409。
    """

    operator: str = Field(min_length=1, max_length=256, examples=["ops-alice"])
    note: str = Field(max_length=2000, examples=["已确认，通知值班处理"])
    expected_revision: int = Field(ge=1, examples=[2])
    idempotency_key: str = Field(
        min_length=1, max_length=256, examples=["ack-20260921-0001"]
    )


class AcknowledgementOut(BaseModel):
    """一条人工确认审计记录（不可变）；ack_id 为单调递增的确认序号。

    segment_revision 为确认落库时刻段的实际 revision（与校验通过的
    expected_revision 一致），此后段如何变化都不影响该记录。
    """

    ack_id: int
    segment_id: int
    operator: str
    note: str
    expected_revision: int
    segment_revision: int
    idempotency_key: str
    created_at: datetime


class MaintenanceWindowIn(BaseModel):
    """创建维护窗口：source 精确匹配，窗口为 [start_at, end_at) 左闭右开。

    无时区的时间按 UTC 处理；start_at 必须早于 end_at，否则返回 422。
    """

    source: str = Field(min_length=1, max_length=256, examples=["edge-gateway-01"])
    start_at: datetime = Field(examples=["2026-09-20T08:00:00Z"])
    end_at: datetime = Field(examples=["2026-09-20T09:00:00Z"])
    reason: str = Field(max_length=2000, examples=["机房电源检修"])

    @field_validator("start_at", "end_at")
    @classmethod
    def _assume_utc(cls, v: datetime) -> datetime:
        # 无时区的时间按 UTC 处理
        return v.replace(tzinfo=timezone.utc) if v.tzinfo is None else v


class MaintenanceWindowOut(BaseModel):
    """一条维护窗口；start_at 含、end_at 不含。"""

    id: int
    source: str
    start_at: datetime
    end_at: datetime
    reason: str
    created_at: datetime


class MaintenanceWindowEventOut(BaseModel):
    """维护窗口覆盖的事件：事件四要素 + 查询时刻当前 segment_id。"""

    source: str
    event_id: str
    occurred_at: datetime
    severity: str
    segment_id: int


class MaintenanceWindowEventsResponse(BaseModel):
    """单个维护窗口覆盖事件的查询结果（单一只读快照，纯读）。

    - fully：事件**当前所在段**整体落在窗口 [start_at, end_at) 内
      （segment.start >= window.start 且 segment.end < window.end）的覆盖事件；
    - partially：事件在窗口内，但其当前段还包含窗口之外的事件；
    - 两类均按 (occurred_at, event_id) 升序；事件按 events 主键唯一，
      即使被多个重叠窗口命中，对每个窗口也只返回一次。
    """

    window: MaintenanceWindowOut
    fully: list[MaintenanceWindowEventOut]
    partially: list[MaintenanceWindowEventOut]
