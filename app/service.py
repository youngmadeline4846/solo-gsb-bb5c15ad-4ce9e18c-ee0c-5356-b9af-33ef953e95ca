"""事件归段核心逻辑（仅依赖标准库，便于独立测试）。

归段规则：
- 同一 source 下，事件 occurred_at 与某事件段的 [start, end] 相距不超过
  silence 秒（含段外前后各 silence 的扩展窗口）即归入该段；
- 乱序/迟到事件可能同时落在多个段的扩展窗口内，或桥接相邻段，
  此时把这些段合并为一段（链式传导，直到不再扩大）；
- (source, event_id) 唯一，重复提交直接返回原段，不改变任何计数；
- revision 初始为 1，仅当新事件导致段的起止边界或最高级别发生变化时递增，
  仅事件数增加不递增。

变更流（供下游增量同步）：
- 每次创建段、边界或最高级别变化（revision 递增）、多段合并，都在事件写入
  事务内向 segment_changes 追加记录，change_id 单调递增；
- 合并时为每个被删除段写一条指向保留段的 merged 墓碑；
- 仅事件数增加（revision 不变）不写 upsert；event_id 幂等重放不写任何记录。

误报事件撤回（withdraw_event，与写入同级别单事务）：
- 删除指定 (source, event_id) 事件后，按当前静默间隔对原段的剩余事件重新切分；
- 无剩余事件（唯一事件）→ 删除原段并写一条 deleted 墓碑；
- 桥接事件移除导致拆段时，最早子段沿用原段 id（revision 在原值上 +1），
  其余子段新建（revision=1），各写一条 upsert；未拆段时同样 +1 并写 upsert；
- 所有删除/更新/新建与变更流追加在同一事务内，任一步失败由调用方整体回滚。

重分段预演（preview_resegmentation，纯读）：
- 在同一只读事务内读取某 source 全部原始事件，按 (occurred_at, event_id)
  稳定排序后以候选静默间隔重新切分，得到不落库的模拟段与临时编号；
- 依据事件成员关系给出每个现有段到模拟段的 unchanged/split/merged 映射；
- 不写任何表（调用方开启 query_only 双重保险），结果对重复请求确定一致。

事件审计查询（list_events，纯读）：
- 在调用方提供的同一只读快照内按 (occurred_at, event_id) 稳定排序返回事件，
  支持 source 必填、起止时间与 severity 多值过滤；
- 键集分页游标 after/next_after 为不透明字符串，编码上一页末行的
  (occurred_at, event_id) 复合位置；空页回显入参 after，游标保持不变。

事件审计时间桶统计（event_metrics，纯读）：
- 在调用方提供的同一只读快照内读取 [start_ts, end_ts) 左闭右开窗口内
  指定 source 的事件，按 occurred_at 划入等宽时间桶（左闭右开，桶边界
  对齐 start_ts），返回每桶各 canonical severity 计数、事件总数及涉及的
  不同 segment_id 数；无事件桶同样输出（计数为 0），末桶可短于桶宽；
- 非法时间范围（end <= start）、非正/非有限桶长或桶数超过上限抛
  ValueError（调用方回 422）；仅执行 SELECT，不改写任何表。

事件段严重性序列模式指纹（segment_patterns，纯读）：
- 在调用方提供的同一只读快照内扫描当前事件（可选 source 与
  [start_ts, end_ts] 闭区间过滤），按事件当前 segment_id 归属分组成段，
  段内按 (occurred_at, event_id) 稳定升序取出 canonical severity 序列；
- 对每个段序列用可配置的连续窗口长度 window_length 取全部长度为该值的
  连续子序列，规范化为指纹（canonical 级别序列的稳定哈希）；按指纹分组，
  返回出现次数、涉及的 segment_id/source 及首次/最近发生时间
  （分别为窗口首末事件的发生时间）；结果对重复请求逐字节确定；
- window_length 非正整数或非有限时间范围抛 ValueError（调用方回 422）；
  仅执行 SELECT，不改写任何表。

变更水位历史快照（get_segment_snapshot，纯读）：
- 在调用方提供的同一只读事务内，以 segment_changes 为唯一事实来源重建
  指定变更水位（at_change_id）时刻的段状态：每个 segment_id 取不晚于水位的
  最后一条变更，upsert 保留其快照，merged/deleted 墓碑移除该段；
- 结果按 segment_id 升序、键集分页；游标绑定水位与 source，
  跨条件复用或伪造游标由调用方返回 422；
- 请求水位超过当前最大 change_id 时按当前最大水位截断并返回实际水位，
  游标固定该水位，后续页据此校验一致性；
- 仅执行 SELECT，不改写任何表或变更流。

人工确认审计（acknowledge_segment / list_acknowledgements）：
- 在调用方事务（BEGIN IMMEDIATE）内先按 (segment_id, idempotency_key) 查重：
  相同键 + 相同载荷（operator/note/expected_revision 完全一致）直接返回原记录，
  不再校验段的当前状态（即使段已变化）；相同键 + 不同载荷报载荷冲突；
- 新键则校验段存在且 expected_revision 等于段当前 revision，
  通过后追加一条不可变确认记录（确认序号 id 单调递增，仅追加、永不更新）；
- 确认不改变段的 revision、不写变更流；段后续合并/删除不影响已落库的审计记录。

维护窗口抑制（create_maintenance_window / list_maintenance_window_events）：
- 维护窗口为 [start_ts, end_ts) 左闭右开、source 精确匹配；多个窗口允许
  互相重叠，互不去重、互不影响；窗口写入在调用方事务（BEGIN IMMEDIATE）内
  单条 INSERT 完成，失败由调用方整体回滚；end_ts <= start_ts 抛 ValueError
  （调用方回 422）；窗口表不与事件/段建外键，窗口永不改变事件与段；
- 覆盖事件查询在调用方提供的同一只读快照内完成：取窗口（不存在返回 None，
  调用方回 404），联读 events 与 segments，事件按
  (occurred_at, event_id) 升序、按事件主键去重（重叠窗口对每个窗口也只
  返回一次）；按事件当前 segment_id 实时归属分两类——当前段整体落在
  [start_ts, end_ts) 内为 fully，段还延伸到窗口之外为 partially。
"""
from __future__ import annotations

import base64
import hashlib
import json
import math
import sqlite3
import time
from datetime import datetime, timedelta, timezone

from . import severity as sev


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def _row_to_segment(row: sqlite3.Row) -> dict:
    return {
        "id": row["id"],
        "source": row["source"],
        "started_at": _iso(row["start_ts"]),
        "ended_at": _iso(row["end_ts"]),
        "max_severity": row["max_severity"],
        "event_count": row["event_count"],
        "revision": row["revision"],
    }


def get_segment(conn: sqlite3.Connection, segment_id: int) -> dict | None:
    row = conn.execute(
        "SELECT * FROM segments WHERE id = ?", (segment_id,)
    ).fetchone()
    return _row_to_segment(row) if row else None


def list_segments(conn: sqlite3.Connection, source: str | None = None) -> list[dict]:
    if source is None:
        rows = conn.execute("SELECT * FROM segments ORDER BY id").fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM segments WHERE source = ? ORDER BY id", (source,)
        ).fetchall()
    return [_row_to_segment(r) for r in rows]


def encode_event_cursor(occurred_ts: float, event_id: str) -> str:
    """把 (occurred_at, event_id) 复合排序位置编码为不透明分页游标（base64url JSON）。"""
    raw = json.dumps(
        [occurred_ts, event_id], separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def decode_event_cursor(cursor: str) -> tuple[float, str]:
    """解码游标为 (occurred_ts, event_id)；游标非法时抛 ValueError（调用方回 422）。"""
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        raw = base64.urlsafe_b64decode(padded.encode("ascii"))
        value = json.loads(raw)
    except (ValueError, UnicodeDecodeError) as exc:
        raise ValueError("非法 after 游标") from exc
    if (
        not isinstance(value, list)
        or len(value) != 2
        or isinstance(value[0], bool)
        or not isinstance(value[0], (int, float))
        or not isinstance(value[1], str)
        or not math.isfinite(float(value[0]))
    ):
        raise ValueError("非法 after 游标")
    return float(value[0]), value[1]


def _row_to_event(row: sqlite3.Row) -> dict:
    return {
        "source": row["source"],
        "event_id": row["event_id"],
        "occurred_at": _iso(row["occurred_at"]),
        "severity": row["severity"],
        "segment_id": row["segment_id"],
    }


def list_events(
    conn: sqlite3.Connection,
    *,
    source: str,
    start_ts: float | None = None,
    end_ts: float | None = None,
    severities: list[str] | None = None,
    after: str | None = None,
    limit: int = 100,
) -> dict:
    """事件审计查询：同一只读快照内按 (occurred_at, event_id) 升序返回一页。

    - source 必填；start_ts/end_ts 为可选闭区间边界（epoch 秒）；
    - severities 为已归一化的级别多值过滤（None 或空列表表示不过滤）；
    - after 为上一页 next_after 不透明游标（None 表示从头开始），键集分页条件为
      (occurred_at, event_id) 严格大于游标位置，不漏读、不重复；
    - 返回 {"events": [...], "next_after": str}；空页时 next_after 回显入参 after。

    调用方应使用 read_only_session：本函数只执行 SELECT，多次访问共享同一快照。
    """
    clauses = ["source = ?"]
    params: list = [source]
    if start_ts is not None:
        clauses.append("occurred_at >= ?")
        params.append(start_ts)
    if end_ts is not None:
        clauses.append("occurred_at <= ?")
        params.append(end_ts)
    if severities:
        clauses.append(f"severity IN ({','.join('?' * len(severities))})")
        params.extend(severities)
    if after is not None:
        cursor_ts, cursor_event_id = decode_event_cursor(after)
        # 复合键严格大于：(occurred_at > ts) OR (occurred_at = ts AND event_id > eid)
        clauses.append(
            "(occurred_at > ? OR (occurred_at = ? AND event_id > ?))"
        )
        params.extend([cursor_ts, cursor_ts, cursor_event_id])

    where = " AND ".join(clauses)
    rows = conn.execute(
        f"""
        SELECT source, event_id, occurred_at, severity, segment_id
        FROM events
        WHERE {where}
        ORDER BY occurred_at, event_id
        LIMIT ?
        """,
        (*params, limit),
    ).fetchall()
    events = [_row_to_event(r) for r in rows]
    next_after = (
        encode_event_cursor(rows[-1]["occurred_at"], rows[-1]["event_id"])
        if rows
        else after
    )
    return {"events": events, "next_after": next_after}


# 时间桶统计的防御上限：避免极大窗口/极小桶长导致输出无界桶位
MAX_METRIC_BUCKETS = 10_000


def event_metrics(
    conn: sqlite3.Connection,
    *,
    source: str,
    start_ts: float,
    end_ts: float,
    bucket_seconds: float,
) -> dict:
    """事件审计时间桶统计（纯读）：等宽左闭右开桶内的级别计数/总数/不同段数。

    - 查询窗口为 [start_ts, end_ts)（左闭右开，epoch 秒），仅统计该 source；
    - 桶从 start_ts 起按 bucket_seconds 等宽划分，末桶可短于桶宽；
      无事件桶同样输出，severity_counts 固定包含五个 canonical 级别键；
    - distinct_segment_count 按查询快照中事件的当前 segment_id 实时归属统计；
    - end_ts <= start_ts、bucket_seconds 非正/非有限或桶数超过
      MAX_METRIC_BUCKETS 时抛 ValueError（调用方回 422）。

    桶序号一律以整数微秒（occurred_at 的存储精度）相对 start_ts 的跨度计算，
    避免大 epoch 基值上浮点除法把恰好落在桶边界的事件划入前一桶。

    调用方应使用 read_only_session：本函数只执行 SELECT，在同一快照内完成统计。
    """
    if not math.isfinite(start_ts) or not math.isfinite(end_ts):
        raise ValueError("非法时间范围")
    if end_ts <= start_ts:
        raise ValueError("时间范围为空：end_at 必须晚于 start_at")
    if not isinstance(bucket_seconds, (int, float)) or isinstance(
        bucket_seconds, bool
    ) or not math.isfinite(bucket_seconds) or bucket_seconds <= 0:
        raise ValueError("bucket_seconds 必须为正数")

    # 用整数微秒跨度算桶数与桶位，避免浮点除法 ceil 在整倍数边界多算一个桶
    start_dt = datetime.fromtimestamp(start_ts, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(end_ts, tz=timezone.utc)
    span_us = (end_dt - start_dt) // timedelta(microseconds=1)
    width_us = int(round(bucket_seconds * 1_000_000))
    if width_us <= 0:
        raise ValueError("bucket_seconds 过小（最小粒度为 1 微秒）")
    bucket_count = (span_us + width_us - 1) // width_us
    if bucket_count > MAX_METRIC_BUCKETS:
        raise ValueError(
            f"桶数超过上限：时间范围/桶长最多生成 {MAX_METRIC_BUCKETS} 个桶"
        )

    def _bucket_index_for(ts: float) -> int:
        delta_us = (
            datetime.fromtimestamp(ts, tz=timezone.utc) - start_dt
        ) // timedelta(microseconds=1)
        return max(delta_us // width_us, 0)

    buckets: list[dict] = [
        {
            "bucket_start": _iso(start_ts + i * bucket_seconds),
            "bucket_end": _iso(min(start_ts + (i + 1) * bucket_seconds, end_ts)),
            "severity_counts": {level: 0 for level in sev.LEVELS},
            "event_count": 0,
            "distinct_segment_count": 0,
        }
        for i in range(bucket_count)
    ]
    segment_sets: list[set[int]] = [set() for _ in range(bucket_count)]

    rows = conn.execute(
        """
        SELECT occurred_at, severity, segment_id
        FROM events
        WHERE source = ? AND occurred_at >= ? AND occurred_at < ?
        ORDER BY occurred_at, event_id
        """,
        (source, start_ts, end_ts),
    ).fetchall()
    for row in rows:
        index = _bucket_index_for(row["occurred_at"])
        bucket = buckets[index]
        bucket["severity_counts"][row["severity"]] += 1
        bucket["event_count"] += 1
        segment_sets[index].add(row["segment_id"])

    for bucket, segment_ids in zip(buckets, segment_sets):
        bucket["distinct_segment_count"] = len(segment_ids)

    return {
        "source": source,
        "start_at": _iso(start_ts),
        "end_at": _iso(end_ts),
        "bucket_seconds": float(bucket_seconds),
        "buckets": buckets,
    }


# 模式指纹连续窗口长度的防御上限：避免超长窗口造成无谓扫描
MAX_PATTERN_WINDOW_LENGTH = 1_000


def pattern_fingerprint(severities: tuple[str, ...]) -> str:
    """把一段 canonical severity 序列规范化为稳定指纹（SHA-256 hex）。

    规范化键为以   分隔的级别名（级别名取自 sev.LEVELS 固定表，
    别名/大小写已在入库时归一化），相同序列无论出现在哪个段、哪个来源
    都得到相同指纹；不同序列（含长度不同）以密码学哈希区分，碰撞可忽略。
    """
    key = " ".join(severities).encode("utf-8")
    return hashlib.sha256(key).hexdigest()


def segment_patterns(
    conn: sqlite3.Connection,
    *,
    source: str | None = None,
    start_ts: float | None = None,
    end_ts: float | None = None,
    window_length: int,
) -> dict:
    """扫描当前事件提取严重性序列模式并按指纹分组（纯读）。

    - source 为 None 时扫描全部来源，否则精确匹配该 source；
      start_ts/end_ts 为可选闭区间边界（epoch 秒，两端均含）；
    - 按事件当前 segment_id（查询快照中的实时归属）分组，组内按
      (occurred_at, event_id) 稳定升序得到 canonical severity 序列——
      同刻事件以 event_id 决定次序，重复请求结果确定；
    - 对每个段序列取全部长度 window_length 的连续子序列（滑动窗口，
      含重叠）；序列短于窗口的段不产生任何模式；
    - 按指纹分组统计：occurrence_count 为窗口出现次数（同一序列重复的
      窗口分别计数），segment_ids/sources 为涉及 id/来源的去重升序列表，
      first/last_occurred_at 分别为所有窗口首/末事件时间的最小值/最大值；
    - window_length 非正整数或超过 MAX_PATTERN_WINDOW_LENGTH、时间范围
      非有限或 start_ts > end_ts 时抛 ValueError（调用方回 422）。

    调用方应使用 read_only_session：本函数只执行 SELECT，在同一快照内
    完成扫描，多个 SELECT 共享同一读事务，不改写任何表。
    """
    if (
        isinstance(window_length, bool)
        or not isinstance(window_length, int)
        or window_length <= 0
        or window_length > MAX_PATTERN_WINDOW_LENGTH
    ):
        raise ValueError(
            f"window_length 必须为 1..{MAX_PATTERN_WINDOW_LENGTH} 的整数"
        )
    if start_ts is not None and not math.isfinite(start_ts):
        raise ValueError("非法时间范围")
    if end_ts is not None and not math.isfinite(end_ts):
        raise ValueError("非法时间范围")
    if start_ts is not None and end_ts is not None and start_ts > end_ts:
        raise ValueError("start_at 不能晚于 end_at")

    clauses: list[str] = []
    params: list = []
    if source is not None:
        clauses.append("source = ?")
        params.append(source)
    if start_ts is not None:
        clauses.append("occurred_at >= ?")
        params.append(start_ts)
    if end_ts is not None:
        clauses.append("occurred_at <= ?")
        params.append(end_ts)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    rows = conn.execute(
        f"""
        SELECT segment_id, source, event_id, occurred_at, severity
        FROM events
        {where}
        ORDER BY segment_id, occurred_at, event_id
        """,
        params,
    ).fetchall()

    # 同一读事务快照内按当前 segment_id 聚合成段；行已按
    # (segment_id, occurred_at, event_id) 排序，直接顺序分组即可
    groups: dict[int, dict] = {}
    order: list[int] = []
    for row in rows:
        segment_id = row["segment_id"]
        group = groups.get(segment_id)
        if group is None:
            group = {"source": row["source"], "severities": []}
            groups[segment_id] = group
            order.append(segment_id)
        group["severities"].append(
            (row["occurred_at"], row["event_id"], row["severity"])
        )

    # 指纹 -> 聚合状态；以窗口首次出现的 (segment_id, 起始下标) 顺序记录
    # 首次遭遇次序，最终输出仍按指纹排序，保证响应逐字节确定
    aggregates: dict[str, dict] = {}
    for segment_id in order:
        group = groups[segment_id]
        sequence = group["severities"]
        if len(sequence) < window_length:
            continue
        for i in range(len(sequence) - window_length + 1):
            window = sequence[i:i + window_length]
            severities = tuple(item[2] for item in window)
            fingerprint = pattern_fingerprint(severities)
            window_start = window[0][0]
            window_end = window[-1][0]
            agg = aggregates.get(fingerprint)
            if agg is None:
                agg = {
                    "pattern": list(severities),
                    "occurrence_count": 0,
                    "segment_ids": set(),
                    "sources": set(),
                    "first_ts": window_start,
                    "last_ts": window_end,
                }
                aggregates[fingerprint] = agg
            agg["occurrence_count"] += 1
            agg["segment_ids"].add(segment_id)
            agg["sources"].add(group["source"])
            agg["first_ts"] = min(agg["first_ts"], window_start)
            agg["last_ts"] = max(agg["last_ts"], window_end)

    patterns = [
        {
            "fingerprint": fingerprint,
            "pattern": agg["pattern"],
            "occurrence_count": agg["occurrence_count"],
            "segment_ids": sorted(agg["segment_ids"]),
            "sources": sorted(agg["sources"]),
            "first_occurred_at": _iso(agg["first_ts"]),
            "last_occurred_at": _iso(agg["last_ts"]),
        }
        for fingerprint, agg in sorted(aggregates.items())
    ]

    return {
        "source": source,
        "start_at": _iso(start_ts) if start_ts is not None else None,
        "end_at": _iso(end_ts) if end_ts is not None else None,
        "window_length": window_length,
        "scanned_event_count": len(rows),
        "scanned_segment_count": len(order),
        "patterns": patterns,
    }


def _record_change(
    conn: sqlite3.Connection,
    *,
    change_type: str,
    segment_id: int,
    now: float,
    merged_into: int | None = None,
    revision: int | None = None,
    source: str | None = None,
    start_ts: float | None = None,
    end_ts: float | None = None,
    max_severity: str | None = None,
    event_count: int | None = None,
) -> None:
    """在事件写入事务内追加一条变更记录（change_id 由 AUTOINCREMENT 保证单调递增）。"""
    conn.execute(
        """
        INSERT INTO segment_changes
            (type, segment_id, merged_into, revision, source,
             start_ts, end_ts, max_severity, event_count, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            change_type, segment_id, merged_into, revision, source,
            start_ts, end_ts, max_severity, event_count, now,
        ),
    )


def _row_to_change(row: sqlite3.Row) -> dict:
    change = {
        "change_id": row["change_id"],
        "type": row["type"],
        "segment_id": row["segment_id"],
        "merged_into": row["merged_into"],
        "revision": row["revision"],
        "segment": None,
    }
    if row["type"] == "upsert":
        change["segment"] = {
            "id": row["segment_id"],
            "source": row["source"],
            "started_at": _iso(row["start_ts"]),
            "ended_at": _iso(row["end_ts"]),
            "max_severity": row["max_severity"],
            "event_count": row["event_count"],
            "revision": row["revision"],
        }
    return change


def list_segment_changes(
    conn: sqlite3.Connection, *, after: int = 0, limit: int = 100
) -> dict:
    """按 change_id 升序返回变更流的一页：{"changes": [...], "next_after": int}。

    键集分页（change_id > after）：change_id 单调递增，分页期间新写入的变更
    只会获得更大的 change_id、出现在后续页，因此不漏读也不重复。
    next_after 可直接作为下一页的 after；空页时与入参 after 相同，便于轮询。
    """
    rows = conn.execute(
        """
        SELECT * FROM segment_changes
        WHERE change_id > ?
        ORDER BY change_id
        LIMIT ?
        """,
        (after, limit),
    ).fetchall()
    changes = [_row_to_change(r) for r in rows]
    next_after = changes[-1]["change_id"] if changes else after
    return {"changes": changes, "next_after": next_after}


def encode_snapshot_cursor(
    at_change_id: int, source: str | None, last_segment_id: int
) -> str:
    """把快照分页位置编码为不透明游标（base64url JSON）。

    游标绑定查询条件（实际水位 at_change_id 与 source 过滤）与键集位置
    （上一页末行的 segment_id），带类型标签以便与事件游标区分；
    跨条件复用或篡改在 decode 后的校验中被拒绝（调用方回 422）。
    """
    raw = json.dumps(
        ["segment-snapshot", at_change_id, source, last_segment_id],
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def decode_snapshot_cursor(cursor: str) -> tuple[int, str | None, int]:
    """解码快照游标为 (at_change_id, source, last_segment_id)；非法时抛 ValueError。"""
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        raw = base64.urlsafe_b64decode(padded.encode("ascii"))
        value = json.loads(raw)
    except (ValueError, UnicodeDecodeError) as exc:
        raise ValueError("非法 after 游标") from exc
    if (
        not isinstance(value, list)
        or len(value) != 4
        or value[0] != "segment-snapshot"
        or isinstance(value[1], bool)
        or not isinstance(value[1], int)
        or value[1] < 0
        or (value[2] is not None and not isinstance(value[2], str))
        or isinstance(value[3], bool)
        or not isinstance(value[3], int)
        or value[3] < 0
    ):
        raise ValueError("非法 after 游标")
    return value[1], value[2], value[3]


def get_segment_snapshot(
    conn: sqlite3.Connection,
    *,
    at_change_id: int,
    source: str | None = None,
    after: str | None = None,
    limit: int = 100,
) -> dict:
    """按变更水位重建段历史快照的一页（纯读）。

    - 以 segment_changes 为唯一事实来源：每个 segment_id 取 change_id 不大于
      水位的最后一条变更，upsert 保留其快照，merged/deleted 墓碑移除该段；
    - 请求水位超过当前最大 change_id 时按当前最大水位截断，响应的
      at_change_id 为实际生效水位；变更流为空时水位为 0（快照为空）；
    - 结果按 segment_id 升序，键集分页（segment_id 严格大于游标位置）；
      after 游标绑定水位与 source，不一致抛 ValueError（调用方回 422）；
    - 返回 {"segments": [...], "at_change_id": int, "next_after": str|None}；
      空页时 next_after 回显入参 after，游标保持不变。

    调用方应使用 read_only_session：本函数只执行 SELECT，多次访问共享同一快照。
    """
    row = conn.execute(
        "SELECT MAX(change_id) AS max_id FROM segment_changes"
    ).fetchone()
    current_max = row["max_id"] if row["max_id"] is not None else 0
    watermark = min(at_change_id, current_max)  # 超出当前水位时截断

    last_segment_id = 0
    if after is not None:
        cursor_at, cursor_source, last_segment_id = decode_snapshot_cursor(after)
        if cursor_at != watermark or cursor_source != source:
            raise ValueError("after 游标与查询条件（at_change_id/source）不匹配")

    rows = conn.execute(
        """
        WITH latest AS (
            SELECT segment_id, MAX(change_id) AS change_id
            FROM segment_changes
            WHERE change_id <= ?
            GROUP BY segment_id
        )
        SELECT c.segment_id, c.source, c.start_ts, c.end_ts,
               c.max_severity, c.event_count, c.revision
        FROM segment_changes c
        JOIN latest
          ON latest.segment_id = c.segment_id AND latest.change_id = c.change_id
        WHERE c.type = 'upsert'
          AND (? IS NULL OR c.source = ?)
          AND c.segment_id > ?
        ORDER BY c.segment_id
        LIMIT ?
        """,
        (watermark, source, source, last_segment_id, limit),
    ).fetchall()
    segments = [
        {
            "id": r["segment_id"],
            "source": r["source"],
            "started_at": _iso(r["start_ts"]),
            "ended_at": _iso(r["end_ts"]),
            "max_severity": r["max_severity"],
            "event_count": r["event_count"],
            "revision": r["revision"],
        }
        for r in rows
    ]
    next_after = (
        encode_snapshot_cursor(watermark, source, rows[-1]["segment_id"])
        if rows
        else after
    )
    return {
        "segments": segments,
        "at_change_id": watermark,
        "next_after": next_after,
    }


def _row_to_acknowledgement(row: sqlite3.Row) -> dict:
    return {
        "ack_id": row["id"],
        "segment_id": row["segment_id"],
        "operator": row["operator"],
        "note": row["note"],
        "expected_revision": row["expected_revision"],
        "segment_revision": row["segment_revision"],
        "idempotency_key": row["idempotency_key"],
        "created_at": _iso(row["created_at"]),
    }


def acknowledge_segment(
    conn: sqlite3.Connection,
    *,
    segment_id: int,
    operator: str,
    note: str,
    expected_revision: int,
    idempotency_key: str,
) -> dict:
    """为事件段追加一条人工确认审计记录（不可变、仅追加）。

    调用方需持有事务（建议 BEGIN IMMEDIATE）并保证串行化。返回
    {"outcome": <结果>, ...}，outcome 取值与附加字段：

    - "created"：revision 校验通过，已追加记录，"acknowledgement" 为新记录；
    - "replayed"：幂等键重放且载荷一致，"acknowledgement" 为**原记录**——
      不再校验段的当前状态，即使段已变化（甚至被删除）也原样返回；
    - "payload_conflict"：幂等键已存在但载荷不同（调用方回 409）；
    - "not_found"：段不存在（调用方回 404）；
    - "revision_conflict"：expected_revision 与段当前 revision 不一致，
      "segment" 为当前段快照（调用方回 409 并携带快照）。

    幂等键查重先于段状态校验：重放语义不依赖段的实时状态；
    确认记录仅追加、不可变，不改变段的 revision，也不写变更流。
    """
    existing = conn.execute(
        """
        SELECT * FROM segment_acknowledgements
        WHERE segment_id = ? AND idempotency_key = ?
        """,
        (segment_id, idempotency_key),
    ).fetchone()
    if existing is not None:
        same_payload = (
            existing["operator"] == operator
            and existing["note"] == note
            and existing["expected_revision"] == expected_revision
        )
        if same_payload:
            return {
                "outcome": "replayed",
                "acknowledgement": _row_to_acknowledgement(existing),
            }
        return {"outcome": "payload_conflict"}

    segment = conn.execute(
        "SELECT * FROM segments WHERE id = ?", (segment_id,)
    ).fetchone()
    if segment is None:
        return {"outcome": "not_found"}
    if segment["revision"] != expected_revision:
        return {
            "outcome": "revision_conflict",
            "segment": _row_to_segment(segment),
        }

    cur = conn.execute(
        """
        INSERT INTO segment_acknowledgements
            (segment_id, operator, note, expected_revision, segment_revision,
             idempotency_key, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            segment_id, operator, note, expected_revision,
            segment["revision"], idempotency_key, time.time(),
        ),
    )
    row = conn.execute(
        "SELECT * FROM segment_acknowledgements WHERE id = ?",
        (cur.lastrowid,),
    ).fetchone()
    return {"outcome": "created", "acknowledgement": _row_to_acknowledgement(row)}


def list_acknowledgements(
    conn: sqlite3.Connection, segment_id: int
) -> list[dict]:
    """按确认序号（id）升序返回该段的全部人工确认记录（纯读）。"""
    rows = conn.execute(
        """
        SELECT * FROM segment_acknowledgements
        WHERE segment_id = ?
        ORDER BY id
        """,
        (segment_id,),
    ).fetchall()
    return [_row_to_acknowledgement(r) for r in rows]


def _find_merge_candidates(
    conn: sqlite3.Connection, source: str, occurred_ts: float, silence: float
) -> list[sqlite3.Row]:
    """找出应与新事件合并的所有段。

    新事件的初始窗口为 [occurred - silence, occurred + silence]；
    每并入一个段，窗口随该段边界继续向外扩展，循环直至收敛，
    从而正确处理一个迟到事件桥接多个段的链式情形。
    """
    lo, hi = occurred_ts - silence, occurred_ts + silence
    merged: dict[int, sqlite3.Row] = {}
    while True:
        rows = conn.execute(
            """
            SELECT * FROM segments
            WHERE source = ? AND start_ts <= ? AND end_ts >= ?
            ORDER BY id
            """,
            (source, hi, lo),
        ).fetchall()
        fresh = {r["id"]: r for r in rows if r["id"] not in merged}
        if not fresh:
            return [merged[k] for k in sorted(merged)]
        merged.update(fresh)
        lo = min([lo] + [r["start_ts"] - silence for r in fresh.values()])
        hi = max([hi] + [r["end_ts"] + silence for r in fresh.values()])


def ingest_event(
    conn: sqlite3.Connection,
    *,
    source: str,
    event_id: str,
    occurred_ts: float,
    severity: str,
    silence: float,
) -> dict:
    """写入一个事件并返回 {"deduplicated": bool, "segment": {...}}。

    调用方需持有事务（建议 BEGIN IMMEDIATE）并保证串行化。
    """
    now = time.time()

    dup = conn.execute(
        "SELECT segment_id FROM events WHERE source = ? AND event_id = ?",
        (source, event_id),
    ).fetchone()
    if dup:
        return {"deduplicated": True, "segment": get_segment(conn, dup["segment_id"])}

    candidates = _find_merge_candidates(conn, source, occurred_ts, silence)

    if not candidates:
        cur = conn.execute(
            """
            INSERT INTO segments
                (source, start_ts, end_ts, max_severity, event_count, revision,
                 created_at, updated_at)
            VALUES (?, ?, ?, ?, 1, 1, ?, ?)
            """,
            (source, occurred_ts, occurred_ts, severity, now, now),
        )
        segment_id = cur.lastrowid
        _record_change(
            conn,
            change_type="upsert",
            segment_id=segment_id,
            now=now,
            revision=1,
            source=source,
            start_ts=occurred_ts,
            end_ts=occurred_ts,
            max_severity=severity,
            event_count=1,
        )
    else:
        survivor = candidates[0]  # id 最小者作为保留段
        segment_id = survivor["id"]

        start_ts = min([occurred_ts] + [c["start_ts"] for c in candidates])
        end_ts = max([occurred_ts] + [c["end_ts"] for c in candidates])
        max_severity = sev.highest(severity, *[c["max_severity"] for c in candidates])
        event_count = 1 + sum(c["event_count"] for c in candidates)

        changed = (
            start_ts != survivor["start_ts"]
            or end_ts != survivor["end_ts"]
            or max_severity != survivor["max_severity"]
        )
        revision = max(c["revision"] for c in candidates) + (1 if changed else 0)

        others = [c["id"] for c in candidates[1:]]
        if others:
            marks = ",".join("?" * len(others))
            conn.execute(
                f"UPDATE events SET segment_id = ? WHERE segment_id IN ({marks})",
                [segment_id, *others],
            )
            conn.execute(f"DELETE FROM segments WHERE id IN ({marks})", others)
            # 每个被删除段一条 merged 墓碑，指向保留段
            for deleted_id in others:
                _record_change(
                    conn,
                    change_type="merged",
                    segment_id=deleted_id,
                    merged_into=segment_id,
                    now=now,
                )

        conn.execute(
            """
            UPDATE segments
            SET start_ts = ?, end_ts = ?, max_severity = ?,
                event_count = ?, revision = ?, updated_at = ?
            WHERE id = ?
            """,
            (start_ts, end_ts, max_severity, event_count, revision, now, segment_id),
        )
        if changed:
            # 边界或最高级别变化（revision 递增）才写 upsert；
            # 仅事件数增加时不产生变更记录
            _record_change(
                conn,
                change_type="upsert",
                segment_id=segment_id,
                now=now,
                revision=revision,
                source=source,
                start_ts=start_ts,
                end_ts=end_ts,
                max_severity=max_severity,
                event_count=event_count,
            )

    conn.execute(
        """
        INSERT INTO events (segment_id, source, event_id, occurred_at, severity, received_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (segment_id, source, event_id, occurred_ts, severity, now),
    )
    return {"deduplicated": False, "segment": get_segment(conn, segment_id)}


def preview_resegmentation(
    conn: sqlite3.Connection, *, source: str, silence: float
) -> dict:
    """按候选静默间隔对某 source 的全部原始事件重新归段（纯读预演）。

    - 仅执行 SELECT：不写 events / segments / segment_changes；调用方还应使用
      query_only 的只读事务（见 db.read_only_session）双重保证；
    - 重新归段等价于把事件按 (occurred_at, event_id) 稳定排序后，在相邻事件
      间隔大于 silence 处切分——这正是增量归段规则收敛后的最终分段；
    - 排序与临时编号均为确定性行为，相同输入重复请求结果逐字节一致；
    - 映射依据事件成员关系给出：现有段与模拟段一一对应为 unchanged，
      一对多为 split，多对一为 merged。
    """
    # 同一读事务内一次性读取该来源全部原始事件；Python 侧按
    # (occurred_at, event_id) 稳定排序，event_id 作为同刻事件的确定次序
    rows = conn.execute(
        """
        SELECT event_id, occurred_at, severity, segment_id
        FROM events
        WHERE source = ?
        """,
        (source,),
    ).fetchall()
    events = sorted(rows, key=lambda r: (r["occurred_at"], r["event_id"]))

    # ---- 按候选间隔重算模拟段 ----
    simulated: list[dict] = []
    # 事件 event_id -> 所属模拟段 temp_id（event_id 在同一 source 内唯一）
    event_sim_index: dict[str, int] = {}

    for event in events:
        ts = event["occurred_at"]
        if simulated and ts <= simulated[-1]["end_ts"] + silence:
            # 与上一个模拟段相邻（含恰好等于间隔的边界）→ 归入
            seg = simulated[-1]
            seg["end_ts"] = ts
            seg["max_severity"] = sev.highest(seg["max_severity"], event["severity"])
            seg["event_count"] += 1
        else:
            # 与上一段间隔超过 silence → 开新段；temp_id 从 1 起按时间序编号
            simulated.append(
                {
                    "temp_id": len(simulated) + 1,
                    "source": source,
                    "start_ts": ts,
                    "end_ts": ts,
                    "max_severity": event["severity"],
                    "event_count": 1,
                }
            )
        event_sim_index[event["event_id"]] = simulated[-1]["temp_id"]

    # ---- 依据事件成员关系构建现有段 -> 模拟段映射（同一读事务内读取）----
    existing_rows = conn.execute(
        "SELECT id FROM segments WHERE source = ? ORDER BY id", (source,)
    ).fetchall()
    existing_ids = [r["id"] for r in existing_rows]

    # 模拟段 temp_id -> 含多少个不同现有段（用于反向判定 merged）
    sim_to_existing: dict[int, set[int]] = {s["temp_id"]: set() for s in simulated}
    # 现有段 id -> 覆盖到的模拟段 temp_id 有序集合
    existing_to_sims: dict[int, list[int]] = {sid: [] for sid in existing_ids}
    existing_counts: dict[int, int] = {sid: 0 for sid in existing_ids}

    for event in events:
        sid = event["segment_id"]
        temp_id = event_sim_index[event["event_id"]]
        sim_to_existing[temp_id].add(sid)
        if sid in existing_to_sims and (
            not existing_to_sims[sid] or existing_to_sims[sid][-1] != temp_id
        ):
            # 事件已按时间排序，落入的模拟段 temp_id 单调不减，去重追加即可
            existing_to_sims[sid].append(temp_id)
        existing_counts[sid] = existing_counts.get(sid, 0) + 1

    mappings: list[dict] = []
    split_count = merged_count = unchanged_count = 0
    for sid in existing_ids:  # 按现有段 id 升序，保证输出确定
        temp_ids = existing_to_sims.get(sid, [])
        if len(temp_ids) == 1 and len(sim_to_existing[temp_ids[0]]) == 1:
            relation = "unchanged"
            unchanged_count += 1
        elif len(temp_ids) > 1:
            relation = "split"
            split_count += 1
        else:
            relation = "merged"
            merged_count += 1
        mappings.append(
            {
                "segment_id": sid,
                "relation": relation,
                "simulated_temp_ids": temp_ids,
                "event_count": existing_counts.get(sid, 0),
            }
        )

    return {
        "source": source,
        "silence_interval_seconds": silence,
        "event_count": len(events),
        "simulated_segments": [
            {
                "temp_id": s["temp_id"],
                "source": s["source"],
                "started_at": _iso(s["start_ts"]),
                "ended_at": _iso(s["end_ts"]),
                "max_severity": s["max_severity"],
                "event_count": s["event_count"],
            }
            for s in simulated
        ],
        "mappings": mappings,
        "summary": {
            "changed_existing_segment_count": split_count + merged_count,
            "split_existing_segment_count": split_count,
            "merged_existing_segment_count": merged_count,
            "unchanged_existing_segment_count": unchanged_count,
            "existing_segment_count": len(existing_ids),
            "simulated_segment_count": len(simulated),
        },
    }


def withdraw_event(
    conn: sqlite3.Connection,
    *,
    source: str,
    event_id: str,
    silence: float,
) -> dict | None:
    """撤回一个误报事件并按静默间隔重算其原段；事件不存在时返回 None（调用方回 404）。

    调用方需持有事务（建议 BEGIN IMMEDIATE）并保证串行化；本函数的所有删除、
    更新、新建与变更流追加均在该事务内执行，异常向上抛出由调用方整体回滚。

    - 唯一事件移除（原段清空）：删除原段，写一条 deleted 墓碑；
    - 未拆段：原段边界/最高级别重算，沿用原 id，revision 一律 +1，写 upsert；
    - 桥接事件移除导致拆段：剩余事件按时间序切分，最早子段沿用原 id
      （revision 在原值上 +1），其余子段新建（revision=1），各写一条 upsert。
      change_id 顺序：保留段 upsert 在前，新建子段按时间序在后。
    """
    now = time.time()

    event = conn.execute(
        "SELECT segment_id, occurred_at, severity FROM events "
        "WHERE source = ? AND event_id = ?",
        (source, event_id),
    ).fetchone()
    if event is None:
        return None

    old_segment_id = event["segment_id"]
    old = conn.execute(
        "SELECT * FROM segments WHERE id = ?", (old_segment_id,)
    ).fetchone()
    if old is None:  # 理论不可达（外键），仍显式处理
        return None

    # 原段其余事件（排除被撤回事件），按时间序稳定排序后重切分
    remaining = conn.execute(
        """
        SELECT event_id, occurred_at, severity
        FROM events
        WHERE segment_id = ? AND NOT (source = ? AND event_id = ?)
        ORDER BY occurred_at, event_id
        """,
        (old_segment_id, source, event_id),
    ).fetchall()

    if not remaining:
        # 唯一事件移除：删事件 + 删原段，原段写 deleted 墓碑
        conn.execute(
            "DELETE FROM events WHERE source = ? AND event_id = ?",
            (source, event_id),
        )
        conn.execute("DELETE FROM segments WHERE id = ?", (old_segment_id,))
        _record_change(
            conn,
            change_type="deleted",
            segment_id=old_segment_id,
            now=now,
        )
        return {
            "source": source,
            "event_id": event_id,
            "result": "deleted_segment",
            "segments": [],
        }

    # 与 preview 相同的切分：相邻事件间隔 > silence 处切分
    groups: list[list[sqlite3.Row]] = []
    for row in remaining:
        if groups and row["occurred_at"] <= groups[-1][-1]["occurred_at"] + silence:
            groups[-1].append(row)
        else:
            groups.append([row])

    def _group_fields(group: list[sqlite3.Row]) -> tuple[float, float, str, int]:
        start_ts = group[0]["occurred_at"]
        end_ts = group[-1]["occurred_at"]
        max_severity = sev.highest(*[g["severity"] for g in group])
        return start_ts, end_ts, max_severity, len(group)

    # 先删除被撤回事件，再更新/新建段（同事务，任一步失败整体回滚）
    conn.execute(
        "DELETE FROM events WHERE source = ? AND event_id = ?",
        (source, event_id),
    )

    # 最早子段沿用原 id（含未拆段的唯一组），revision 在原值上 +1
    start_ts, end_ts, max_severity, event_count = _group_fields(groups[0])
    revision = old["revision"] + 1  # 撤回后保留段的 revision 必须递增
    conn.execute(
        """
        UPDATE segments
        SET start_ts = ?, end_ts = ?, max_severity = ?,
            event_count = ?, revision = ?, updated_at = ?
        WHERE id = ?
        """,
        (start_ts, end_ts, max_severity, event_count, revision, now,
         old_segment_id),
    )
    _record_change(
        conn,
        change_type="upsert",
        segment_id=old_segment_id,
        now=now,
        revision=revision,
        source=source,
        start_ts=start_ts,
        end_ts=end_ts,
        max_severity=max_severity,
        event_count=event_count,
    )
    result_segments = [get_segment(conn, old_segment_id)]

    # 拆出的其余子段：新建段（revision=1）并把事件归属改到新段；
    # 变更记录在保留段 upsert 之后按时间序追加，change_id 天然有序
    for group in groups[1:]:
        start_ts, end_ts, max_severity, event_count = _group_fields(group)
        cur = conn.execute(
            """
            INSERT INTO segments
                (source, start_ts, end_ts, max_severity, event_count, revision,
                 created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, 1, ?, ?)
            """,
            (source, start_ts, end_ts, max_severity, event_count, now, now),
        )
        new_id = cur.lastrowid
        conn.executemany(
            "UPDATE events SET segment_id = ? WHERE source = ? AND event_id = ?",
            [(new_id, source, g["event_id"]) for g in group],
        )
        _record_change(
            conn,
            change_type="upsert",
            segment_id=new_id,
            now=now,
            revision=1,
            source=source,
            start_ts=start_ts,
            end_ts=end_ts,
            max_severity=max_severity,
            event_count=event_count,
        )
        result_segments.append(get_segment(conn, new_id))

    result = "shrunk_segment" if len(groups) == 1 else "split_segment"
    return {
        "source": source,
        "event_id": event_id,
        "result": result,
        "segments": result_segments,
    }


def _row_to_maintenance_window(row: sqlite3.Row) -> dict:
    return {
        "id": row["id"],
        "source": row["source"],
        "start_at": _iso(row["start_ts"]),
        "end_at": _iso(row["end_ts"]),
        "reason": row["reason"],
        "created_at": _iso(row["created_at"]),
    }


def create_maintenance_window(
    conn: sqlite3.Connection,
    *,
    source: str,
    start_ts: float,
    end_ts: float,
    reason: str,
) -> dict:
    """在调用方事务内创建一个维护窗口并返回其完整记录。

    - 窗口为 [start_ts, end_ts) 左闭右开，source 精确匹配；窗口间允许重叠；
    - end_ts <= start_ts 时抛 ValueError（调用方回 422）；
    - 单条 INSERT 与调用方事务原子提交，异常向上抛出由调用方整体回滚；
      窗口写入不触碰事件、段与变更流。
    """
    if not math.isfinite(start_ts) or not math.isfinite(end_ts) or end_ts <= start_ts:
        raise ValueError("时间范围非法：end_at 必须晚于 start_at")
    now = time.time()
    cur = conn.execute(
        """
        INSERT INTO maintenance_windows
            (source, start_ts, end_ts, reason, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (source, start_ts, end_ts, reason, now),
    )
    row = conn.execute(
        "SELECT * FROM maintenance_windows WHERE id = ?", (cur.lastrowid,)
    ).fetchone()
    return _row_to_maintenance_window(row)


def get_maintenance_window(conn: sqlite3.Connection, window_id: int) -> dict | None:
    """按 id 查询维护窗口；不存在返回 None（调用方回 404）。"""
    row = conn.execute(
        "SELECT * FROM maintenance_windows WHERE id = ?", (window_id,)
    ).fetchone()
    return _row_to_maintenance_window(row) if row else None


def list_maintenance_window_events(
    conn: sqlite3.Connection, window_id: int
) -> dict | None:
    """单一只读快照内返回维护窗口覆盖的事件及事件当前 segment_id（纯读）。

    - 窗口不存在返回 None（调用方回 404）；
    - source 精确匹配；事件 occurred_at 落在 [start_ts, end_ts) 左闭右开
      窗口内（恰好等于 end_ts 不算覆盖）；
    - 按事件**当前所在段**（events.segment_id JOIN segments 实时归属）划分：
      - fully：当前段整体在窗口内（segment.start_ts >= start_ts 且
        segment.end_ts < end_ts）；
      - partially：当前段还包含窗口之外的事件（段起点早于 start_ts，或
        段终点不早于 end_ts）；
    - 两类均按 (occurred_at, event_id) 升序；按事件主键唯一，命中多个
      重叠窗口的事件对每个窗口也只出现一次（本函数本就只查一个窗口）；
    - 仅执行 SELECT，不改写任何表；调用方应使用 read_only_session。
    """
    window_row = conn.execute(
        "SELECT * FROM maintenance_windows WHERE id = ?", (window_id,)
    ).fetchone()
    if window_row is None:
        return None

    start_ts = window_row["start_ts"]
    end_ts = window_row["end_ts"]
    rows = conn.execute(
        """
        SELECT e.source       AS source,
               e.event_id     AS event_id,
               e.occurred_at  AS occurred_at,
               e.severity     AS severity,
               e.segment_id   AS segment_id,
               s.start_ts     AS seg_start_ts,
               s.end_ts       AS seg_end_ts
        FROM events AS e
        JOIN segments AS s ON s.id = e.segment_id
        WHERE e.source = ?
          AND e.occurred_at >= ?
          AND e.occurred_at < ?
        ORDER BY e.occurred_at, e.event_id
        """,
        (window_row["source"], start_ts, end_ts),
    ).fetchall()

    fully: list[dict] = []
    partially: list[dict] = []
    for row in rows:
        event = {
            "source": row["source"],
            "event_id": row["event_id"],
            "occurred_at": _iso(row["occurred_at"]),
            "severity": row["severity"],
            "segment_id": row["segment_id"],
        }
        # 当前段完全被窗口包含 → fully；否则段还延伸到窗口之外 → partially
        if row["seg_start_ts"] >= start_ts and row["seg_end_ts"] < end_ts:
            fully.append(event)
        else:
            partially.append(event)

    return {
        "window": _row_to_maintenance_window(window_row),
        "fully": fully,
        "partially": partially,
    }
