"""SQLite 连接与 schema 初始化（幂等，服务启动时自动执行）。"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS segments (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    source        TEXT    NOT NULL,
    start_ts      REAL    NOT NULL,          -- 段内最早 occurred_at（epoch 秒）
    end_ts        REAL    NOT NULL,          -- 段内最晚 occurred_at（epoch 秒）
    max_severity  TEXT    NOT NULL,
    event_count   INTEGER NOT NULL,
    revision      INTEGER NOT NULL,
    created_at    REAL    NOT NULL,
    updated_at    REAL    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_segments_source ON segments (source, start_ts, end_ts);

CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    segment_id  INTEGER NOT NULL REFERENCES segments (id),
    source      TEXT    NOT NULL,
    event_id    TEXT    NOT NULL,
    occurred_at REAL    NOT NULL,            -- epoch 秒
    severity    TEXT    NOT NULL,
    received_at REAL    NOT NULL,
    UNIQUE (source, event_id)                -- 幂等去重键
);
CREATE INDEX IF NOT EXISTS idx_events_segment ON events (segment_id);
-- 事件审计查询：WHERE source = ? ... ORDER BY occurred_at, event_id
CREATE INDEX IF NOT EXISTS idx_events_source_time ON events (source, occurred_at, event_id);

-- 段变更流：供下游增量同步。与事件写入同事务追加，change_id 单调递增
CREATE TABLE IF NOT EXISTS segment_changes (
    change_id    INTEGER PRIMARY KEY AUTOINCREMENT,  -- 单调递增的变更序号（分页游标）
    type         TEXT    NOT NULL CHECK (type IN ('upsert', 'merged', 'deleted')),
    segment_id   INTEGER NOT NULL,          -- upsert: 目标段；merged/deleted: 被删除段
    merged_into  INTEGER,                   -- 仅 merged：指向保留段 id
    revision     INTEGER,                   -- 仅 upsert：变更后的 revision
    source       TEXT,                      -- 以下仅 upsert：变更时刻的段快照
    start_ts     REAL,
    end_ts       REAL,
    max_severity TEXT,
    event_count  INTEGER,
    created_at   REAL    NOT NULL
);
-- 查询模式为 WHERE change_id > ? ORDER BY change_id，主键即索引，无需额外索引

-- 人工确认审计：追加式不可变记录，确认序号 id 全局单调递增。
-- 刻意不建外键：段被合并/撤回删除后审计记录仍完整保留（审计语义），
-- 也不反向阻塞段的删除；新表由 CREATE TABLE IF NOT EXISTS 建立，
-- 已有持久化卷在启动时自动补齐，无需重建迁移
CREATE TABLE IF NOT EXISTS segment_acknowledgements (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,  -- 确认序号（单调递增）
    segment_id        INTEGER NOT NULL,
    operator          TEXT    NOT NULL,
    note              TEXT    NOT NULL,
    expected_revision INTEGER NOT NULL,          -- 请求方声明的段 revision
    segment_revision  INTEGER NOT NULL,          -- 确认落库时刻段的实际 revision
    idempotency_key   TEXT    NOT NULL,
    created_at        REAL    NOT NULL,
    UNIQUE (segment_id, idempotency_key)         -- 幂等键在段内唯一
);
-- 查询模式为 WHERE segment_id = ? ORDER BY id
CREATE INDEX IF NOT EXISTS idx_segment_ack_segment
    ON segment_acknowledgements (segment_id, id);

-- 维护窗口抑制：左闭右开 [start_ts, end_ts)，source 精确匹配。
-- 窗口可互相重叠（互不去重、互不影响）；覆盖事件在查询时按事件当前 segment_id
-- 实时计算，故窗口表不与事件/段做任何外键关联。同其他新表，CREATE TABLE IF
-- NOT EXISTS 使已有持久化卷在启动时自动补齐，无需重建迁移
CREATE TABLE IF NOT EXISTS maintenance_windows (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    source     TEXT    NOT NULL,          -- 精确匹配的事件来源（大小写敏感）
    start_ts   REAL    NOT NULL,          -- 窗口起点（含，epoch 秒）
    end_ts     REAL    NOT NULL,          -- 窗口终点（不含，epoch 秒）
    reason     TEXT    NOT NULL,          -- 维护原因
    created_at REAL    NOT NULL
);
-- 查询模式为按 source + 时间范围定位窗口
CREATE INDEX IF NOT EXISTS idx_maintenance_source_time
    ON maintenance_windows (source, start_ts, end_ts);
"""


def _migrate_segment_changes(conn: sqlite3.Connection) -> None:
    """旧库 segment_changes 的 CHECK 仅允许 upsert/merged 时重建为含 deleted 的新表。

    撤回功能新增 'deleted' 墓碑类型；CREATE TABLE IF NOT EXISTS 不会更新已有
    持久化卷上的旧 CHECK 约束，故检测旧表 SQL 并在一次性事务内重建
    （SQLite 无法直接 ALTER CHECK）。历史记录按原序保留，change_id 不变。
    """
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='segment_changes'"
    ).fetchone()
    if row is not None and "'deleted'" not in row["sql"]:
        conn.execute("ALTER TABLE segment_changes RENAME TO segment_changes_old")
        conn.executescript(SCHEMA)  # 以新 CHECK 建 segment_changes
        conn.execute(
            """
            INSERT INTO segment_changes
                (change_id, type, segment_id, merged_into, revision, source,
                 start_ts, end_ts, max_severity, event_count, created_at)
            SELECT change_id, type, segment_id, merged_into, revision, source,
                   start_ts, end_ts, max_severity, event_count, created_at
            FROM segment_changes_old
            ORDER BY change_id
            """
        )
        conn.execute("DROP TABLE segment_changes_old")


def init_db(db_path: str) -> None:
    """建表并执行幂等迁移；Docker 启动时即完成数据库初始化。"""
    with connect(db_path) as conn:
        conn.executescript(SCHEMA)
        _migrate_segment_changes(conn)


def connect(db_path: str) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


@contextmanager
def session(db_path: str):
    """事务上下文：成功提交、异常回滚、保证关闭连接。"""
    conn = connect(db_path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@contextmanager
def read_only_session(db_path: str):
    """只读事务上下文：所有读在同一快照内完成，结束以回滚释放事务。

    - 显式 BEGIN 开启延迟读事务，多次 SELECT 共享同一快照；
    - query_only=ON 使任何 INSERT/UPDATE/DELETE/DDL 直接报错，
      从机制上杜绝预演逻辑误写事件、段或变更流。
    """
    conn = connect(db_path)
    try:
        conn.execute("PRAGMA query_only=ON")
        conn.execute("BEGIN")
        yield conn
        conn.rollback()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
