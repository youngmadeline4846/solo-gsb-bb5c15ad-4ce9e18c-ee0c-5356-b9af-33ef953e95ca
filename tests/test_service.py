"""核心归段逻辑测试：仅依赖标准库，可直接运行：

    python3 -m unittest tests.test_service -v
"""
import os
import base64
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.db import init_db, read_only_session, session  # noqa: E402
from app import service  # noqa: E402

SILENCE = 10.0


class ServiceTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "test.db")
        init_db(self.db_path)

    def tearDown(self):
        self.tmp.cleanup()

    def submit(self, source, event_id, occurred_ts, severity="info"):
        with session(self.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            return service.ingest_event(
                conn,
                source=source,
                event_id=event_id,
                occurred_ts=occurred_ts,
                severity=severity,
                silence=SILENCE,
            )

    def withdraw(self, source, event_id):
        with session(self.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            return service.withdraw_event(
                conn, source=source, event_id=event_id, silence=SILENCE
            )

    def get(self, segment_id):
        with session(self.db_path) as conn:
            return service.get_segment(conn, segment_id)

    def changes(self, after=0, limit=100):
        with session(self.db_path) as conn:
            return service.list_segment_changes(conn, after=after, limit=limit)

    def test_first_event_creates_segment(self):
        r = self.submit("s1", "e1", 1000.0, "warning")
        self.assertFalse(r["deduplicated"])
        seg = r["segment"]
        self.assertEqual(seg["event_count"], 1)
        self.assertEqual(seg["revision"], 1)
        self.assertEqual(seg["max_severity"], "warning")
        self.assertEqual(seg["started_at"], seg["ended_at"])

    def test_duplicate_event_id_is_idempotent(self):
        r1 = self.submit("s1", "e1", 1000.0)
        r2 = self.submit("s1", "e1", 1000.0)
        self.assertTrue(r2["deduplicated"])
        seg = self.get(r1["segment"]["id"])
        self.assertEqual(seg["event_count"], 1)
        self.assertEqual(seg["revision"], 1)

    def test_same_event_id_different_source_not_duplicate(self):
        self.submit("s1", "e1", 1000.0)
        r = self.submit("s2", "e1", 1000.0)
        self.assertFalse(r["deduplicated"])
        self.assertEqual(r["segment"]["source"], "s2")

    def test_out_of_order_event_extends_start_and_bumps_revision(self):
        r1 = self.submit("s1", "e1", 1000.0)
        r2 = self.submit("s1", "e2", 995.0)  # 更早到达的乱序事件
        self.assertEqual(r2["segment"]["id"], r1["segment"]["id"])
        seg = r2["segment"]
        self.assertEqual(seg["event_count"], 2)
        self.assertEqual(seg["revision"], 2)  # 边界变化 → 递增
        self.assertEqual(seg["started_at"], "1970-01-01T00:16:35+00:00")  # 995.0

    def test_inner_event_lower_severity_keeps_revision(self):
        self.submit("s1", "e1", 1000.0, "error")
        r = self.submit("s1", "e2", 1010.0, "info")  # 扩展 end → rev=2
        self.assertEqual(r["segment"]["revision"], 2)
        r = self.submit("s1", "e3", 1005.0, "debug")  # 严格位于段内、级别更低
        self.assertEqual(r["segment"]["event_count"], 3)
        self.assertEqual(r["segment"]["revision"], 2)  # 边界与级别未变
        self.assertEqual(r["segment"]["max_severity"], "error")

    def test_inner_event_higher_severity_bumps_revision(self):
        self.submit("s1", "e1", 1000.0, "info")
        r = self.submit("s1", "e2", 1003.0, "critical")
        self.assertEqual(r["segment"]["revision"], 2)
        self.assertEqual(r["segment"]["max_severity"], "critical")

    def test_event_beyond_silence_creates_new_segment(self):
        r1 = self.submit("s1", "e1", 1000.0)
        r2 = self.submit("s1", "e2", 1000.0 + SILENCE + 0.1)
        self.assertNotEqual(r1["segment"]["id"], r2["segment"]["id"])

    def test_event_at_exact_silence_boundary_joins_segment(self):
        r1 = self.submit("s1", "e1", 1000.0)
        r2 = self.submit("s1", "e2", 1000.0 + SILENCE)
        self.assertEqual(r1["segment"]["id"], r2["segment"]["id"])

    def test_late_event_bridges_two_segments(self):
        r1 = self.submit("s1", "e1", 0.0)          # 段 A [0, 0]
        r2 = self.submit("s1", "e2", 11.0)         # 11 > 0+10 → 段 B [11, 11]
        self.assertNotEqual(r1["segment"]["id"], r2["segment"]["id"])
        r3 = self.submit("s1", "e3", 5.0)          # 桥接 A 与 B
        seg = r3["segment"]
        self.assertEqual(seg["id"], r1["segment"]["id"])
        self.assertEqual(seg["event_count"], 3)
        self.assertEqual(seg["revision"], 2)
        with session(self.db_path) as conn:
            self.assertEqual(len(service.list_segments(conn, "s1")), 1)

    def test_chain_merge_via_fixpoint(self):
        self.submit("s1", "e1", 0.0)               # 段 A
        self.submit("s1", "e2", 11.0)              # 段 B
        self.submit("s1", "e3", 22.0)              # 22 > 11+10 → 段 C
        self.submit("s1", "e4", 5.0)               # 合并 A+B → [0, 11]
        r = self.submit("s1", "e5", 16.0)          # 桥接 [0,11] 与 [22,22]
        seg = r["segment"]
        self.assertEqual(seg["event_count"], 5)
        with session(self.db_path) as conn:
            segs = service.list_segments(conn, "s1")
        self.assertEqual(len(segs), 1)
        self.assertTrue(segs[0]["ended_at"].startswith("1970-01-01T00:00:22"))

    def test_sources_are_isolated(self):
        self.submit("s1", "e1", 1000.0)
        r = self.submit("s2", "e2", 1001.0)  # 时间接近但 source 不同
        self.assertEqual(r["segment"]["event_count"], 1)
        with session(self.db_path) as conn:
            self.assertEqual(len(service.list_segments(conn)), 2)

    def test_revision_never_decreases_after_merge(self):
        self.submit("s1", "e1", 0.0, "info")
        self.submit("s1", "e2", 1.0, "critical")   # 段 A rev=2
        self.submit("s1", "e3", 21.0, "info")      # 21 > 1+10 → 段 B rev=1
        r = self.submit("s1", "e4", 11.0)          # 桥接 A、B
        self.assertGreaterEqual(r["segment"]["revision"], 3)

    # ---- 变更流（segment_changes）----

    def test_create_emits_upsert_change(self):
        r = self.submit("s1", "e1", 1000.0, "warning")
        page = self.changes()
        self.assertEqual(len(page["changes"]), 1)
        ch = page["changes"][0]
        self.assertEqual(ch["change_id"], 1)
        self.assertEqual(ch["type"], "upsert")
        self.assertEqual(ch["segment_id"], r["segment"]["id"])
        self.assertEqual(ch["revision"], 1)
        self.assertIsNone(ch["merged_into"])
        self.assertEqual(ch["segment"], r["segment"])  # 快照与段状态一致
        self.assertEqual(page["next_after"], 1)

    def test_boundary_or_severity_change_emits_upsert(self):
        self.submit("s1", "e1", 1000.0, "info")
        self.submit("s1", "e2", 1005.0, "error")   # 扩 end + 提级 → rev 2
        page = self.changes()
        self.assertEqual([c["type"] for c in page["changes"]], ["upsert", "upsert"])
        second = page["changes"][1]
        self.assertEqual(second["revision"], 2)
        self.assertEqual(second["segment"]["max_severity"], "error")
        self.assertEqual(second["segment"]["event_count"], 2)
        ids = [c["change_id"] for c in page["changes"]]
        self.assertLess(ids[0], ids[1])            # 单调递增

    def test_count_only_increase_emits_no_change(self):
        self.submit("s1", "e1", 1000.0, "error")
        self.submit("s1", "e2", 1010.0)            # 扩边界 → upsert
        before = self.changes()["changes"]
        self.assertEqual(len(before), 2)
        self.submit("s1", "e3", 1005.0, "debug")   # 仅事件数 +1，revision 不变
        self.assertEqual(self.changes()["changes"], before)  # 无新增变更

    def test_duplicate_event_id_emits_no_change(self):
        self.submit("s1", "e1", 1000.0)
        before = self.changes()["changes"]
        r = self.submit("s1", "e1", 1000.0)        # 幂等重放
        self.assertTrue(r["deduplicated"])
        self.assertEqual(self.changes()["changes"], before)

    def test_merge_emits_tombstone_per_deleted_segment(self):
        r1 = self.submit("s1", "e1", 0.0)          # 段 A
        r2 = self.submit("s1", "e2", 11.0)         # 段 B
        r3 = self.submit("s1", "e3", 22.0)         # 段 C
        self.submit("s1", "e4", 5.0)               # 合并 A+B
        self.submit("s1", "e5", 16.0)              # 桥接 [A+B] 与 C → 三段合并
        page = self.changes()
        types = [c["type"] for c in page["changes"]]
        # 3 次创建 + (B 墓碑 + A upsert) + (C 墓碑 + A upsert)
        self.assertEqual(
            types,
            ["upsert", "upsert", "upsert", "merged", "upsert", "merged", "upsert"],
        )
        tombs = [c for c in page["changes"] if c["type"] == "merged"]
        self.assertEqual(
            {t["segment_id"] for t in tombs},
            {r2["segment"]["id"], r3["segment"]["id"]},
        )
        for t in tombs:                            # 每个墓碑都指向保留段 A
            self.assertEqual(t["merged_into"], r1["segment"]["id"])
            self.assertIsNone(t["segment"])
        final = page["changes"][-1]
        self.assertEqual(final["type"], "upsert")
        self.assertEqual(final["segment_id"], r1["segment"]["id"])
        self.assertEqual(final["segment"]["event_count"], 5)
        ids = [c["change_id"] for c in page["changes"]]
        self.assertEqual(ids, sorted(ids))         # 全局单调递增
        self.assertEqual(len(set(ids)), len(ids))

    def test_pagination_no_miss_no_dup_while_ingesting(self):
        for i in range(5):                         # 5 个独立段 → 5 条 upsert
            self.submit("s1", f"e{i}", 1000.0 + i * 100.0)

        page1 = self.changes(after=0, limit=2)
        self.assertEqual([c["change_id"] for c in page1["changes"]], [1, 2])
        self.assertEqual(page1["next_after"], 2)

        self.submit("s1", "e5", 2000.0)            # 分页期间又有新事件

        page2 = self.changes(after=page1["next_after"], limit=2)
        page3 = self.changes(after=page2["next_after"], limit=100)
        seen = [c["change_id"] for p in (page1, page2, page3) for c in p["changes"]]
        self.assertEqual(seen, [1, 2, 3, 4, 5, 6])  # 不漏读
        self.assertEqual(len(seen), len(set(seen)))  # 不重复

        empty = self.changes(after=page3["next_after"])  # 已读完：空页
        self.assertEqual(empty["changes"], [])
        self.assertEqual(empty["next_after"], page3["next_after"])  # 游标不动

    def test_empty_changes_next_after_echoes_after(self):
        page = self.changes(after=0)
        self.assertEqual(page, {"changes": [], "next_after": 0})
        page = self.changes(after=42)
        self.assertEqual(page, {"changes": [], "next_after": 42})

    # ---- 变更水位历史快照（get_segment_snapshot，纯读）----

    def snapshot(self, at_change_id, source=None, after=None, limit=100):
        with read_only_session(self.db_path) as conn:
            return service.get_segment_snapshot(
                conn,
                at_change_id=at_change_id,
                source=source,
                after=after,
                limit=limit,
            )

    def test_snapshot_reconstructs_state_at_watermark(self):
        # SILENCE=10：e1@0 → 段A；e2@20 → 段B；e3@10 桥接合并 A+B
        a = self.submit("s1", "e1", 0.0, "warning")["segment"]
        b = self.submit("s1", "e2", 20.0, "error")["segment"]
        self.submit("s1", "e3", 10.0, "critical")  # 合并：B 墓碑 + A upsert(rev2)

        # 水位 1：仅 A 的初始快照
        page = self.snapshot(1)
        self.assertEqual(page["at_change_id"], 1)
        self.assertEqual([s["id"] for s in page["segments"]], [a["id"]])
        self.assertEqual(page["segments"][0]["revision"], 1)
        self.assertEqual(page["segments"][0]["event_count"], 1)
        self.assertEqual(page["segments"][0]["max_severity"], "warning")

        # 水位 2：A、B 各自独立，按 segment_id 升序
        page = self.snapshot(2)
        self.assertEqual([s["id"] for s in page["segments"]], [a["id"], b["id"]])

        # 水位 3：B 被 merged 墓碑移除；A 的最后一条变更仍是合并前快照
        page = self.snapshot(3)
        self.assertEqual([s["id"] for s in page["segments"]], [a["id"]])
        self.assertEqual(page["segments"][0]["revision"], 1)

        # 水位 4（合并完成）：A 为合并后快照（upsert 保留其快照）
        page = self.snapshot(4)
        self.assertEqual([s["id"] for s in page["segments"]], [a["id"]])
        seg = page["segments"][0]
        self.assertEqual(seg["revision"], 2)
        self.assertEqual(seg["event_count"], 3)
        self.assertEqual(seg["max_severity"], "critical")

    def test_snapshot_deleted_tombstone_removes_segment(self):
        a = self.submit("s1", "e1", 0.0)["segment"]
        self.withdraw("s1", "e1")  # 唯一事件撤回 → deleted 墓碑（change 2）
        page = self.snapshot(1)
        self.assertEqual([s["id"] for s in page["segments"]], [a["id"]])
        page = self.snapshot(2)
        self.assertEqual(page["segments"], [])

    def test_snapshot_empty_change_stream_truncates_to_zero(self):
        page = self.snapshot(10)
        self.assertEqual(
            page, {"segments": [], "at_change_id": 0, "next_after": None}
        )

    def test_snapshot_watermark_truncated_to_current_max(self):
        self.submit("s1", "e1", 0.0)
        page = self.snapshot(999)
        self.assertEqual(page["at_change_id"], 1)  # 截断到当前最大水位
        self.assertEqual(len(page["segments"]), 1)

    def test_snapshot_filters_by_source(self):
        a = self.submit("s1", "e1", 0.0)["segment"]
        self.submit("s2", "e2", 0.0)
        page = self.snapshot(2, source="s1")
        self.assertEqual([s["id"] for s in page["segments"]], [a["id"]])
        self.assertEqual(page["segments"][0]["source"], "s1")
        page = self.snapshot(2, source="ghost")
        self.assertEqual(page["segments"], [])
        self.assertIsNone(page["next_after"])  # 首页空页：游标为 None

    def test_snapshot_pagination_walks_segments_in_id_order(self):
        ids = [
            self.submit("s1", f"e{i}", i * 100.0)["segment"]["id"]
            for i in range(5)
        ]
        p1 = self.snapshot(5, limit=2)
        self.assertEqual([s["id"] for s in p1["segments"]], ids[:2])
        p2 = self.snapshot(5, after=p1["next_after"], limit=2)
        self.assertEqual([s["id"] for s in p2["segments"]], ids[2:4])
        p3 = self.snapshot(5, after=p2["next_after"], limit=2)
        self.assertEqual([s["id"] for s in p3["segments"]], ids[4:])
        # 空页：游标保持不变
        p4 = self.snapshot(5, after=p3["next_after"], limit=2)
        self.assertEqual(p4["segments"], [])
        self.assertEqual(p4["next_after"], p3["next_after"])
        self.assertEqual(p4["at_change_id"], 5)

    def test_snapshot_cursor_binds_watermark_and_source(self):
        self.submit("s1", "e1", 0.0)
        self.submit("s2", "e2", 0.0)
        cursor = self.snapshot(1, source="s1")["next_after"]
        # 跨条件复用：水位或 source 不一致 → ValueError（API 层 422）
        with self.assertRaises(ValueError):
            self.snapshot(2, source="s1", after=cursor)
        with self.assertRaises(ValueError):
            self.snapshot(1, source="s2", after=cursor)
        with self.assertRaises(ValueError):
            self.snapshot(1, source=None, after=cursor)
        # 相同条件正常翻页
        page = self.snapshot(1, source="s1", after=cursor)
        self.assertEqual(page["segments"], [])
        self.assertEqual(page["next_after"], cursor)

    def test_snapshot_cursor_keeps_truncated_watermark_pinned(self):
        self.submit("s1", "e1", 0.0)
        p1 = self.snapshot(999, limit=1)  # 截断到实际水位 1
        self.assertEqual(p1["at_change_id"], 1)
        self.submit("s1", "e2", 100.0)  # 新变更：最大水位变为 2
        # 后续页固定截断后的水位 1，不受新变更影响
        p2 = self.snapshot(1, after=p1["next_after"])
        self.assertEqual(p2["at_change_id"], 1)
        self.assertEqual(p2["segments"], [])
        # 水位条件不再等价于游标绑定的 1 → 拒绝
        with self.assertRaises(ValueError):
            self.snapshot(2, after=p1["next_after"])
        with self.assertRaises(ValueError):
            self.snapshot(999, after=p1["next_after"])

    def test_snapshot_cursor_roundtrip_and_validation(self):
        cursor = service.encode_snapshot_cursor(7, "s1", 42)
        self.assertEqual(service.decode_snapshot_cursor(cursor), (7, "s1", 42))
        cursor = service.encode_snapshot_cursor(0, None, 0)
        self.assertEqual(service.decode_snapshot_cursor(cursor), (0, None, 0))
        self.assertIsInstance(cursor, str)
        for bad in [
            "",
            "@@@",
            base64.urlsafe_b64encode(b"123").decode(),  # 合法 base64 但 JSON 是标量
            "[1]",
            '["segment-snapshot",1,"s","x"]',           # 末位应为 int
            '["segment-snapshot",-1,"s",0]',            # 水位应非负
            '["segment-snapshot",1,"s"]',               # 长度不符
            service.encode_event_cursor(1.0, "e1"),     # 事件游标不能混用
            "null",
        ]:
            with self.assertRaises(ValueError):
                service.decode_snapshot_cursor(bad)

    def test_snapshot_does_not_mutate_data(self):
        self.submit("s1", "e1", 0.0)
        before = self.changes()
        segs_before = self.get(1)
        self.snapshot(1)
        self.snapshot(999, limit=1)
        self.assertEqual(self.changes(), before)
        self.assertEqual(self.get(1), segs_before)

    # ---- 静默间隔重分段预演（纯读）----

    def preview(self, source, silence):
        with read_only_session(self.db_path) as conn:
            return service.preview_resegmentation(
                conn, source=source, silence=silence
            )

    def test_preview_smaller_interval_splits_segments(self):
        # 当前 SILENCE=10：0、8、20、28、40 → 三段 [0,8]、[20,28]、[40]
        a = self.submit("s1", "e1", 0.0)["segment"]
        self.submit("s1", "e2", 8.0)
        b = self.submit("s1", "e3", 20.0)["segment"]
        self.submit("s1", "e4", 28.0)
        c = self.submit("s1", "e5", 40.0)["segment"]

        r = self.preview("s1", 5.0)
        # 间隔 5：8→20(12)、28→40(12) 切开；0→8(8) 切开 → 五段
        sims = r["simulated_segments"]
        self.assertEqual([s["temp_id"] for s in sims], [1, 2, 3, 4, 5])
        self.assertEqual(
            [s["event_count"] for s in sims], [1, 1, 1, 1, 1]
        )
        self.assertTrue(all(s["source"] == "s1" for s in sims))

        by_id = {m["segment_id"]: m for m in r["mappings"]}
        self.assertEqual(by_id[a["id"]]["relation"], "split")
        self.assertEqual(by_id[a["id"]]["simulated_temp_ids"], [1, 2])
        self.assertEqual(by_id[b["id"]]["relation"], "split")
        self.assertEqual(by_id[b["id"]]["simulated_temp_ids"], [3, 4])
        self.assertEqual(by_id[c["id"]]["relation"], "unchanged")
        self.assertEqual(by_id[c["id"]]["simulated_temp_ids"], [5])

        summary = r["summary"]
        self.assertEqual(summary["split_existing_segment_count"], 2)
        self.assertEqual(summary["merged_existing_segment_count"], 0)
        self.assertEqual(summary["unchanged_existing_segment_count"], 1)
        self.assertEqual(summary["changed_existing_segment_count"], 2)
        self.assertEqual(summary["existing_segment_count"], 3)
        self.assertEqual(summary["simulated_segment_count"], 5)
        self.assertEqual(r["event_count"], 5)

    def test_preview_larger_interval_merges_segments(self):
        # 0、8、20、28 → 现有两段 [0,8]、[20,28]
        a = self.submit("s1", "e1", 0.0)["segment"]
        self.submit("s1", "e2", 8.0)
        b = self.submit("s1", "e3", 20.0)["segment"]
        self.submit("s1", "e4", 28.0)

        r = self.preview("s1", 100.0)
        sims = r["simulated_segments"]
        self.assertEqual(len(sims), 1)
        self.assertEqual(sims[0]["temp_id"], 1)
        self.assertEqual(sims[0]["event_count"], 4)
        self.assertEqual(sims[0]["started_at"], "1970-01-01T00:00:00+00:00")
        self.assertEqual(sims[0]["max_severity"], "info")

        by_id = {m["segment_id"]: m for m in r["mappings"]}
        self.assertEqual(by_id[a["id"]]["relation"], "merged")
        self.assertEqual(by_id[a["id"]]["simulated_temp_ids"], [1])
        self.assertEqual(by_id[b["id"]]["relation"], "merged")
        self.assertEqual(by_id[b["id"]]["simulated_temp_ids"], [1])
        self.assertEqual(r["summary"]["merged_existing_segment_count"], 2)
        self.assertEqual(r["summary"]["changed_existing_segment_count"], 2)
        self.assertEqual(r["summary"]["split_existing_segment_count"], 0)
        self.assertEqual(r["summary"]["unchanged_existing_segment_count"], 0)

    def test_preview_equal_interval_all_unchanged(self):
        a = self.submit("s1", "e1", 0.0)["segment"]
        self.submit("s1", "e2", 8.0)
        b = self.submit("s1", "e3", 20.0)["segment"]

        r = self.preview("s1", SILENCE)
        self.assertEqual(len(r["simulated_segments"]), 2)
        self.assertTrue(all(m["relation"] == "unchanged" for m in r["mappings"]))
        self.assertEqual(
            [m["simulated_temp_ids"] for m in r["mappings"]], [[1], [2]]
        )
        self.assertEqual(r["summary"]["changed_existing_segment_count"], 0)
        self.assertEqual(r["summary"]["unchanged_existing_segment_count"], 2)
        # 模拟段与现有段边界一致
        sims = {s["temp_id"]: s for s in r["simulated_segments"]}
        self.assertEqual(sims[1]["event_count"], 2)
        self.assertEqual(sims[2]["event_count"], 1)

    def test_preview_boundary_gap_equal_to_interval_joins(self):
        self.submit("s1", "e1", 0.0)
        self.submit("s1", "e2", SILENCE)  # 恰好等于间隔 → 同段
        r = self.preview("s1", SILENCE)
        self.assertEqual(len(r["simulated_segments"]), 1)

    def test_preview_max_severity_and_timestamps(self):
        self.submit("s1", "e1", 100.0, "info")
        self.submit("s1", "e2", 103.0, "critical")
        self.submit("s1", "e3", 95.0, "warning")  # 乱序到达
        r = self.preview("s1", 1000.0)
        seg = r["simulated_segments"][0]
        self.assertEqual(seg["started_at"], "1970-01-01T00:01:35+00:00")  # 95
        self.assertEqual(seg["ended_at"], "1970-01-01T00:01:43+00:00")    # 103
        self.assertEqual(seg["max_severity"], "critical")
        self.assertEqual(seg["event_count"], 3)

    def test_preview_stable_sort_tiebreaks_same_timestamp_by_event_id(self):
        self.submit("s1", "b", 0.0)
        self.submit("s1", "a", 0.0)    # 同刻事件
        self.submit("s1", "c", 100.0)  # 超静默 → 第二段
        r = self.preview("s1", 5.0)
        self.assertEqual(
            [s["temp_id"] for s in r["simulated_segments"]], [1, 2]
        )
        # 同刻事件必在同一段；temp_id 编号按 (occurred_at, event_id) 次序确定
        self.assertEqual(r["simulated_segments"][0]["event_count"], 2)

    def test_preview_is_isolated_by_source(self):
        self.submit("s1", "e1", 0.0)
        self.submit("s2", "e2", 0.0)
        self.submit("s2", "e3", 100.0)
        r = self.preview("s2", 5.0)
        self.assertEqual(r["source"], "s2")
        self.assertEqual(r["event_count"], 2)
        self.assertEqual(len(r["simulated_segments"]), 2)
        self.assertEqual(r["summary"]["existing_segment_count"], 2)
        self.assertTrue(all(s["source"] == "s2" for s in r["simulated_segments"]))

    def test_preview_unknown_source_is_empty(self):
        self.submit("s1", "e1", 0.0)
        r = self.preview("nope", 10.0)
        self.assertEqual(r["event_count"], 0)
        self.assertEqual(r["simulated_segments"], [])
        self.assertEqual(r["mappings"], [])
        self.assertEqual(
            r["summary"],
            {
                "changed_existing_segment_count": 0,
                "split_existing_segment_count": 0,
                "merged_existing_segment_count": 0,
                "unchanged_existing_segment_count": 0,
                "existing_segment_count": 0,
                "simulated_segment_count": 0,
            },
        )

    def test_preview_repeated_requests_are_identical(self):
        for i in range(6):
            self.submit("s1", f"e{i}", float(i) * 7)
        r1 = self.preview("s1", 5.0)
        r2 = self.preview("s1", 5.0)
        self.assertEqual(r1, r2)

    def test_preview_does_not_mutate_data(self):
        self.submit("s1", "e1", 0.0)
        self.submit("s1", "e2", 8.0)
        self.submit("s1", "e3", 20.0)
        with session(self.db_path) as conn:
            segs_before = service.list_segments(conn)
            changes_before = service.list_segment_changes(conn)
            events_before = conn.execute("SELECT * FROM events ORDER BY id").fetchall()

        for candidate in (0.0, 5.0, 100.0):
            self.preview("s1", candidate)

        with session(self.db_path) as conn:
            segs_after = service.list_segments(conn)
            changes_after = service.list_segment_changes(conn)
            events_after = conn.execute("SELECT * FROM events ORDER BY id").fetchall()
        self.assertEqual(segs_after, segs_before)
        self.assertEqual(changes_after, changes_before)
        self.assertEqual(
            [tuple(e) for e in events_after], [tuple(e) for e in events_before]
        )

    def test_preview_read_only_session_rejects_writes(self):
        from app.db import read_only_session

        self.submit("s1", "e1", 0.0)
        with read_only_session(self.db_path) as conn:
            with self.assertRaises(sqlite3.OperationalError):
                conn.execute(
                    "INSERT INTO segments "
                    "(source, start_ts, end_ts, max_severity, event_count, "
                    " revision, created_at, updated_at) "
                    "VALUES ('x', 0, 0, 'info', 1, 1, 0, 0)"
                )
        # 回滚释放后库仍可正常写入
        self.submit("s1", "e2", 8.0)

    # ---- 误报事件撤回（withdraw_event）----

    def test_withdraw_unknown_event_returns_none(self):
        self.submit("s1", "e1", 0.0)
        self.assertIsNone(self.withdraw("s1", "ghost"))
        # 同 event_id、不同 source 也视为未知
        self.assertIsNone(self.withdraw("other", "e1"))

    def test_withdraw_unknown_event_changes_nothing(self):
        self.submit("s1", "e1", 0.0)
        with session(self.db_path) as conn:
            segs = service.list_segments(conn)
            changes = service.list_segment_changes(conn)
            events = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        self.assertIsNone(self.withdraw("s1", "missing"))
        with session(self.db_path) as conn:
            self.assertEqual(service.list_segments(conn), segs)
            self.assertEqual(service.list_segment_changes(conn), changes)
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM events").fetchone()[0], events
            )

    def test_withdraw_sole_event_deletes_segment_and_writes_deleted_tombstone(self):
        r = self.submit("s1", "e1", 1000.0, "warning")
        sid = r["segment"]["id"]
        w = self.withdraw("s1", "e1")
        self.assertEqual(w["result"], "deleted_segment")
        self.assertEqual(w["segments"], [])
        with session(self.db_path) as conn:
            self.assertIsNone(service.get_segment(conn, sid))
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM events").fetchone()[0], 0
            )
        page = self.changes()
        self.assertEqual([c["type"] for c in page["changes"]], ["upsert", "deleted"])
        tomb = page["changes"][-1]
        self.assertEqual(tomb["segment_id"], sid)
        self.assertIsNone(tomb["merged_into"])
        self.assertIsNone(tomb["segment"])
        # 删除后再删一次：未知事件，404 语义
        self.assertIsNone(self.withdraw("s1", "e1"))

    def test_withdraw_inner_event_shrinks_segment_without_split(self):
        a = self.submit("s1", "e1", 0.0, "error")
        self.submit("s1", "e2", 5.0, "info")     # 扩 end → rev=2
        self.submit("s1", "e3", 8.0, "info")     # 扩 end → rev=3
        w = self.withdraw("s1", "e2")
        self.assertEqual(w["result"], "shrunk_segment")
        self.assertEqual(len(w["segments"]), 1)
        seg = w["segments"][0]
        self.assertEqual(seg["id"], a["segment"]["id"])  # 沿用原 id
        self.assertEqual(seg["revision"], 4)             # rev 必须递增
        self.assertEqual(seg["event_count"], 2)
        self.assertEqual(seg["max_severity"], "error")
        self.assertTrue(seg["ended_at"].startswith("1970-01-01T00:00:08"))
        with session(self.db_path) as conn:
            self.assertEqual(len(service.list_segments(conn, "s1")), 1)
            self.assertEqual(
                conn.execute(
                    "SELECT event_id FROM events WHERE segment_id = ? ORDER BY event_id",
                    (a["segment"]["id"],),
                ).fetchall().__len__(),
                2,
            )

    def test_withdraw_recomputes_max_severity(self):
        # 提级事件在段内部：撤回后最高级别必须重算回落
        self.submit("s1", "e1", 0.0, "info")
        self.submit("s1", "e2", 5.0, "critical")  # 提级 → rev=2
        w = self.withdraw("s1", "e2")
        seg = w["segments"][0]
        self.assertEqual(seg["max_severity"], "info")
        self.assertEqual(seg["event_count"], 1)
        self.assertEqual(seg["revision"], 3)

    def test_withdraw_shrinks_when_new_first_event(self):
        # 撤回段首事件后边界前移但未拆段：仍沿用原 id
        a = self.submit("s1", "e1", 0.0)
        self.submit("s1", "e2", 5.0)
        w = self.withdraw("s1", "e1")
        self.assertEqual(w["result"], "shrunk_segment")
        self.assertEqual(w["segments"][0]["id"], a["segment"]["id"])
        self.assertTrue(
            w["segments"][0]["started_at"].startswith("1970-01-01T00:00:05")
        )
        self.assertEqual(w["segments"][0]["event_count"], 1)

    def test_withdraw_bridge_event_splits_segment(self):
        a = self.submit("s1", "e1", 0.0, "error")    # 段 A [0,0]
        b = self.submit("s1", "e2", 20.0, "warning")  # 段 B [20,20]
        c = self.submit("s1", "e3", 10.0, "critical")  # 桥接 → A rev=2
        self.assertEqual(c["segment"]["id"], a["segment"]["id"])
        w = self.withdraw("s1", "e3")
        self.assertEqual(w["result"], "split_segment")
        self.assertEqual(len(w["segments"]), 2)
        kept, new = w["segments"]
        # 最早子段沿用原 id，revision 在原值上递增
        self.assertEqual(kept["id"], a["segment"]["id"])
        self.assertEqual(kept["revision"], 3)
        self.assertEqual(kept["event_count"], 1)
        self.assertEqual(kept["max_severity"], "error")
        self.assertTrue(kept["started_at"].startswith("1970-01-01T00:00:00"))
        # 其余子段新建：revision=1，不复用被合并段 B 的 id
        self.assertNotIn(new["id"], (a["segment"]["id"], b["segment"]["id"]))
        self.assertEqual(new["revision"], 1)
        self.assertEqual(new["event_count"], 1)
        self.assertEqual(new["max_severity"], "warning")
        self.assertTrue(new["started_at"].startswith("1970-01-01T00:00:20"))
        with session(self.db_path) as conn:
            segs = service.list_segments(conn, "s1")
            self.assertEqual(len(segs), 2)
            rows = conn.execute(
                "SELECT event_id, segment_id FROM events ORDER BY event_id"
            ).fetchall()
            self.assertEqual(
                {r["event_id"]: r["segment_id"] for r in rows},
                {"e1": kept["id"], "e2": new["id"]},
            )

    def test_withdraw_split_writes_ordered_upserts_only(self):
        a = self.submit("s1", "e1", 0.0)
        b = self.submit("s1", "e2", 20.0)
        self.submit("s1", "e3", 10.0)  # 桥接：B 写 merged 墓碑
        before = self.changes()["changes"]
        w = self.withdraw("s1", "e3")
        new_id = w["segments"][1]["id"]
        page = self.changes()
        added = page["changes"][len(before):]
        # 拆段只追加 upsert（保留段在前、新段在后），不写墓碑
        self.assertEqual([c["type"] for c in added], ["upsert", "upsert"])
        self.assertEqual(added[0]["segment_id"], a["segment"]["id"])
        self.assertEqual(added[0]["revision"], 3)
        self.assertEqual(added[0]["segment"]["event_count"], 1)
        self.assertEqual(added[1]["segment_id"], new_id)
        self.assertEqual(added[1]["revision"], 1)
        self.assertLess(added[0]["change_id"], added[1]["change_id"])
        ids = [c["change_id"] for c in page["changes"]]
        self.assertEqual(ids, sorted(ids))

    def test_withdraw_split_at_silence_boundary_rule(self):
        # 0、10 同段（恰好等于间隔）；撤回中间事件后 0 与 20 间隔 20 > 10 → 拆开
        self.submit("s1", "e1", 0.0)
        self.submit("s1", "e2", 20.0)
        self.submit("s1", "e3", 10.0)  # 10 同时桥接两段
        w = self.withdraw("s1", "e3")
        self.assertEqual(w["result"], "split_segment")
        self.assertEqual(len(w["segments"]), 2)

    def test_withdraw_same_timestamp_events_stay_one_segment(self):
        a = self.submit("s1", "e1", 0.0)
        self.submit("s1", "e2", 0.0)
        w = self.withdraw("s1", "e1")
        self.assertEqual(w["result"], "shrunk_segment")
        self.assertEqual(w["segments"][0]["id"], a["segment"]["id"])
        self.assertEqual(w["segments"][0]["event_count"], 1)
        w2 = self.withdraw("s1", "e2")
        self.assertEqual(w2["result"], "deleted_segment")

    def test_withdraw_does_not_touch_other_sources(self):
        self.submit("s1", "e1", 0.0)
        self.submit("s1", "e2", 20.0)
        self.submit("s1", "e3", 10.0)
        other = self.submit("s2", "e9", 5.0, "critical")
        before = self.get(other["segment"]["id"])
        self.withdraw("s1", "e3")
        self.assertEqual(self.get(other["segment"]["id"]), before)

    def test_withdraw_then_reingest_bridges_again(self):
        # 撤回 → 拆段后，以相同 id 重新提交应重新桥接，数据自洽
        a = self.submit("s1", "e1", 0.0)
        self.submit("s1", "e2", 20.0)
        self.submit("s1", "e3", 10.0)
        self.withdraw("s1", "e3")
        r = self.submit("s1", "e3", 10.0, "critical")
        self.assertFalse(r["deduplicated"])
        self.assertEqual(r["segment"]["id"], a["segment"]["id"])
        self.assertEqual(r["segment"]["event_count"], 3)
        with session(self.db_path) as conn:
            self.assertEqual(len(service.list_segments(conn, "s1")), 1)

    def test_withdraw_revision_increases_even_when_bounds_unchanged(self):
        # 撤回严格位于段内的事件：边界/级别都不变，revision 仍必须 +1 并写 upsert
        self.submit("s1", "e1", 0.0, "critical")
        self.submit("s1", "e2", 5.0, "info")    # rev=2
        self.submit("s1", "e3", 3.0, "warning")  # 段内，rev 不变仍为 2
        w = self.withdraw("s1", "e3")
        seg = w["segments"][0]
        self.assertEqual(seg["revision"], 3)
        self.assertEqual(seg["max_severity"], "critical")
        page = self.changes()
        self.assertEqual(page["changes"][-1]["type"], "upsert")
        self.assertEqual(page["changes"][-1]["revision"], 3)

    def test_withdraw_failure_rolls_back_entire_transaction(self):
        from unittest import mock

        self.submit("s1", "e1", 0.0)
        self.submit("s1", "e2", 20.0)
        self.submit("s1", "e3", 10.0)  # 桥接成一段（撤回应拆段）
        with session(self.db_path) as conn:
            snapshot = {
                "events": [
                    tuple(r)
                    for r in conn.execute(
                        "SELECT segment_id, event_id FROM events ORDER BY id"
                    )
                ],
                "segments": [
                    tuple(r)
                    for r in conn.execute(
                        "SELECT id, start_ts, end_ts, max_severity, event_count, "
                        "revision FROM segments ORDER BY id"
                    )
                ],
                "changes": conn.execute(
                    "SELECT COUNT(*) FROM segment_changes"
                ).fetchone()[0],
            }

        real_record = service._record_change
        calls = {"n": 0}

        def flaky(conn, **kwargs):
            calls["n"] += 1
            if calls["n"] == 2:  # 保留段 upsert 之后、新段 upsert 时失败
                raise RuntimeError("simulated failure")
            return real_record(conn, **kwargs)

        with mock.patch.object(service, "_record_change", flaky):
            with self.assertRaises(RuntimeError):
                self.withdraw("s1", "e3")

        with session(self.db_path) as conn:
            self.assertEqual(
                [
                    tuple(r)
                    for r in conn.execute(
                        "SELECT segment_id, event_id FROM events ORDER BY id"
                    )
                ],
                snapshot["events"],
            )
            self.assertEqual(
                [
                    tuple(r)
                    for r in conn.execute(
                        "SELECT id, start_ts, end_ts, max_severity, event_count, "
                        "revision FROM segments ORDER BY id"
                    )
                ],
                snapshot["segments"],
            )
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM segment_changes"
                ).fetchone()[0],
                snapshot["changes"],
            )

    # ---- 事件审计查询（list_events，纯读）----

    def audit(self, source, **kwargs):
        with read_only_session(self.db_path) as conn:
            return service.list_events(conn, source=source, **kwargs)

    def test_list_events_orders_by_occurred_at_then_event_id(self):
        # 乱序写入、含同刻事件：输出按 (occurred_at, event_id) 稳定排序
        self.submit("s1", "c", 30.0)
        self.submit("s1", "b", 10.0, "error")
        self.submit("s1", "a2", 10.0)
        self.submit("s1", "a1", 10.0)
        page = self.audit("s1")
        self.assertEqual(
            [(e["event_id"], e["severity"]) for e in page["events"]],
            [("a1", "info"), ("a2", "info"), ("b", "error"), ("c", "info")],
        )
        self.assertIsNotNone(page["next_after"])  # 非空首页返回游标
        first = page["events"][0]
        self.assertEqual(first["source"], "s1")
        self.assertEqual(first["occurred_at"], "1970-01-01T00:00:10+00:00")
        self.assertIsInstance(first["segment_id"], int)

    def test_list_events_fields_include_current_segment_id(self):
        r1 = self.submit("s1", "e1", 0.0)["segment"]
        self.submit("s1", "e2", 20.0)
        self.submit("s1", "e3", 10.0)  # 桥接合并到 e1 的段
        page = self.audit("s1")
        self.assertEqual([e["event_id"] for e in page["events"]], ["e1", "e3", "e2"])
        self.assertTrue(all(e["segment_id"] == r1["id"] for e in page["events"]))

        # 撤回桥接事件 → 拆段：segment_id 反映重算后的实时归属
        self.withdraw("s1", "e3")
        page = self.audit("s1")
        sids = {e["event_id"]: e["segment_id"] for e in page["events"]}
        self.assertEqual(sids["e1"], r1["id"])
        self.assertNotEqual(sids["e2"], r1["id"])

    def test_list_events_filters_by_source(self):
        self.submit("s1", "e1", 0.0)
        self.submit("s2", "e2", 0.0)
        page = self.audit("s1")
        self.assertEqual([e["event_id"] for e in page["events"]], ["e1"])
        self.assertEqual(self.audit("ghost")["events"], [])

    def test_list_events_filters_by_time_window_inclusive(self):
        for eid, ts in [("a", 0.0), ("b", 10.0), ("c", 20.0), ("d", 30.0)]:
            self.submit("s1", eid, ts)
        page = self.audit("s1", start_ts=10.0, end_ts=20.0)
        self.assertEqual([e["event_id"] for e in page["events"]], ["b", "c"])
        page = self.audit("s1", start_ts=10.0)
        self.assertEqual([e["event_id"] for e in page["events"]], ["b", "c", "d"])
        page = self.audit("s1", end_ts=10.0)
        self.assertEqual([e["event_id"] for e in page["events"]], ["a", "b"])

    def test_list_events_filters_by_severities(self):
        self.submit("s1", "a", 0.0, "info")
        self.submit("s1", "b", 1.0, "error")
        self.submit("s1", "c", 2.0, "critical")
        page = self.audit("s1", severities=["error", "critical"])
        self.assertEqual([e["event_id"] for e in page["events"]], ["b", "c"])
        # 空列表等同于不过滤
        self.assertEqual(len(self.audit("s1", severities=[])["events"]), 3)

    def test_list_events_cursor_pagination_walks_all_rows(self):
        for i in range(5):
            self.submit("s1", f"e{i}", float(i) * 100.0)
        seen = []
        after = None
        pages = 0
        while True:
            page = self.audit("s1", after=after, limit=2)
            pages += 1
            seen.extend(page["events"])
            after = page["next_after"]
            if not page["events"]:
                break
        self.assertEqual([e["event_id"] for e in seen], [f"e{i}" for i in range(5)])
        self.assertEqual(pages, 4)  # 2 + 2 + 1 + 空页

    def test_list_events_cursor_pagination_with_filters(self):
        for eid, ts, s in [
            ("a", 0.0, "info"), ("b", 1.0, "error"), ("c", 2.0, "info"),
            ("d", 3.0, "error"), ("e", 4.0, "info"),
        ]:
            self.submit("s1", eid, ts, s)
        after = None
        picked = []
        while True:
            page = self.audit(
                "s1", severities=["error"], start_ts=0.0, end_ts=4.0,
                after=after, limit=1,
            )
            picked.extend(page["events"])
            after = page["next_after"]
            if not page["events"]:
                break
        self.assertEqual([e["event_id"] for e in picked], ["b", "d"])

    def test_list_events_same_timestamp_tiebreaks_by_event_id(self):
        # 游标停在同刻首事件后，下一页必须从同刻的下一个 event_id 继续
        self.submit("s1", "b", 0.0)
        self.submit("s1", "a", 0.0)
        self.submit("s1", "c", 0.0)
        p1 = self.audit("s1", limit=1)
        self.assertEqual(p1["events"][0]["event_id"], "a")
        p2 = self.audit("s1", after=p1["next_after"], limit=10)
        self.assertEqual([e["event_id"] for e in p2["events"]], ["b", "c"])

    def test_list_events_empty_page_keeps_cursor(self):
        self.submit("s1", "e1", 0.0)
        cursor = service.encode_event_cursor(1000.0, "zzz")
        page = self.audit("s1", after=cursor, limit=10)
        self.assertEqual(page, {"events": [], "next_after": cursor})

    def test_list_events_first_empty_page_next_after_is_none(self):
        self.assertEqual(
            self.audit("s1"), {"events": [], "next_after": None}
        )

    def test_list_events_does_not_repeat_or_skip_rows(self):
        for i in range(7):
            self.submit("s1", f"e{i}", float(i))
        p1 = self.audit("s1", limit=3)
        p2 = self.audit("s1", after=p1["next_after"], limit=3)
        p3 = self.audit("s1", after=p2["next_after"], limit=3)
        ids = [e["event_id"] for p in (p1, p2, p3) for e in p["events"]]
        self.assertEqual(ids, [f"e{i}" for i in range(7)])
        self.assertEqual(len(set(ids)), 7)

    def test_event_cursor_roundtrip_and_validation(self):
        cursor = service.encode_event_cursor(123.5, "evt-1")
        self.assertEqual(service.decode_event_cursor(cursor), (123.5, "evt-1"))
        # 游标为不透明字符串，且不依赖具体 JSON 结构细节
        self.assertIsInstance(cursor, str)
        for bad in [
            "",
            "@@@",
            base64.urlsafe_b64encode(b"123").decode(),  # 合法 base64 但 JSON 是标量
            "[1]",
            '["x","y"]',
            "null",
        ]:
            with self.assertRaises(ValueError):
                service.decode_event_cursor(bad)

    def test_list_events_reads_consistent_snapshot(self):
        # 只读快照内两次读取结果一致；外部并发写入不影响快照内后续读取
        self.submit("s1", "e1", 0.0)
        with read_only_session(self.db_path) as conn:
            p1 = service.list_events(conn, source="s1", limit=100)
            self.submit("s1", "e2", 5.0)  # 快照开启后写入
            p2 = service.list_events(conn, source="s1", limit=100)
        self.assertEqual(p1, p2)
        self.assertEqual(len(p2["events"]), 1)

    # ---- 事件审计时间桶统计（event_metrics，纯读）----

    def metrics(self, source, start_ts, end_ts, bucket_seconds):
        with read_only_session(self.db_path) as conn:
            return service.event_metrics(
                conn,
                source=source,
                start_ts=start_ts,
                end_ts=end_ts,
                bucket_seconds=bucket_seconds,
            )

    def test_metrics_buckets_counts_and_empty_buckets(self):
        # SILENCE=10：e1@0、e2@5 同段 A；e3@20、e4@25 同段 B；中间桶无事件
        self.submit("s1", "e1", 0.0, "info")
        self.submit("s1", "e2", 5.0, "error")
        self.submit("s1", "e3", 20.0, "warning")
        self.submit("s1", "e4", 25.0, "critical")

        r = self.metrics("s1", 0.0, 40.0, 10.0)
        self.assertEqual(r["source"], "s1")
        self.assertEqual(r["start_at"], "1970-01-01T00:00:00+00:00")
        self.assertEqual(r["end_at"], "1970-01-01T00:00:40+00:00")
        self.assertEqual(r["bucket_seconds"], 10.0)

        buckets = r["buckets"]
        self.assertEqual(len(buckets), 4)  # 含无事件桶
        self.assertEqual(
            [(b["bucket_start"], b["bucket_end"]) for b in buckets],
            [
                ("1970-01-01T00:00:00+00:00", "1970-01-01T00:00:10+00:00"),
                ("1970-01-01T00:00:10+00:00", "1970-01-01T00:00:20+00:00"),
                ("1970-01-01T00:00:20+00:00", "1970-01-01T00:00:30+00:00"),
                ("1970-01-01T00:00:30+00:00", "1970-01-01T00:00:40+00:00"),
            ],
        )
        # 桶按时间升序（左闭右开边界单调）
        starts = [b["bucket_start"] for b in buckets]
        self.assertEqual(starts, sorted(starts))

        zero_counts = {level: 0 for level in (
            "debug", "info", "warning", "error", "critical"
        )}
        for bucket in buckets:
            self.assertEqual(
                set(bucket["severity_counts"]),
                {"debug", "info", "warning", "error", "critical"},
            )

        b0, b1, b2, b3 = buckets
        self.assertEqual(b0["severity_counts"], {**zero_counts, "info": 1, "error": 1})
        self.assertEqual(b0["event_count"], 2)
        self.assertEqual(b0["distinct_segment_count"], 1)  # 两事件同段 A
        self.assertEqual(b1["severity_counts"], zero_counts)
        self.assertEqual(b1["event_count"], 0)
        self.assertEqual(b1["distinct_segment_count"], 0)
        self.assertEqual(
            b2["severity_counts"], {**zero_counts, "warning": 1, "critical": 1}
        )
        self.assertEqual(b2["event_count"], 2)
        self.assertEqual(b2["distinct_segment_count"], 1)  # 两事件同段 B
        self.assertEqual(b3["severity_counts"], zero_counts)
        self.assertEqual(b3["event_count"], 0)

    def test_metrics_window_is_half_open(self):
        # 恰好落在桶边界/窗口边界的事件：左闭右开
        self.submit("s1", "a", 10.0, "info")
        self.submit("s1", "b", 20.0, "error")

        # 窗口 [0, 20)：a@10 在内（第二个桶），b@20 被排除
        r = self.metrics("s1", 0.0, 20.0, 10.0)
        self.assertEqual(len(r["buckets"]), 2)
        self.assertEqual(r["buckets"][0]["event_count"], 0)
        self.assertEqual(r["buckets"][1]["event_count"], 1)
        self.assertEqual(r["buckets"][1]["severity_counts"]["info"], 1)

        # 窗口 [20, 30)：b@20 在起点被包含
        r = self.metrics("s1", 20.0, 30.0, 10.0)
        self.assertEqual(len(r["buckets"]), 1)
        self.assertEqual(r["buckets"][0]["event_count"], 1)
        self.assertEqual(r["buckets"][0]["severity_counts"]["error"], 1)

    def test_metrics_last_bucket_may_be_shorter(self):
        self.submit("s1", "e1", 24.0)
        r = self.metrics("s1", 0.0, 25.0, 10.0)  # 25/10 向上取整 → 3 桶
        self.assertEqual(len(r["buckets"]), 3)
        last = r["buckets"][-1]
        self.assertEqual(last["bucket_start"], "1970-01-01T00:00:20+00:00")
        self.assertEqual(last["bucket_end"], "1970-01-01T00:00:25+00:00")  # 截到 end
        self.assertEqual(last["event_count"], 1)

    def test_metrics_distinct_segment_ids_per_bucket(self):
        # 同段事件落在两个桶：每桶 distinct=1；不同段落同一桶：distinct=2
        a = self.submit("s1", "e1", 0.0)["segment"]["id"]
        self.submit("s1", "e2", 5.0)   # 与 e1 同段 A
        b = self.submit("s1", "e3", 20.0)["segment"]["id"]  # 间隔 15 > 10 → 段 B

        r = self.metrics("s1", 0.0, 40.0, 20.0)
        self.assertEqual(len(r["buckets"]), 2)
        self.assertEqual(r["buckets"][0]["event_count"], 2)
        self.assertEqual(r["buckets"][0]["distinct_segment_count"], 1)
        self.assertEqual(r["buckets"][1]["event_count"], 1)
        self.assertEqual(r["buckets"][1]["distinct_segment_count"], 1)

        r = self.metrics("s1", 0.0, 40.0, 40.0)  # 单桶含两个不同段
        self.assertEqual(len(r["buckets"]), 1)
        self.assertEqual(r["buckets"][0]["event_count"], 3)
        self.assertEqual(r["buckets"][0]["distinct_segment_count"], 2)
        self.assertNotEqual(a, b)

    def test_metrics_distinct_segments_track_current_ownership_after_withdraw(self):
        # 桥接合并后所有事件同段；撤回桥接事件拆段，统计反映实时归属
        self.submit("s1", "e1", 0.0)
        self.submit("s1", "e2", 20.0)
        self.submit("s1", "e3", 10.0, "critical")  # 桥接 → 全并入段 A
        r = self.metrics("s1", 0.0, 30.0, 10.0)
        self.assertEqual([b["distinct_segment_count"] for b in r["buckets"]], [1, 1, 1])

        self.withdraw("s1", "e3")  # 拆段：e2 归新段
        r = self.metrics("s1", 0.0, 30.0, 10.0)
        counts = [b["event_count"] for b in r["buckets"]]
        distinct = [b["distinct_segment_count"] for b in r["buckets"]]
        self.assertEqual(counts, [1, 0, 1])
        self.assertEqual(distinct, [1, 0, 1])
        # 有数据的两个桶时间位置不同（首桶与第三桶）
        self.assertNotEqual(
            r["buckets"][0]["bucket_start"], r["buckets"][2]["bucket_start"]
        )
        self.assertEqual(r["buckets"][1]["event_count"], 0)  # 中间桶无事件

    def test_metrics_filters_by_source(self):
        self.submit("s1", "e1", 0.0, "error")
        self.submit("s2", "e2", 0.0, "critical")
        r = self.metrics("s1", 0.0, 10.0, 10.0)
        self.assertEqual(r["buckets"][0]["event_count"], 1)
        self.assertEqual(r["buckets"][0]["severity_counts"]["error"], 1)
        self.assertEqual(r["buckets"][0]["severity_counts"]["critical"], 0)

    def test_metrics_unknown_source_all_zero_buckets(self):
        r = self.metrics("ghost", 0.0, 30.0, 10.0)
        self.assertEqual(len(r["buckets"]), 3)
        for bucket in r["buckets"]:
            self.assertEqual(bucket["event_count"], 0)
            self.assertEqual(bucket["distinct_segment_count"], 0)
            self.assertTrue(all(v == 0 for v in bucket["severity_counts"].values()))

    def test_metrics_same_timestamp_events_same_bucket(self):
        for eid, level in [("a", "info"), ("b", "error"), ("c", "critical")]:
            self.submit("s1", eid, 0.0, level)
        r = self.metrics("s1", 0.0, 10.0, 10.0)
        self.assertEqual(len(r["buckets"]), 1)
        self.assertEqual(r["buckets"][0]["event_count"], 3)
        self.assertEqual(
            r["buckets"][0]["severity_counts"],
            {"debug": 0, "info": 1, "warning": 0, "error": 1, "critical": 1},
        )

    def test_metrics_float_width_boundary_aligned_correctly(self):
        # 0.3/0.1 的浮点结果约为 2.9999999999999996：必须归入第 4 个桶（index 3）
        self.submit("s1", "e1", 0.3)
        r = self.metrics("s1", 0.0, 0.5, 0.1)
        self.assertEqual(len(r["buckets"]), 5)
        counts = [b["event_count"] for b in r["buckets"]]
        self.assertEqual(counts, [0, 0, 0, 1, 0])
        self.assertEqual(
            r["buckets"][3]["bucket_start"], "1970-01-01T00:00:00.300000+00:00"
        )

    def test_metrics_float_exact_multiple_does_not_add_bucket(self):
        # 窗口恰好是桶宽整倍数时桶数不 +1（0.3 == 3 * 0.1）
        r = self.metrics("s1", 0.0, 0.3, 0.1)
        self.assertEqual(len(r["buckets"]), 3)
        self.assertEqual(
            r["buckets"][-1]["bucket_end"], "1970-01-01T00:00:00.300000+00:00"
        )

    def test_metrics_rejects_invalid_range_and_width(self):
        with self.assertRaises(ValueError):
            self.metrics("s1", 10.0, 10.0, 10.0)  # end == start：空窗口
        with self.assertRaises(ValueError):
            self.metrics("s1", 20.0, 10.0, 10.0)  # end < start
        with self.assertRaises(ValueError):
            self.metrics("s1", 0.0, 10.0, 0.0)   # 桶长为 0
        with self.assertRaises(ValueError):
            self.metrics("s1", 0.0, 10.0, -1.0)  # 负桶长
        with self.assertRaises(ValueError):
            self.metrics("s1", 0.0, 10.0, float("nan"))
        with self.assertRaises(ValueError):
            self.metrics("s1", 0.0, 10.0, float("inf"))

    def test_metrics_bucket_count_capped(self):
        # 10000 个桶允许，10001 个拒绝
        r = self.metrics("s1", 0.0, 10_000.0, 1.0)
        self.assertEqual(len(r["buckets"]), service.MAX_METRIC_BUCKETS)
        with self.assertRaises(ValueError):
            self.metrics("s1", 0.0, 10_001.0, 1.0)

    def test_metrics_reads_consistent_snapshot(self):
        self.submit("s1", "e1", 0.0)
        with read_only_session(self.db_path) as conn:
            kwargs = dict(source="s1", start_ts=0.0, end_ts=20.0, bucket_seconds=10.0)
            p1 = service.event_metrics(conn, **kwargs)
            self.submit("s1", "e2", 5.0)  # 快照开启后写入
            p2 = service.event_metrics(conn, **kwargs)
        self.assertEqual(p1, p2)
        self.assertEqual(sum(b["event_count"] for b in p2["buckets"]), 1)

    def test_metrics_does_not_mutate_data(self):
        self.submit("s1", "e1", 0.0)
        self.submit("s1", "e2", 20.0)
        before = self.changes()
        self.metrics("s1", 0.0, 40.0, 10.0)
        self.assertEqual(self.changes(), before)

    # ---- 人工确认审计（acknowledge_segment / list_acknowledgements）----

    def ack(self, segment_id, *, operator="ops", note="n",
            expected_revision=1, key="k1"):
        with session(self.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            return service.acknowledge_segment(
                conn,
                segment_id=segment_id,
                operator=operator,
                note=note,
                expected_revision=expected_revision,
                idempotency_key=key,
            )

    def acks(self, segment_id):
        with session(self.db_path) as conn:
            return service.list_acknowledgements(conn, segment_id)

    def test_acknowledge_created_when_revision_matches(self):
        seg = self.submit("s1", "e1", 1000.0, "warning")["segment"]
        r = self.ack(seg["id"], operator="alice", note="已确认",
                     expected_revision=1, key="k1")
        self.assertEqual(r["outcome"], "created")
        a = r["acknowledgement"]
        self.assertEqual(a["ack_id"], 1)
        self.assertEqual(a["segment_id"], seg["id"])
        self.assertEqual(a["operator"], "alice")
        self.assertEqual(a["note"], "已确认")
        self.assertEqual(a["expected_revision"], 1)
        self.assertEqual(a["segment_revision"], 1)
        self.assertEqual(a["idempotency_key"], "k1")
        self.assertTrue(a["created_at"])
        self.assertEqual(self.acks(seg["id"]), [a])

    def test_acknowledge_revision_conflict_returns_current_snapshot(self):
        self.submit("s1", "e1", 1000.0)
        seg = self.submit("s1", "e2", 1005.0, "error")["segment"]  # rev=2
        r = self.ack(seg["id"], expected_revision=1, key="k1")
        self.assertEqual(r["outcome"], "revision_conflict")
        self.assertEqual(r["segment"], self.get(seg["id"]))  # 当前段快照
        self.assertEqual(r["segment"]["revision"], 2)
        self.assertEqual(self.acks(seg["id"]), [])           # 冲突不落库
        # 用正确 revision 重试同一键：可正常落库（冲突不占用幂等键）
        r2 = self.ack(seg["id"], expected_revision=2, key="k1")
        self.assertEqual(r2["outcome"], "created")
        self.assertEqual(r2["acknowledgement"]["segment_revision"], 2)

    def test_acknowledge_unknown_segment_not_found(self):
        r = self.ack(9999, expected_revision=1, key="k1")
        self.assertEqual(r["outcome"], "not_found")

    def test_acknowledge_replay_returns_original_even_after_segment_changes(self):
        seg = self.submit("s1", "e1", 1000.0)["segment"]
        first = self.ack(seg["id"], operator="alice", note="n",
                         expected_revision=1, key="k1")["acknowledgement"]
        # 段随后发生变化：revision 升到 2，原 expected_revision 已过期
        self.submit("s1", "e2", 1005.0, "critical")
        self.assertEqual(self.get(seg["id"])["revision"], 2)
        replay = self.ack(seg["id"], operator="alice", note="n",
                          expected_revision=1, key="k1")
        self.assertEqual(replay["outcome"], "replayed")
        self.assertEqual(replay["acknowledgement"], first)  # 原记录原样返回
        self.assertEqual(len(self.acks(seg["id"])), 1)      # 不产生重复记录

    def test_acknowledge_same_key_different_payload_conflicts(self):
        seg = self.submit("s1", "e1", 1000.0)["segment"]
        self.ack(seg["id"], operator="alice", note="n",
                 expected_revision=1, key="k1")
        for kwargs in ({"operator": "bob"}, {"note": "other"},
                       {"expected_revision": 2}):
            r = self.ack(seg["id"], key="k1", **kwargs)
            self.assertEqual(r["outcome"], "payload_conflict")
        self.assertEqual(len(self.acks(seg["id"])), 1)

    def test_acknowledge_key_scoped_per_segment(self):
        a = self.submit("s1", "e1", 1000.0)["segment"]
        b = self.submit("s2", "e2", 1000.0)["segment"]
        r1 = self.ack(a["id"], key="same-key")
        r2 = self.ack(b["id"], key="same-key")  # 不同段复用同键互不干扰
        self.assertEqual(r1["outcome"], "created")
        self.assertEqual(r2["outcome"], "created")
        self.assertNotEqual(
            r1["acknowledgement"]["ack_id"], r2["acknowledgement"]["ack_id"]
        )

    def test_acknowledge_list_ordered_by_ack_id(self):
        seg = self.submit("s1", "e1", 1000.0)["segment"]
        other = self.submit("s2", "e2", 1000.0)["segment"]
        self.ack(seg["id"], key="k1")
        self.ack(other["id"], key="k2")  # 交织写入其他段，不影响本段次序
        self.ack(seg["id"], key="k3")
        self.ack(seg["id"], key="k2")
        rows = self.acks(seg["id"])
        self.assertEqual([r["idempotency_key"] for r in rows], ["k1", "k3", "k2"])
        ids = [r["ack_id"] for r in rows]
        self.assertEqual(ids, sorted(ids))

    def test_acknowledge_does_not_touch_segment_or_change_stream(self):
        seg = self.submit("s1", "e1", 1000.0)["segment"]
        before_seg = self.get(seg["id"])
        before_changes = self.changes()
        self.ack(seg["id"], expected_revision=1, key="k1")
        self.assertEqual(self.get(seg["id"]), before_seg)      # revision 不变
        self.assertEqual(self.changes(), before_changes)       # 不写变更流

    def test_acknowledge_records_survive_segment_deletion(self):
        seg = self.submit("s1", "e1", 1000.0)["segment"]
        first = self.ack(seg["id"], key="k1")["acknowledgement"]
        self.withdraw("s1", "e1")  # 唯一事件撤回 → 段被删除
        self.assertIsNone(self.get(seg["id"]))
        # 审计记录仍保留；幂等重放（查重先于段状态校验）仍返回原记录
        replay = self.ack(seg["id"], key="k1")
        self.assertEqual(replay["outcome"], "replayed")
        self.assertEqual(replay["acknowledgement"], first)
        self.assertEqual(self.acks(seg["id"]), [first])

    # ---- 旧库迁移（segment_changes CHECK 增加 deleted）----

    def test_init_db_migrates_legacy_segment_changes_check(self):
        import sqlite3

        legacy = os.path.join(self.tmp.name, "legacy.db")
        conn = sqlite3.connect(legacy)
        conn.executescript(
            """
            CREATE TABLE segment_changes (
                change_id INTEGER PRIMARY KEY AUTOINCREMENT,
                type TEXT NOT NULL CHECK (type IN ('upsert','merged')),
                segment_id INTEGER NOT NULL, merged_into INTEGER, revision INTEGER,
                source TEXT, start_ts REAL, end_ts REAL, max_severity TEXT,
                event_count INTEGER, created_at REAL NOT NULL);
            """
        )
        conn.execute(
            "INSERT INTO segment_changes "
            "(type, segment_id, revision, created_at) VALUES ('upsert', 7, 1, 1.0)"
        )
        conn.commit()
        conn.close()

        from app.db import init_db as _init

        _init(legacy)  # 迁移
        _init(legacy)  # 再次执行必须幂等

        conn = sqlite3.connect(legacy)
        try:
            sql = conn.execute(
                "SELECT sql FROM sqlite_master "
                "WHERE type='table' AND name='segment_changes'"
            ).fetchone()[0]
            self.assertIn("'deleted'", sql)
            # 历史记录与 change_id 原样保留
            row = conn.execute(
                "SELECT change_id, type, segment_id, revision "
                "FROM segment_changes"
            ).fetchone()
            self.assertEqual(tuple(row), (1, "upsert", 7, 1))
            conn.execute(
                "INSERT INTO segment_changes (type, segment_id, created_at) "
                "VALUES ('deleted', 7, 2.0)"
            )
            with self.assertRaises(sqlite3.IntegrityError):
                conn.execute(
                    "INSERT INTO segment_changes (type, segment_id, created_at) "
                    "VALUES ('bogus', 7, 3.0)"
                )
            conn.commit()
        finally:
            conn.close()

        # 迁移后的库可直接完成完整撤回流程
        from app import service as svc

        with session(legacy) as c:
            c.execute(
                "INSERT INTO segments "
                "(id, source, start_ts, end_ts, max_severity, event_count, "
                " revision, created_at, updated_at) "
                "VALUES (7, 's', 0, 0, 'info', 1, 1, 1, 1)"
            )
            c.execute(
                "INSERT INTO events "
                "(segment_id, source, event_id, occurred_at, severity, received_at) "
                "VALUES (7, 's', 'only', 0, 'info', 1)"
            )
            w = svc.withdraw_event(c, source="s", event_id="only", silence=SILENCE)
        self.assertEqual(w["result"], "deleted_segment")

    def test_init_db_adds_acknowledgements_table_to_legacy_volume(self):
        # 模拟确认功能上线前的持久化卷：只有旧三表，无 segment_acknowledgements
        import sqlite3

        legacy = os.path.join(self.tmp.name, "legacy_ack.db")
        conn = sqlite3.connect(legacy)
        conn.executescript(
            """
            CREATE TABLE segments (
                id INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT NOT NULL,
                start_ts REAL NOT NULL, end_ts REAL NOT NULL,
                max_severity TEXT NOT NULL, event_count INTEGER NOT NULL,
                revision INTEGER NOT NULL,
                created_at REAL NOT NULL, updated_at REAL NOT NULL);
            CREATE TABLE events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                segment_id INTEGER NOT NULL REFERENCES segments (id),
                source TEXT NOT NULL, event_id TEXT NOT NULL,
                occurred_at REAL NOT NULL, severity TEXT NOT NULL,
                received_at REAL NOT NULL, UNIQUE (source, event_id));
            CREATE TABLE segment_changes (
                change_id INTEGER PRIMARY KEY AUTOINCREMENT,
                type TEXT NOT NULL CHECK (type IN ('upsert','merged')),
                segment_id INTEGER NOT NULL, merged_into INTEGER, revision INTEGER,
                source TEXT, start_ts REAL, end_ts REAL, max_severity TEXT,
                event_count INTEGER, created_at REAL NOT NULL);
            """
        )
        conn.execute(
            "INSERT INTO segments (source, start_ts, end_ts, max_severity, "
            "event_count, revision, created_at, updated_at) "
            "VALUES ('s', 0, 0, 'info', 1, 1, 1, 1)"
        )
        conn.commit()
        conn.close()

        from app.db import init_db as _init

        _init(legacy)  # 启动迁移：补齐新表
        _init(legacy)  # 再次执行必须幂等

        # 旧数据完整保留，确认流程在新表上正常工作
        with session(legacy) as c:
            c.execute("BEGIN IMMEDIATE")
            r = service.acknowledge_segment(
                c, segment_id=1, operator="ops", note="n",
                expected_revision=1, idempotency_key="k1",
            )
        self.assertEqual(r["outcome"], "created")
        with session(legacy) as c:
            self.assertEqual(len(service.list_acknowledgements(c, 1)), 1)
            self.assertIsNotNone(service.get_segment(c, 1))  # 旧段数据未受影响


class MaintenanceWindowTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "test.db")
        init_db(self.db_path)

    def tearDown(self):
        self.tmp.cleanup()

    def submit(self, source, event_id, occurred_ts, severity="info"):
        with session(self.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            return service.ingest_event(
                conn,
                source=source,
                event_id=event_id,
                occurred_ts=occurred_ts,
                severity=severity,
                silence=SILENCE,
            )

    def create_window(self, source, start_ts, end_ts, reason="maint"):
        with session(self.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            return service.create_maintenance_window(
                conn,
                source=source,
                start_ts=start_ts,
                end_ts=end_ts,
                reason=reason,
            )

    def window_events(self, window_id):
        with read_only_session(self.db_path) as conn:
            return service.list_maintenance_window_events(conn, window_id)

    def test_create_window_persists_fields(self):
        w = self.create_window("s1", 1000.0, 2000.0, "power")
        self.assertEqual(w["source"], "s1")
        self.assertEqual(w["reason"], "power")
        with read_only_session(self.db_path) as conn:
            row = conn.execute(
                "SELECT start_ts, end_ts FROM maintenance_windows WHERE id = ?",
                (w["id"],),
            ).fetchone()
        self.assertEqual((row["start_ts"], row["end_ts"]), (1000.0, 2000.0))

    def test_overlapping_windows_allowed_and_get_distinct_ids(self):
        w1 = self.create_window("s1", 1000.0, 2000.0)
        w2 = self.create_window("s1", 1500.0, 2500.0)
        self.assertNotEqual(w1["id"], w2["id"])

    def test_invalid_range_raises_and_rolls_back(self):
        with self.assertRaises(ValueError):
            self.create_window("s1", 2000.0, 2000.0)
        with self.assertRaises(ValueError):
            self.create_window("s1", 2001.0, 2000.0)
        with read_only_session(self.db_path) as conn:
            count = conn.execute(
                "SELECT COUNT(*) AS n FROM maintenance_windows"
            ).fetchone()["n"]
        self.assertEqual(count, 0)

    def test_unknown_window_returns_none(self):
        self.assertIsNone(self.window_events(9999))

    def test_fully_covered_segment_events_classified_fully(self):
        # 同一 source 两个间隔 5s 的事件 → 同一段，且整体在窗口内
        self.submit("s1", "e1", 1000.0, "warning")
        self.submit("s1", "e2", 1005.0, "error")
        w = self.create_window("s1", 900.0, 1100.0)
        r = self.window_events(w["id"])
        fully_ids = [e["event_id"] for e in r["fully"]]
        self.assertEqual(fully_ids, ["e1", "e2"])  # 时间升序
        self.assertEqual(r["partially"], [])
        self.assertEqual({e["segment_id"] for e in r["fully"]}, {1})

    def test_partial_coverage_segment_spans_window_start(self):
        # 段 [1000, 1005]，窗口 [1003, 1100)：e2 在窗口内但段起点在窗口外
        self.submit("s1", "e1", 1000.0)
        self.submit("s1", "e2", 1005.0)
        w = self.create_window("s1", 1003.0, 1100.0)
        r = self.window_events(w["id"])
        self.assertEqual(r["fully"], [])
        self.assertEqual(
            [e["event_id"] for e in r["partially"]], ["e2"]
        )

    def test_partial_coverage_segment_touches_window_end(self):
        # 段 [1000, 1005]，窗口 [1000, 1005)：恰在 end_ts 上的 e2 不被覆盖
        # （左闭右开）；窗口内的 e1 因段终点 == end_ts（不早于窗口终点）→ partially
        self.submit("s1", "e1", 1000.0)
        self.submit("s1", "e2", 1005.0)
        w = self.create_window("s1", 1000.0, 1005.0)
        r = self.window_events(w["id"])
        self.assertEqual(
            [e["event_id"] for e in r["partially"]], ["e1"]
        )
        self.assertEqual(r["fully"], [])

    def test_end_boundary_event_is_excluded_half_open(self):
        # 恰好在 end_ts 上的事件不算窗口覆盖（左闭右开）；若其段无其他事件
        # 在窗口内，则两个分类都不出现该事件
        self.submit("s1", "e1", 995.0)
        self.submit("s1", "e2", 1000.0)  # 与 e1 同段（间隔 5s）
        w = self.create_window("s1", 990.0, 1000.0)
        r = self.window_events(w["id"])
        ids = [e["event_id"] for e in r["fully"] + r["partially"]]
        self.assertNotIn("e2", ids)
        self.assertIn("e1", ids)

    def test_source_exact_match_is_case_sensitive_and_no_fuzzy_match(self):
        self.submit("s1", "e1", 1000.0)
        self.submit("S1", "e1", 1000.0)
        w = self.create_window("s1", 900.0, 1100.0)
        r = self.window_events(w["id"])
        sources = {e["source"] for e in r["fully"]}
        self.assertEqual(sources, {"s1"})
        self.assertEqual(len(r["fully"]), 1)
        # 不存在的 source 的窗口：两类均空
        w2 = self.create_window("other", 900.0, 1100.0)
        r2 = self.window_events(w2["id"])
        self.assertEqual((r2["fully"], r2["partially"]), ([], []))

    def test_events_ordered_by_occurred_at_then_event_id(self):
        # 乱序写入、同一时刻多个事件：结果按 (occurred_at, event_id) 稳定升序
        for eid, ts in [("e3", 1020.0), ("e1", 1000.0), ("e2b", 1010.0),
                        ("e2a", 1010.0)]:
            self.submit("s1", eid, ts)
        w = self.create_window("s1", 0.0, 2000.0)
        r = self.window_events(w["id"])
        self.assertEqual([e["event_id"] for e in r["fully"]],
                         ["e1", "e2a", "e2b", "e3"])

    def test_event_covered_by_overlapping_windows_returned_once_per_window(self):
        self.submit("s1", "e1", 1000.0)
        w1 = self.create_window("s1", 900.0, 1100.0)
        w2 = self.create_window("s1", 950.0, 1050.0)
        r1 = self.window_events(w1["id"])
        r2 = self.window_events(w2["id"])
        self.assertEqual(len(r1["fully"]), 1)
        self.assertEqual(len(r2["fully"]), 1)
        self.assertEqual(r1["fully"][0]["event_id"], "e1")
        self.assertEqual(r2["fully"][0]["event_id"], "e1")

    def test_segment_id_reflects_current_assignment_after_resegment(self):
        # 撤回导致拆段：覆盖事件的 segment_id 为当前实时归属
        self.submit("s1", "e1", 1000.0)
        self.submit("s1", "bridge", 1008.0)
        self.submit("s1", "e2", 1016.0)  # 桥接前同一段
        self.withdraw("s1", "bridge")
        w = self.create_window("s1", 0.0, 2000.0)
        r = self.window_events(w["id"])
        by_id = {e["event_id"]: e["segment_id"] for e in r["fully"]}
        self.assertNotEqual(by_id["e1"], by_id["e2"])  # 已拆成两个段

    def withdraw(self, source, event_id):
        with session(self.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            return service.withdraw_event(
                conn, source=source, event_id=event_id, silence=SILENCE
            )

    def test_window_events_runs_in_one_read_only_snapshot(self):
        self.submit("s1", "e1", 1000.0)
        w = self.create_window("s1", 900.0, 1100.0)
        # query_only 连接内执行纯读函数不报错即验证未发生任何写操作
        with read_only_session(self.db_path) as conn:
            r = service.list_maintenance_window_events(conn, w["id"])
        self.assertEqual(len(r["fully"]), 1)


class SegmentPatternsTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "test.db")
        init_db(self.db_path)

    def tearDown(self):
        self.tmp.cleanup()

    def submit(self, source, event_id, occurred_ts, severity="info"):
        with session(self.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            return service.ingest_event(
                conn,
                source=source,
                event_id=event_id,
                occurred_ts=occurred_ts,
                severity=severity,
                silence=SILENCE,
            )

    def withdraw(self, source, event_id):
        with session(self.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            return service.withdraw_event(
                conn, source=source, event_id=event_id, silence=SILENCE
            )

    def patterns(self, **kwargs):
        with read_only_session(self.db_path) as conn:
            return service.segment_patterns(conn, **kwargs)

    def test_fingerprint_canonicalizes_sequence(self):
        fp = service.pattern_fingerprint(("info", "warning", "error"))
        self.assertEqual(fp, service.pattern_fingerprint(("info", "warning", "error")))
        self.assertEqual(len(fp), 64)  # SHA-256 hex
        # 次序敏感、长度敏感
        self.assertNotEqual(
            service.pattern_fingerprint(("info", "error")),
            service.pattern_fingerprint(("error", "info")),
        )
        self.assertNotEqual(
            service.pattern_fingerprint(("info", "info")),
            service.pattern_fingerprint(("info",)),
        )

    def test_groups_by_fingerprint_across_segments_and_sources(self):
        # 间隔 20s > silence(10) → 两个段；序列同为 info→warning→error
        self.submit("s1", "a1", 1000.0, "info")
        self.submit("s1", "a2", 1002.0, "warning")
        self.submit("s1", "a3", 1004.0, "error")
        self.submit("s1", "b1", 1024.0, "info")
        self.submit("s1", "b2", 1026.0, "warning")
        self.submit("s1", "b3", 1028.0, "error")

        r = self.patterns(window_length=3)
        self.assertEqual(r["scanned_event_count"], 6)
        self.assertEqual(r["scanned_segment_count"], 2)
        self.assertEqual(len(r["patterns"]), 1)
        p = r["patterns"][0]
        self.assertEqual(p["pattern"], ["info", "warning", "error"])
        self.assertEqual(p["occurrence_count"], 2)
        self.assertEqual(len(p["segment_ids"]), 2)
        self.assertEqual(p["sources"], ["s1"])
        self.assertEqual(p["first_occurred_at"], service._iso(1000.0))
        self.assertEqual(p["last_occurred_at"], service._iso(1028.0))

    def test_window_walks_all_consecutive_subsequences_with_overlap(self):
        # 单段序列 info→warning→info：长度 2 的窗口为
        # (info,warning) 与 (warning,info)，各出现 1 次
        for eid, ts, level in [
            ("e1", 1000.0, "info"), ("e2", 1002.0, "warning"),
            ("e3", 1004.0, "info"),
        ]:
            self.submit("s1", eid, ts, level)
        r = self.patterns(window_length=2)
        patterns = {tuple(p["pattern"]): p for p in r["patterns"]}
        self.assertEqual(set(patterns), {("info", "warning"), ("warning", "info")})
        for p in patterns.values():
            self.assertEqual(p["occurrence_count"], 1)

    def test_repeated_overlapping_windows_are_each_counted(self):
        # info→info→info：长度 2 的两个窗口都是 (info,info)
        for eid, ts in [("e1", 1000.0), ("e2", 1002.0), ("e3", 1004.0)]:
            self.submit("s1", eid, ts)
        r = self.patterns(window_length=2)
        self.assertEqual(len(r["patterns"]), 1)
        self.assertEqual(r["patterns"][0]["pattern"], ["info", "info"])
        self.assertEqual(r["patterns"][0]["occurrence_count"], 2)

    def test_window_length_one_emits_every_event(self):
        for eid, ts, level in [
            ("e1", 1000.0, "info"), ("e2", 1002.0, "error"),
            ("e3", 1004.0, "info"),
        ]:
            self.submit("s1", eid, ts, level)
        r = self.patterns(window_length=1)
        by_pattern = {tuple(p["pattern"]): p for p in r["patterns"]}
        self.assertEqual(by_pattern[("info",)]["occurrence_count"], 2)
        self.assertEqual(by_pattern[("error",)]["occurrence_count"], 1)
        # info 跨两个窗口位置：首/末取极值；error 只出现一次，首末相同
        self.assertEqual(by_pattern[("info",)]["first_occurred_at"], service._iso(1000.0))
        self.assertEqual(by_pattern[("info",)]["last_occurred_at"], service._iso(1004.0))
        error_group = by_pattern[("error",)]
        self.assertEqual(error_group["first_occurred_at"], error_group["last_occurred_at"])

    def test_sequences_shorter_than_window_emit_nothing(self):
        self.submit("s1", "e1", 1000.0, "info")
        self.submit("s1", "e2", 1002.0, "error")
        r = self.patterns(window_length=3)
        self.assertEqual(r["patterns"], [])
        # 扫描计数仍反映实际数据
        self.assertEqual(r["scanned_event_count"], 2)
        self.assertEqual(r["scanned_segment_count"], 1)

    def test_same_timestamp_ordered_by_event_id(self):
        # 同刻乱序写入：序列次序由 event_id 稳定决定
        self.submit("s1", "c", 1000.0, "warning")
        self.submit("s1", "a", 1000.0, "info")
        self.submit("s1", "b", 1000.0, "error")
        r = self.patterns(window_length=3)
        self.assertEqual(len(r["patterns"]), 1)
        self.assertEqual(r["patterns"][0]["pattern"], ["info", "error", "warning"])
        first = self.patterns(window_length=3)
        second = self.patterns(window_length=3)
        self.assertEqual(first, second)  # 重复请求逐字节一致

    def test_patterns_sorted_by_fingerprint(self):
        for eid, ts, level in [
            ("e1", 1000.0, "info"), ("e2", 1002.0, "warning"),
            ("e3", 1004.0, "error"),
        ]:
            self.submit("s1", eid, ts, level)
        r = self.patterns(window_length=2)
        fingerprints = [p["fingerprint"] for p in r["patterns"]]
        self.assertEqual(fingerprints, sorted(fingerprints))

    def test_source_filter_and_inclusive_time_window(self):
        self.submit("s1", "e1", 1000.0, "info")
        self.submit("s1", "e2", 1002.0, "warning")
        self.submit("s2", "x", 1000.0, "error")

        r = self.patterns(source="s1", window_length=1)
        self.assertEqual(r["source"], "s1")
        self.assertEqual(r["scanned_event_count"], 2)
        self.assertEqual({tuple(p["pattern"]) for p in r["patterns"]},
                         {("info",), ("warning",)})

        # 闭区间边界：只保留 e2（恰好 1002.0）
        r = self.patterns(
            source="s1", start_ts=1002.0, end_ts=1002.0, window_length=1
        )
        self.assertEqual(r["start_at"], service._iso(1002.0))
        self.assertEqual(r["end_at"], service._iso(1002.0))
        self.assertEqual(r["scanned_event_count"], 1)
        self.assertEqual(r["patterns"][0]["pattern"], ["warning"])

        # 未知 source：空扫描
        r = self.patterns(source="ghost", window_length=1)
        self.assertEqual((r["scanned_event_count"], r["patterns"]), (0, []))

    def test_first_last_occurred_spans_windows_across_segments(self):
        self.submit("s1", "a1", 1000.0, "info")
        self.submit("s1", "a2", 1009.0, "error")
        self.submit("s1", "b1", 1100.0, "info")
        self.submit("s1", "b2", 1109.0, "error")
        r = self.patterns(window_length=2)
        self.assertEqual(len(r["patterns"]), 1)
        p = r["patterns"][0]
        self.assertEqual(p["first_occurred_at"], service._iso(1000.0))
        self.assertEqual(p["last_occurred_at"], service._iso(1109.0))
        self.assertEqual(p["occurrence_count"], 2)

    def test_reflects_current_segment_assignment_after_withdraw(self):
        # e1@1000、e2@1020 原为两段，bridge@1010 桥接合并
        self.submit("s1", "e1", 1000.0, "info")
        self.submit("s1", "e2", 1020.0, "error")
        self.submit("s1", "bridge", 1010.0, "warning")
        r = self.patterns(window_length=3)
        self.assertEqual(
            [p["pattern"] for p in r["patterns"]],
            [["info", "warning", "error"]],
        )
        # 撤回桥接事件 → 拆成两段，长度 3 的窗口不再存在
        self.withdraw("s1", "bridge")
        r = self.patterns(window_length=3)
        self.assertEqual(r["patterns"], [])
        r = self.patterns(window_length=1)
        counts = {tuple(p["pattern"]): p["occurrence_count"] for p in r["patterns"]}
        self.assertEqual(counts, {("info",): 1, ("error",): 1})

    def test_invalid_arguments_raise_value_error(self):
        self.submit("s1", "e1", 1000.0)
        for bad in (0, -1, 1001):
            with self.assertRaises(ValueError):
                self.patterns(window_length=bad)
        # bool 不是合法整数长度
        with self.assertRaises(ValueError):
            self.patterns(window_length=True)
        with self.assertRaises(ValueError):
            self.patterns(window_length=1, start_ts=1000.0, end_ts=999.0)
        with self.assertRaises(ValueError):
            self.patterns(window_length=1, start_ts=float("nan"))

    def test_read_only_snapshot_changes_nothing(self):
        self.submit("s1", "e1", 1000.0, "info")
        with session(self.db_path) as conn:
            before = conn.execute(
                "SELECT * FROM events ORDER BY id"
            ).fetchall()
            seg_before = conn.execute("SELECT * FROM segments").fetchall()
        self.patterns(window_length=1)
        self.patterns(source="s1", start_ts=0.0, end_ts=2000.0, window_length=2)
        with session(self.db_path) as conn:
            after = conn.execute("SELECT * FROM events ORDER BY id").fetchall()
            seg_after = conn.execute("SELECT * FROM segments").fetchall()
        self.assertEqual([tuple(r) for r in before], [tuple(r) for r in after])
        self.assertEqual(
            [tuple(r) for r in seg_before], [tuple(r) for r in seg_after]
        )


class SegmentDurationReportTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "test.db")
        init_db(self.db_path)

    def tearDown(self):
        self.tmp.cleanup()

    def submit(self, source, event_id, occurred_ts, severity="info"):
        with session(self.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            return service.ingest_event(
                conn,
                source=source,
                event_id=event_id,
                occurred_ts=occurred_ts,
                severity=severity,
                silence=SILENCE,
            )

    def withdraw(self, source, event_id):
        with session(self.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            return service.withdraw_event(
                conn, source=source, event_id=event_id, silence=SILENCE
            )

    def report(self, **kwargs):
        with read_only_session(self.db_path) as conn:
            return service.segment_duration_report(conn, **kwargs)

    def _three_segments(self):
        # SILENCE=10：段 A [0,5]（5s）、段 B [20,20]（0s）、段 C [40,48]（8s）
        self.submit("s1", "e1", 0.0, "warning")
        self.submit("s1", "e2", 5.0, "error")
        self.submit("s1", "e3", 20.0, "debug")
        self.submit("s1", "e4", 40.0, "critical")
        self.submit("s1", "e5", 48.0, "info")

    def test_stats_and_buckets(self):
        self._three_segments()
        r = self.report(source="s1", bucket_seconds=2.0)
        self.assertEqual(r["source"], "s1")
        self.assertIsNone(r["start_at"])
        self.assertIsNone(r["end_at"])
        self.assertEqual(r["bucket_seconds"], 2.0)
        self.assertEqual(r["sample_count"], 3)
        # 时长 [0, 5, 8]
        self.assertEqual(r["min_seconds"], 0.0)
        self.assertEqual(r["max_seconds"], 8.0)
        self.assertEqual(r["mean_seconds"], 4.333333)  # 13/3 取整到微秒
        self.assertEqual(r["median_seconds"], 5.0)
        self.assertEqual(r["p90_seconds"], 7.4)  # 5 + 0.8 * (8 - 5)
        # 桶宽 2：[0,2) [2,4) [4,6) [6,8) [8,10)，空桶同样输出
        self.assertEqual(
            r["buckets"],
            [
                {"bucket_start_seconds": 0.0, "bucket_end_seconds": 2.0,
                 "segment_count": 1},
                {"bucket_start_seconds": 2.0, "bucket_end_seconds": 4.0,
                 "segment_count": 0},
                {"bucket_start_seconds": 4.0, "bucket_end_seconds": 6.0,
                 "segment_count": 1},
                {"bucket_start_seconds": 6.0, "bucket_end_seconds": 8.0,
                 "segment_count": 0},
                {"bucket_start_seconds": 8.0, "bucket_end_seconds": 10.0,
                 "segment_count": 1},
            ],
        )
        total = sum(b["segment_count"] for b in r["buckets"])
        self.assertEqual(total, r["sample_count"])

    def test_even_sample_median_and_p90_interpolation(self):
        # 时长 [5, 8]：中位数为两者均值，P90 线性插值
        self.submit("s1", "e1", 0.0)
        self.submit("s1", "e2", 5.0)
        self.submit("s1", "e3", 40.0)
        self.submit("s1", "e4", 48.0)
        r = self.report(bucket_seconds=10.0)
        self.assertEqual(r["sample_count"], 2)
        self.assertEqual(r["median_seconds"], 6.5)
        self.assertEqual(r["p90_seconds"], 7.7)  # 5 + 0.9 * (8 - 5)
        self.assertEqual(r["mean_seconds"], 6.5)

    def test_bucket_boundary_is_left_closed_right_open(self):
        # 时长恰好等于桶宽整倍数：落入右侧桶
        self.submit("s1", "e1", 0.0)
        self.submit("s1", "e2", 10.0)  # 时长 10s，桶宽 5 → 第 3 桶 [10,15)
        r = self.report(bucket_seconds=5.0)
        self.assertEqual(
            [b["segment_count"] for b in r["buckets"]], [0, 0, 1]
        )
        self.assertEqual(r["buckets"][2]["bucket_start_seconds"], 10.0)
        self.assertEqual(r["buckets"][2]["bucket_end_seconds"], 15.0)

    def test_source_filter_and_overlap_window(self):
        self.submit("s1", "e1", 0.0)
        self.submit("s1", "e2", 5.0)      # s1 段 A [0,5]
        self.submit("s2", "x1", 100.0)
        self.submit("s2", "x2", 108.0)    # s2 段 X [100,108]

        r = self.report(source="s1", bucket_seconds=10.0)
        self.assertEqual(r["sample_count"], 1)
        self.assertEqual(r["max_seconds"], 5.0)

        # 段终点不早于 start_ts 才算相交
        r = self.report(start_ts=6.0, bucket_seconds=10.0)
        self.assertEqual(r["sample_count"], 1)
        self.assertEqual(r["max_seconds"], 8.0)
        # 段起点不晚于 end_ts 才算相交
        r = self.report(end_ts=5.0, bucket_seconds=10.0)
        self.assertEqual(r["sample_count"], 1)
        self.assertEqual(r["max_seconds"], 5.0)
        # 闭区间边界：段终点 == start_ts、段起点 == end_ts 均算相交
        r = self.report(start_ts=5.0, end_ts=100.0, bucket_seconds=10.0)
        self.assertEqual(r["sample_count"], 2)
        self.assertEqual(r["min_seconds"], 5.0)
        self.assertEqual(r["max_seconds"], 8.0)
        self.assertEqual(r["start_at"], service._iso(5.0))
        self.assertEqual(r["end_at"], service._iso(100.0))

    def test_empty_result_returns_null_stats_and_no_buckets(self):
        self.submit("s1", "e1", 0.0)
        r = self.report(source="ghost", bucket_seconds=10.0)
        self.assertEqual(
            r,
            {
                "source": "ghost",
                "start_at": None,
                "end_at": None,
                "bucket_seconds": 10.0,
                "sample_count": 0,
                "min_seconds": None,
                "max_seconds": None,
                "mean_seconds": None,
                "median_seconds": None,
                "p90_seconds": None,
                "buckets": [],
            },
        )

    def test_single_event_segment_has_zero_duration(self):
        self.submit("s1", "e1", 1000.0)
        r = self.report(bucket_seconds=60.0)
        self.assertEqual(r["sample_count"], 1)
        self.assertEqual(r["min_seconds"], 0.0)
        self.assertEqual(r["max_seconds"], 0.0)
        self.assertEqual(r["mean_seconds"], 0.0)
        self.assertEqual(r["median_seconds"], 0.0)
        self.assertEqual(r["p90_seconds"], 0.0)
        self.assertEqual(len(r["buckets"]), 1)
        self.assertEqual(r["buckets"][0]["segment_count"], 1)

    def test_fractional_duration_and_subsecond_buckets(self):
        self.submit("s1", "e1", 1000.25)
        self.submit("s1", "e2", 1000.75)  # 时长 0.5s
        r = self.report(bucket_seconds=0.25)
        self.assertEqual(r["min_seconds"], 0.5)
        self.assertEqual(r["max_seconds"], 0.5)
        # 0.5/0.25 = 2 → 第 3 桶 [0.5, 0.75)
        self.assertEqual(
            [b["segment_count"] for b in r["buckets"]], [0, 0, 1]
        )
        self.assertEqual(r["buckets"][2]["bucket_start_seconds"], 0.5)
        self.assertEqual(r["buckets"][2]["bucket_end_seconds"], 0.75)

    def test_reflects_current_segments_after_withdraw(self):
        self.submit("s1", "e1", 0.0)
        self.submit("s1", "e2", 20.0)
        self.submit("s1", "bridge", 10.0)  # 桥接 → 单段 [0,20]，时长 20s
        r = self.report(bucket_seconds=10.0)
        self.assertEqual(r["sample_count"], 1)
        self.assertEqual(r["max_seconds"], 20.0)
        self.withdraw("s1", "bridge")      # 拆成两个 0s 段
        r = self.report(bucket_seconds=10.0)
        self.assertEqual(r["sample_count"], 2)
        self.assertEqual(r["max_seconds"], 0.0)

    def test_invalid_arguments_raise_value_error(self):
        self.submit("s1", "e1", 0.0)
        with self.assertRaises(ValueError):
            self.report(bucket_seconds=10.0, start_ts=100.0, end_ts=99.0)
        with self.assertRaises(ValueError):
            self.report(bucket_seconds=10.0, start_ts=float("nan"))
        with self.assertRaises(ValueError):
            self.report(bucket_seconds=10.0, end_ts=float("inf"))
        for bad in (0.0, -1.0, float("nan"), float("inf"), True):
            with self.assertRaises(ValueError):
                self.report(bucket_seconds=bad)
        with self.assertRaises(ValueError):
            self.report(bucket_seconds=1e-7)  # 不足 1 微秒

    def test_bucket_count_capped(self):
        # 链式事件构成时长 100s 的单段：100/0.01 → 10001 桶拒绝，9901 桶允许
        for i in range(11):
            self.submit("s1", f"e{i}", float(i) * 10.0)
        with self.assertRaises(ValueError):
            self.report(bucket_seconds=0.01)
        r = self.report(bucket_seconds=0.0101)
        self.assertEqual(len(r["buckets"]), 9901)
        # 无匹配段时不生成桶，极小桶长也不报错
        r = self.report(source="ghost", bucket_seconds=0.000001)
        self.assertEqual(r["buckets"], [])

    def test_reads_consistent_snapshot(self):
        self.submit("s1", "e1", 0.0)
        with read_only_session(self.db_path) as conn:
            kwargs = dict(source="s1", bucket_seconds=10.0)
            r1 = service.segment_duration_report(conn, **kwargs)
            self.submit("s1", "e2", 5.0)  # 快照开启后写入
            r2 = service.segment_duration_report(conn, **kwargs)
        self.assertEqual(r1, r2)
        self.assertEqual(r2["sample_count"], 1)

    def test_does_not_mutate_data(self):
        self._three_segments()
        with session(self.db_path) as conn:
            segs_before = service.list_segments(conn)
            changes_before = service.list_segment_changes(conn)
        self.report(bucket_seconds=2.0)
        self.report(source="s1", start_ts=0.0, end_ts=100.0, bucket_seconds=1.0)
        with session(self.db_path) as conn:
            self.assertEqual(service.list_segments(conn), segs_before)
            self.assertEqual(service.list_segment_changes(conn), changes_before)


if __name__ == "__main__":
    unittest.main()
