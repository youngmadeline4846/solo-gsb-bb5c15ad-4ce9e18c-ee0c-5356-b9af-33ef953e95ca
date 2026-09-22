"""FastAPI 接口测试（需要 fastapi/httpx，随 requirements-dev.txt 安装）：

    pytest tests/test_api.py -v
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from app.config import Settings  # noqa: E402
from app.main import create_app  # noqa: E402


@pytest.fixture()
def client(tmp_path):
    settings = Settings(
        database_path=str(tmp_path / "api.db"),
        silence_interval_seconds=60.0,
    )
    with TestClient(create_app(settings)) as c:
        yield c


def test_submit_and_get_segment(client):
    resp = client.post("/events", json={
        "source": "gw-01", "event_id": "e1",
        "occurred_at": "2026-09-20T08:00:00Z", "severity": "warning",
    })
    assert resp.status_code == 200
    body = resp.json()
    assert body["deduplicated"] is False
    seg = body["segment"]
    assert seg["event_count"] == 1
    assert seg["revision"] == 1
    assert seg["max_severity"] == "warning"
    assert seg["started_at"] == seg["ended_at"]

    got = client.get(f"/segments/{seg['id']}")
    assert got.status_code == 200
    assert got.json() == seg


def test_duplicate_submission_keeps_count(client):
    payload = {
        "source": "gw-01", "event_id": "e1",
        "occurred_at": "2026-09-20T08:00:00Z", "severity": "info",
    }
    first = client.post("/events", json=payload).json()
    second = client.post("/events", json=payload).json()
    assert second["deduplicated"] is True
    assert second["segment"]["event_count"] == first["segment"]["event_count"] == 1


def test_late_out_of_order_event_bumps_revision(client):
    client.post("/events", json={
        "source": "gw-01", "event_id": "e1",
        "occurred_at": "2026-09-20T08:00:30Z", "severity": "info",
    })
    late = client.post("/events", json={
        "source": "gw-01", "event_id": "e2",
        "occurred_at": "2026-09-20T08:00:00Z", "severity": "error",
    }).json()
    seg = late["segment"]
    assert seg["revision"] == 2
    assert seg["max_severity"] == "error"
    # pydantic 不同版本对 UTC 可能输出 Z 或 +00:00，统一归一化比较
    assert seg["started_at"].replace("Z", "+00:00") == "2026-09-20T08:00:00+00:00"
    assert seg["ended_at"].replace("Z", "+00:00") == "2026-09-20T08:00:30+00:00"
    assert seg["event_count"] == 2


def test_list_segments_filter_by_source(client):
    for source, eid in [("a", "e1"), ("b", "e2"), ("a", "e3")]:
        client.post("/events", json={
            "source": source, "event_id": eid,
            "occurred_at": "2026-09-20T08:00:00Z", "severity": "info",
        })
    assert len(client.get("/segments").json()) == 2
    segs = client.get("/segments", params={"source": "a"}).json()
    assert len(segs) == 1
    assert segs[0]["event_count"] == 2


def test_invalid_severity_rejected(client):
    resp = client.post("/events", json={
        "source": "gw-01", "event_id": "e1",
        "occurred_at": "2026-09-20T08:00:00Z", "severity": "fatal-error",
    })
    assert resp.status_code == 422


def test_missing_field_rejected(client):
    resp = client.post("/events", json={"source": "gw-01", "event_id": "e1"})
    assert resp.status_code == 422


def test_get_missing_segment_404(client):
    assert client.get("/segments/9999").status_code == 404


def _post(client, source, event_id, occurred_at, severity="info"):
    resp = client.post("/events", json={
        "source": source, "event_id": event_id,
        "occurred_at": occurred_at, "severity": severity,
    })
    assert resp.status_code == 200
    return resp.json()


def test_segment_changes_empty_initially(client):
    assert client.get("/segment-changes").json() == {"changes": [], "next_after": 0}


def test_segment_changes_upsert_rules(client):
    _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z", "warning")  # 创建 → upsert
    _post(client, "gw-01", "e2", "2026-09-20T08:00:30Z", "error")    # 边界+级别 → upsert
    _post(client, "gw-01", "e3", "2026-09-20T08:00:10Z", "debug")    # 仅计数 → 无变更
    _post(client, "gw-01", "e2", "2026-09-20T08:00:30Z", "error")    # 幂等重放 → 无变更

    page = client.get("/segment-changes").json()
    assert [c["type"] for c in page["changes"]] == ["upsert", "upsert"]
    first, second = page["changes"]
    assert first["change_id"] < second["change_id"]
    assert first["revision"] == 1
    assert second["revision"] == 2
    assert second["segment"]["max_severity"] == "error"
    assert second["segment"]["event_count"] == 2  # 变更时刻的快照（e3 未产生记录）
    assert page["next_after"] == second["change_id"]

    seg = client.get(f"/segments/{second['segment_id']}").json()
    assert seg["event_count"] == 3  # 段最终计数包含 e3


def test_segment_changes_merge_tombstone(client):
    a = _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z")["segment"]
    b = _post(client, "gw-01", "e2", "2026-09-20T08:02:00Z")["segment"]  # 超静默 → 新段
    _post(client, "gw-01", "e3", "2026-09-20T08:01:00Z")                 # 桥接合并

    page = client.get("/segment-changes").json()
    assert [c["type"] for c in page["changes"]] == [
        "upsert", "upsert", "merged", "upsert",
    ]
    tomb = page["changes"][2]
    assert tomb["segment_id"] == b["id"]
    assert tomb["merged_into"] == a["id"]
    assert tomb["segment"] is None
    final = page["changes"][3]
    assert final["segment_id"] == a["id"]
    assert final["segment"]["event_count"] == 3


def test_segment_changes_pagination_while_ingesting(client):
    for i in range(3):  # 不同 source → 3 个独立段 → 3 条 upsert
        _post(client, f"gw-{i:02d}", "e1", "2026-09-20T08:00:00Z")

    p1 = client.get("/segment-changes", params={"after": 0, "limit": 2}).json()
    assert len(p1["changes"]) == 2
    assert p1["next_after"] == p1["changes"][-1]["change_id"]

    _post(client, "gw-03", "e1", "2026-09-20T08:00:00Z")  # 分页期间新写入

    p2 = client.get("/segment-changes", params={"after": p1["next_after"], "limit": 100})
    p2 = p2.json()
    ids = [c["change_id"] for c in p1["changes"] + p2["changes"]]
    assert ids == sorted(ids) and len(set(ids)) == 4  # 不漏读、不重复
    assert p2["next_after"] == ids[-1]

    # 空页：next_after 保持不变，可直接继续轮询
    p3 = client.get("/segment-changes", params={"after": p2["next_after"]}).json()
    assert p3 == {"changes": [], "next_after": p2["next_after"]}


def test_segment_changes_param_validation(client):
    assert client.get("/segment-changes", params={"after": -1}).status_code == 422
    assert client.get("/segment-changes", params={"limit": 0}).status_code == 422
    assert client.get("/segment-changes", params={"limit": 1001}).status_code == 422


def test_preview_split_merge_and_unchanged(client):
    # silence=60：08:00:00、08:00:30 同段；08:02:00、08:02:30 另一段
    _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z", "warning")
    _post(client, "gw-01", "e2", "2026-09-20T08:00:30Z", "critical")
    _post(client, "gw-01", "e3", "2026-09-20T08:02:00Z", "info")
    _post(client, "gw-01", "e4", "2026-09-20T08:02:30Z", "debug")

    # 更小间隔 15s：两段各被拆开 → 4 个模拟段，2 个 split
    split = client.get(
        "/segments/resegmentation-preview",
        params={"source": "gw-01", "silence_interval_seconds": 15},
    )
    assert split.status_code == 200
    body = split.json()
    assert body["source"] == "gw-01"
    assert body["silence_interval_seconds"] == 15.0
    assert body["event_count"] == 4
    sims = body["simulated_segments"]
    assert [s["temp_id"] for s in sims] == [1, 2, 3, 4]
    assert all(s["event_count"] == 1 for s in sims)
    assert sims[0]["max_severity"] == "warning"
    assert sims[1]["max_severity"] == "critical"
    assert [m["relation"] for m in body["mappings"]] == ["split", "split"]
    assert body["mappings"][0]["simulated_temp_ids"] == [1, 2]
    assert body["mappings"][1]["simulated_temp_ids"] == [3, 4]
    summary = body["summary"]
    assert summary["split_existing_segment_count"] == 2
    assert summary["merged_existing_segment_count"] == 0
    assert summary["unchanged_existing_segment_count"] == 0
    assert summary["changed_existing_segment_count"] == 2
    assert summary["existing_segment_count"] == 2
    assert summary["simulated_segment_count"] == 4

    # 更大间隔 600s：合并为 1 个模拟段，2 个 merged
    merged = client.get(
        "/segments/resegmentation-preview",
        params={"source": "gw-01", "silence_interval_seconds": 600},
    ).json()
    assert len(merged["simulated_segments"]) == 1
    merged_seg = merged["simulated_segments"][0]
    assert merged_seg["temp_id"] == 1
    assert merged_seg["event_count"] == 4
    assert merged_seg["max_severity"] == "critical"
    assert merged_seg["started_at"].replace("Z", "+00:00") == "2026-09-20T08:00:00+00:00"
    assert merged_seg["ended_at"].replace("Z", "+00:00") == "2026-09-20T08:02:30+00:00"
    assert [m["relation"] for m in merged["mappings"]] == ["merged", "merged"]
    assert all(m["simulated_temp_ids"] == [1] for m in merged["mappings"])
    assert merged["summary"]["merged_existing_segment_count"] == 2
    assert merged["summary"]["changed_existing_segment_count"] == 2

    # 与生效间隔相同（60s）：全部 unchanged
    same = client.get(
        "/segments/resegmentation-preview",
        params={"source": "gw-01", "silence_interval_seconds": 60},
    ).json()
    assert len(same["simulated_segments"]) == 2
    assert all(m["relation"] == "unchanged" for m in same["mappings"])
    assert same["summary"]["changed_existing_segment_count"] == 0
    assert same["summary"]["unchanged_existing_segment_count"] == 2


def test_preview_deterministic_and_read_only(client):
    for i, ts in enumerate(
        ["08:00:00", "08:00:30", "08:02:00", "08:02:30", "08:05:00"]
    ):
        _post(client, "gw-01", f"e{i}", f"2026-09-20T{ts}Z")

    params = {"source": "gw-01", "silence_interval_seconds": 45}
    first = client.get("/segments/resegmentation-preview", params=params).json()
    second = client.get("/segments/resegmentation-preview", params=params).json()
    assert first == second  # 重复请求确定一致

    # 预演不改变现有段与变更流
    segs = client.get("/segments", params={"source": "gw-01"}).json()
    changes = client.get("/segment-changes").json()
    client.get("/segments/resegmentation-preview", params=params)
    client.get(
        "/segments/resegmentation-preview",
        params={"source": "gw-01", "silence_interval_seconds": 1},
    )
    assert client.get("/segments", params={"source": "gw-01"}).json() == segs
    assert client.get("/segment-changes").json() == changes


def test_preview_unknown_source_empty(client):
    body = client.get(
        "/segments/resegmentation-preview",
        params={"source": "ghost", "silence_interval_seconds": 60},
    ).json()
    assert body["event_count"] == 0
    assert body["simulated_segments"] == []
    assert body["mappings"] == []
    assert body["summary"]["existing_segment_count"] == 0
    assert body["summary"]["simulated_segment_count"] == 0


def test_preview_param_validation(client):
    # 缺 source
    r = client.get(
        "/segments/resegmentation-preview",
        params={"silence_interval_seconds": 60},
    )
    assert r.status_code == 422
    # 负间隔
    r = client.get(
        "/segments/resegmentation-preview",
        params={"source": "gw-01", "silence_interval_seconds": -1},
    )
    assert r.status_code == 422
    # 非数字间隔
    r = client.get(
        "/segments/resegmentation-preview",
        params={"source": "gw-01", "silence_interval_seconds": "soon"},
    )
    assert r.status_code == 422
    # 零允许（同刻事件才同段）
    r = client.get(
        "/segments/resegmentation-preview",
        params={"source": "gw-01", "silence_interval_seconds": 0},
    )
    assert r.status_code == 200


def test_preview_route_not_captured_by_segment_id(client):
    # 静态路径必须优先于 /segments/{segment_id} 匹配，不能返回 422
    r = client.get(
        "/segments/resegmentation-preview",
        params={"source": "gw-01", "silence_interval_seconds": 60},
    )
    assert r.status_code == 200


def test_healthz(client):
    assert client.get("/healthz").json() == {"status": "ok"}


# ---- 误报事件撤回 DELETE /events/{source}/{event_id} ----

def _delete(client, source, event_id):
    return client.delete(f"/events/{source}/{event_id}")


def test_withdraw_unknown_event_returns_404(client):
    resp = _delete(client, "gw-01", "ghost")
    assert resp.status_code == 404
    # 存在该 event_id 但 source 不同：同样 404
    _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z")
    before = client.get("/segment-changes").json()
    assert _delete(client, "gw-02", "e1").status_code == 404
    # 404 不产生任何变更
    assert client.get("/segment-changes").json() == before


def test_withdraw_sole_event_deletes_segment(client):
    seg = _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z", "warning")["segment"]
    resp = _delete(client, "gw-01", "e1")
    assert resp.status_code == 200
    assert resp.json() == {
        "source": "gw-01",
        "event_id": "e1",
        "result": "deleted_segment",
        "segments": [],
    }
    # 原段已不存在；重复撤回返回 404
    assert client.get(f"/segments/{seg['id']}").status_code == 404
    assert _delete(client, "gw-01", "e1").status_code == 404
    page = client.get("/segment-changes").json()
    assert [c["type"] for c in page["changes"]] == ["upsert", "deleted"]
    tomb = page["changes"][-1]
    assert tomb["segment_id"] == seg["id"]
    assert tomb["merged_into"] is None
    assert tomb["segment"] is None
    # 撤回后可重新提交同一事件（幂等键已释放）
    again = _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z")
    assert again["deduplicated"] is False


def test_withdraw_inner_event_shrinks_segment_and_bumps_revision(client):
    first = _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z", "critical")
    _post(client, "gw-01", "e2", "2026-09-20T08:00:30Z", "info")
    _post(client, "gw-01", "e3", "2026-09-20T08:00:10Z", "debug")  # 段内，rev 不变
    resp = _delete(client, "gw-01", "e3")
    assert resp.status_code == 200
    body = resp.json()
    assert body["result"] == "shrunk_segment"
    seg = body["segments"][0]
    assert seg["id"] == first["segment"]["id"]  # 沿用原 id
    assert seg["revision"] == 3                  # rev=2 → 3（边界不变也必须递增）
    assert seg["event_count"] == 2
    assert seg["max_severity"] == "critical"
    assert client.get(f"/segments/{seg['id']}").json()["event_count"] == 2


def test_withdraw_recomputes_max_severity(client):
    _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z", "info")
    _post(client, "gw-01", "e2", "2026-09-20T08:00:30Z", "critical")
    body = _delete(client, "gw-01", "e2").json()
    assert body["segments"][0]["max_severity"] == "info"


def test_withdraw_bridge_event_splits_segment(client):
    a = _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z", "error")["segment"]
    b = _post(client, "gw-01", "e2", "2026-09-20T08:02:00Z", "warning")["segment"]
    bridged = _post(client, "gw-01", "e3", "2026-09-20T08:01:00Z", "critical")["segment"]

    resp = _delete(client, "gw-01", "e3")
    assert resp.status_code == 200
    body = resp.json()
    assert body["result"] == "split_segment"
    kept, new = body["segments"]
    # 最早子段沿用原 id，revision 在桥接合并后的值上递增
    assert kept["id"] == a["id"]
    assert kept["revision"] == bridged["revision"] + 1
    assert kept["event_count"] == 1
    assert kept["max_severity"] == "error"
    assert kept["started_at"].replace("Z", "+00:00") == "2026-09-20T08:00:00+00:00"
    # 其余子段新建（不用被合并段 B 的 id），revision 从 1 开始
    assert new["id"] not in (a["id"], b["id"])
    assert new["revision"] == 1
    assert new["event_count"] == 1
    assert new["max_severity"] == "warning"
    assert new["started_at"].replace("Z", "+00:00") == "2026-09-20T08:02:00+00:00"

    segs = client.get("/segments", params={"source": "gw-01"}).json()
    assert len(segs) == 2

    # 变更流：保留段 upsert 在前、新段 upsert 在后，change_id 有序
    page = client.get("/segment-changes").json()
    tail = page["changes"][-2:]
    assert [c["type"] for c in tail] == ["upsert", "upsert"]
    assert tail[0]["segment_id"] == kept["id"]
    assert tail[0]["revision"] == kept["revision"]
    assert tail[1]["segment_id"] == new["id"]
    assert tail[1]["revision"] == 1
    assert tail[0]["change_id"] < tail[1]["change_id"]


def test_withdraw_is_isolated_by_source(client):
    _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z")
    _post(client, "gw-01", "e2", "2026-09-20T08:02:00Z")
    _post(client, "gw-01", "e3", "2026-09-20T08:01:00Z")
    other = _post(client, "gw-02", "e9", "2026-09-20T08:00:05Z", "critical")
    _delete(client, "gw-01", "e3")  # 拆 gw-01
    seg = client.get(f"/segments/{other['segment']['id']}").json()
    assert seg == other["segment"]


def test_withdraw_then_reingest_rebridges(client):
    a = _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z")["segment"]
    _post(client, "gw-01", "e2", "2026-09-20T08:02:00Z")
    _post(client, "gw-01", "e3", "2026-09-20T08:01:00Z")
    _delete(client, "gw-01", "e3")
    again = _post(client, "gw-01", "e3", "2026-09-20T08:01:00Z", "critical")
    assert again["deduplicated"] is False
    assert again["segment"]["id"] == a["id"]
    assert again["segment"]["event_count"] == 3
    assert len(client.get("/segments", params={"source": "gw-01"}).json()) == 1


def test_withdraw_path_params_are_url_segments(client):
    # 路径段支持 source/event_id 允许的 URL 安全字符
    _post(client, "edge-gateway.01", "evt-0001_x", "2026-09-20T08:00:00Z")
    resp = client.delete("/events/edge-gateway.01/evt-0001_x")
    assert resp.status_code == 200
    assert resp.json()["result"] == "deleted_segment"
    # 缺失路径段不会误匹配到该路由（交由 404/405 语义）
    assert client.delete("/events/edge-gateway.01").status_code in (404, 405)


# ---- 事件审计查询 GET /events ----

def test_get_events_stable_order_and_fields(client):
    # 乱序写入、含同刻事件
    _post(client, "gw-01", "c", "2026-09-20T08:00:20Z", "critical")
    _post(client, "gw-01", "b", "2026-09-20T08:00:00Z", "error")
    _post(client, "gw-01", "a2", "2026-09-20T08:00:00Z", "warning")
    _post(client, "gw-01", "a1", "2026-09-20T08:00:00Z", "info")
    _post(client, "gw-02", "x", "2026-09-20T08:00:00Z")  # 其他 source 不出现

    page = client.get("/events", params={"source": "gw-01"}).json()
    assert [e["event_id"] for e in page["events"]] == ["a1", "a2", "b", "c"]
    e = page["events"][0]
    assert set(e) == {"source", "event_id", "occurred_at", "severity", "segment_id"}
    assert e["source"] == "gw-01"
    assert e["occurred_at"].replace("Z", "+00:00") == "2026-09-20T08:00:00+00:00"
    assert isinstance(e["segment_id"], int)
    assert page["next_after"]


def test_get_events_segment_id_tracks_current_segment(client):
    a = _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z")["segment"]
    _post(client, "gw-01", "e2", "2026-09-20T08:02:00Z")
    _post(client, "gw-01", "e3", "2026-09-20T08:01:00Z")  # 桥接合并
    page = client.get("/events", params={"source": "gw-01"}).json()
    assert {e["event_id"]: e["segment_id"] for e in page["events"]} == {
        "e1": a["id"], "e2": a["id"], "e3": a["id"],
    }
    # 撤回桥接事件拆段后，segment_id 返回重算后的实时归属
    client.delete("/events/gw-01/e3")
    page = client.get("/events", params={"source": "gw-01"}).json()
    sids = {e["event_id"]: e["segment_id"] for e in page["events"]}
    assert sids["e1"] == a["id"]
    assert sids["e2"] != a["id"]


def test_get_events_time_window_inclusive(client):
    for eid, ts in [
        ("a", "08:00:00"), ("b", "08:00:30"), ("c", "08:02:00"), ("d", "08:02:30"),
    ]:
        _post(client, "gw-01", eid, f"2026-09-20T{ts}Z")
    body = client.get(
        "/events",
        params={
            "source": "gw-01",
            "start_at": "2026-09-20T08:00:30Z",
            "end_at": "2026-09-20T08:02:00Z",
        },
    ).json()
    assert [e["event_id"] for e in body["events"]] == ["b", "c"]  # 边界包含

    only_start = client.get(
        "/events", params={"source": "gw-01", "start_at": "2026-09-20T08:02:00Z"}
    ).json()
    assert [e["event_id"] for e in only_start["events"]] == ["c", "d"]


def test_get_events_severity_multi_filter(client):
    for eid, s in [("a", "info"), ("b", "error"), ("c", "critical"), ("d", "warning")]:
        _post(client, "gw-01", eid, "2026-09-20T08:00:00Z", s)
    # 重复参数多值；大小写/别名归一化；重复值等价去重
    body = client.get(
        "/events",
        params=[
            ("source", "gw-01"),
            ("severity", "ERROR"),
            ("severity", "fatal"),
            ("severity", "critical"),
        ],
    ).json()
    assert [e["event_id"] for e in body["events"]] == ["b", "c"]
    assert all(e["severity"] in ("error", "critical") for e in body["events"])


def test_get_events_combined_filters_and_source_isolation(client):
    _post(client, "gw-01", "a", "2026-09-20T08:00:00Z", "info")
    _post(client, "gw-01", "b", "2026-09-20T08:00:30Z", "error")
    _post(client, "gw-02", "c", "2026-09-20T08:00:00Z", "error")
    body = client.get(
        "/events",
        params=[("source", "gw-01"), ("severity", "error")],
    ).json()
    assert [e["event_id"] for e in body["events"]] == ["b"]
    assert client.get("/events", params={"source": "ghost"}).json() == {
        "events": [], "next_after": None,
    }


def test_get_events_pagination_walks_all_pages(client):
    for i in range(5):
        _post(client, "gw-01", f"e{i}", f"2026-09-20T08:00:0{i}Z")
    p1 = client.get("/events", params={"source": "gw-01", "limit": 2}).json()
    assert [e["event_id"] for e in p1["events"]] == ["e0", "e1"]
    p2 = client.get(
        "/events", params={"source": "gw-01", "after": p1["next_after"], "limit": 2}
    ).json()
    assert [e["event_id"] for e in p2["events"]] == ["e2", "e3"]
    p3 = client.get(
        "/events", params={"source": "gw-01", "after": p2["next_after"], "limit": 2}
    ).json()
    assert [e["event_id"] for e in p3["events"]] == ["e4"]
    # 空页：游标保持不变
    p4 = client.get(
        "/events", params={"source": "gw-01", "after": p3["next_after"], "limit": 2}
    ).json()
    assert p4 == {"events": [], "next_after": p3["next_after"]}
    ids = [e["event_id"] for p in (p1, p2, p3) for e in p["events"]]
    assert ids == [f"e{i}" for i in range(5)]  # 不漏不重


def test_get_events_cursor_respects_same_timestamp_tiebreak(client):
    for eid in ("b", "a", "c"):
        _post(client, "gw-01", eid, "2026-09-20T08:00:00Z")
    p1 = client.get("/events", params={"source": "gw-01", "limit": 1}).json()
    assert p1["events"][0]["event_id"] == "a"
    p2 = client.get(
        "/events", params={"source": "gw-01", "after": p1["next_after"]}
    ).json()
    assert [e["event_id"] for e in p2["events"]] == ["b", "c"]


def test_get_events_cursor_is_filter_stable(client):
    # 游标是 (occurred_at, event_id) 位置：配合不同过滤条件只返回该位置之后的行
    for eid, s in [("a", "info"), ("b", "error"), ("c", "info")]:
        _post(client, "gw-01", eid, "2026-09-20T08:00:00Z", s)
    full = client.get(
        "/events",
        params=[("source", "gw-01"), ("severity", "info"), ("limit", "1")],
    ).json()
    assert full["events"][0]["event_id"] == "a"
    rest = client.get(
        "/events",
        params=[
            ("source", "gw-01"), ("severity", "info"),
            ("after", full["next_after"]),
        ],
    ).json()
    assert [e["event_id"] for e in rest["events"]] == ["c"]


def test_get_events_does_not_conflict_with_post_or_withdraw(client):
    # GET /events 与 POST /events 同路径不同方法共存
    assert client.post("/events", json={
        "source": "gw-01", "event_id": "e1",
        "occurred_at": "2026-09-20T08:00:00Z", "severity": "info",
    }).status_code == 200
    assert client.get("/events", params={"source": "gw-01"}).status_code == 200
    assert client.delete("/events/gw-01/e1").status_code == 200
    assert client.get("/events", params={"source": "gw-01"}).json()["events"] == []


def test_get_events_param_validation(client):
    # 缺 source
    assert client.get("/events").status_code == 422
    # 空 source
    assert client.get("/events", params={"source": ""}).status_code == 422
    # limit 越界
    assert client.get("/events", params={"source": "g", "limit": 0}).status_code == 422
    assert client.get("/events", params={"source": "g", "limit": 1001}).status_code == 422
    # 非法 severity
    r = client.get("/events", params=[("source", "g"), ("severity", "bogus")])
    assert r.status_code == 422
    # 非法时间
    assert client.get(
        "/events", params={"source": "g", "start_at": "soon"}
    ).status_code == 422
    # start_at 晚于 end_at
    assert client.get(
        "/events",
        params={
            "source": "g",
            "start_at": "2026-09-20T09:00:00Z",
            "end_at": "2026-09-20T08:00:00Z",
        },
    ).status_code == 422
    # 伪造游标
    assert client.get(
        "/events", params={"source": "g", "after": "@@not-base64@@"}
    ).status_code == 422
    assert client.get(
        "/events", params={"source": "g", "after": "WzFd"}  # base64("[1]") 结构非法
    ).status_code == 422
    # limit 默认上限内正常
    assert client.get("/events", params={"source": "g", "limit": 1000}).status_code == 200


def test_get_events_naive_datetime_treated_as_utc(client):
    _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z")
    body = client.get(
        "/events",
        params={"source": "gw-01", "start_at": "2026-09-20 08:00:00"},
    ).json()
    assert [e["event_id"] for e in body["events"]] == ["e1"]


def test_get_events_is_read_only(client):
    _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z")
    segs = client.get("/segments").json()
    changes = client.get("/segment-changes").json()
    for _ in range(3):
        client.get("/events", params={"source": "gw-01"})
    assert client.get("/segments").json() == segs
    assert client.get("/segment-changes").json() == changes


# ---- 事件审计时间桶统计 GET /events/metrics ----

def _metrics(client, **params):
    return client.get("/events/metrics", params=params)


def test_metrics_bucketing_counts_and_empty_buckets(client):
    # silence=60：e1@08:00:00、e2@08:00:30 同段；e3@08:02:00、e4@08:02:30 另一段
    _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z", "info")
    _post(client, "gw-01", "e2", "2026-09-20T08:00:30Z", "error")
    _post(client, "gw-01", "e3", "2026-09-20T08:02:00Z", "warning")
    _post(client, "gw-01", "e4", "2026-09-20T08:02:30Z", "critical")
    _post(client, "gw-02", "x", "2026-09-20T08:00:00Z", "critical")  # 其他 source

    resp = _metrics(
        client,
        source="gw-01",
        start_at="2026-09-20T08:00:00Z",
        end_at="2026-09-20T08:04:00Z",
        bucket_seconds=60,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["source"] == "gw-01"
    assert body["bucket_seconds"] == 60.0
    assert body["start_at"].replace("Z", "+00:00") == "2026-09-20T08:00:00+00:00"
    assert body["end_at"].replace("Z", "+00:00") == "2026-09-20T08:04:00+00:00"

    buckets = body["buckets"]
    assert len(buckets) == 4  # 含无事件桶，按时间升序
    assert [b["bucket_start"] for b in buckets] == sorted(
        b["bucket_start"] for b in buckets
    )
    assert (buckets[0]["bucket_start"].replace("Z", "+00:00")
            == "2026-09-20T08:00:00+00:00")
    assert (buckets[0]["bucket_end"].replace("Z", "+00:00")
            == "2026-09-20T08:01:00+00:00")

    zero = {"debug": 0, "info": 0, "warning": 0, "error": 0, "critical": 0}
    for bucket in buckets:
        assert set(bucket) == {
            "bucket_start", "bucket_end", "severity_counts",
            "event_count", "distinct_segment_count",
        }
        assert set(bucket["severity_counts"]) == set(zero)

    assert buckets[0]["severity_counts"] == {**zero, "info": 1, "error": 1}
    assert buckets[0]["event_count"] == 2
    assert buckets[0]["distinct_segment_count"] == 1  # e1/e2 同段
    assert buckets[1]["severity_counts"] == zero
    assert buckets[1]["event_count"] == 0
    assert buckets[1]["distinct_segment_count"] == 0
    assert buckets[2]["severity_counts"] == {**zero, "warning": 1, "critical": 1}
    assert buckets[2]["event_count"] == 2
    assert buckets[2]["distinct_segment_count"] == 1
    assert buckets[3]["severity_counts"] == zero


def test_metrics_half_open_boundary_excludes_end_at(client):
    # 桶边界与窗口边界上的事件：左闭右开
    _post(client, "gw-01", "a", "2026-09-20T08:00:00Z", "info")
    _post(client, "gw-01", "b", "2026-09-20T08:01:00Z", "error")
    _post(client, "gw-01", "c", "2026-09-20T08:02:00Z", "critical")

    # [08:00, 08:02)：包含 08:00、08:01，排除 08:02
    body = _metrics(
        client,
        source="gw-01",
        start_at="2026-09-20T08:00:00Z",
        end_at="2026-09-20T08:02:00Z",
        bucket_seconds=60,
    ).json()
    assert [b["event_count"] for b in body["buckets"]] == [1, 1]
    assert body["buckets"][0]["severity_counts"]["info"] == 1
    assert body["buckets"][1]["severity_counts"]["error"] == 1

    # 恰好等于桶边界的事件落在下一个桶（@08:01 属第二桶，不属第一桶）
    body = _metrics(
        client,
        source="gw-01",
        start_at="2026-09-20T08:01:00Z",
        end_at="2026-09-20T08:03:00Z",
        bucket_seconds=60,
    ).json()
    assert [b["event_count"] for b in body["buckets"]] == [1, 1]
    assert body["buckets"][0]["severity_counts"]["error"] == 1
    assert body["buckets"][1]["severity_counts"]["critical"] == 1


def test_metrics_last_bucket_clamped_to_end_at(client):
    _post(client, "gw-01", "e1", "2026-09-20T08:02:30Z")
    body = _metrics(
        client,
        source="gw-01",
        start_at="2026-09-20T08:00:00Z",
        end_at="2026-09-20T08:02:30Z",
        bucket_seconds=60,
    ).json()
    assert len(body["buckets"]) == 3
    last = body["buckets"][-1]
    assert last["bucket_start"].replace("Z", "+00:00") == "2026-09-20T08:02:00+00:00"
    assert last["bucket_end"].replace("Z", "+00:00") == "2026-09-20T08:02:30+00:00"
    assert last["event_count"] == 0  # e1@08:02:30 恰好等于 end，被排除


def test_metrics_distinct_segment_ids_tracks_current_ownership(client):
    a = _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z")["segment"]
    _post(client, "gw-01", "e2", "2026-09-20T08:02:00Z")
    _post(client, "gw-01", "e3", "2026-09-20T08:01:00Z", "critical")  # 桥接合并

    def counts():
        body = _metrics(
            client,
            source="gw-01",
            start_at="2026-09-20T08:00:00Z",
            end_at="2026-09-20T08:03:00Z",
            bucket_seconds=60,
        ).json()
        return [(b["event_count"], b["distinct_segment_count"]) for b in body["buckets"]]

    assert counts() == [(1, 1), (1, 1), (1, 1)]  # 全部并入段 A
    client.delete("/events/gw-01/e3")           # 拆段
    assert counts() == [(1, 1), (0, 0), (1, 1)]  # 反映重算后的实时归属


def test_metrics_single_bucket_counts_distinct_segments(client):
    a = _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z")["segment"]
    b = _post(client, "gw-01", "e2", "2026-09-20T08:02:00Z", "error")["segment"]
    body = _metrics(
        client,
        source="gw-01",
        start_at="2026-09-20T08:00:00Z",
        end_at="2026-09-20T08:05:00Z",
        bucket_seconds=300,
    ).json()
    assert len(body["buckets"]) == 1
    bucket = body["buckets"][0]
    assert bucket["event_count"] == 2
    assert bucket["distinct_segment_count"] == 2
    assert bucket["severity_counts"]["info"] == 1
    assert bucket["severity_counts"]["error"] == 1
    assert a["id"] != b["id"]


def test_metrics_source_isolation(client):
    _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z")
    _post(client, "gw-02", "e2", "2026-09-20T08:00:00Z", "critical")
    body = _metrics(
        client,
        source="gw-01",
        start_at="2026-09-20T08:00:00Z",
        end_at="2026-09-20T08:01:00Z",
        bucket_seconds=60,
    ).json()
    assert body["buckets"][0]["event_count"] == 1
    assert body["buckets"][0]["severity_counts"]["info"] == 1
    assert body["buckets"][0]["severity_counts"]["critical"] == 0

    # 未知 source：仍覆盖整个范围输出零计数桶
    body = _metrics(
        client,
        source="ghost",
        start_at="2026-09-20T08:00:00Z",
        end_at="2026-09-20T08:02:00Z",
        bucket_seconds=60,
    ).json()
    assert len(body["buckets"]) == 2
    assert all(b["event_count"] == 0 for b in body["buckets"])
    assert all(b["distinct_segment_count"] == 0 for b in body["buckets"])


def test_metrics_naive_datetime_treated_as_utc(client):
    _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z", "warning")
    body = _metrics(
        client,
        source="gw-01",
        start_at="2026-09-20 08:00:00",
        end_at="2026-09-20 08:01:00",
        bucket_seconds=60,
    ).json()
    assert body["buckets"][0]["event_count"] == 1
    assert body["buckets"][0]["severity_counts"]["warning"] == 1


def test_metrics_timezone_offsets_normalized_to_utc_boundaries(client):
    # +08:00 的 16:00 等于 UTC 08:00；start/end 混合带时区写法
    _post(client, "gw-01", "e1", "2026-09-20T08:00:30Z", "error")
    body = _metrics(
        client,
        source="gw-01",
        start_at="2026-09-20T16:00:00+08:00",
        end_at="2026-09-20T08:01:00+00:00",
        bucket_seconds=60,
    ).json()
    assert len(body["buckets"]) == 1
    assert body["start_at"].replace("Z", "+00:00") == "2026-09-20T08:00:00+00:00"
    assert body["buckets"][0]["event_count"] == 1
    assert body["buckets"][0]["severity_counts"]["error"] == 1


def test_metrics_float_boundary_aligned(client):
    # 桶长 0.1s：事件落在 0.3s 边界上必须进入 [0.3, 0.4) 桶
    _post(client, "gw-01", "e1", "2026-09-20T08:00:00.300Z")
    body = _metrics(
        client,
        source="gw-01",
        start_at="2026-09-20T08:00:00Z",
        end_at="2026-09-20T08:00:00.500Z",
        bucket_seconds=0.1,
    ).json()
    assert len(body["buckets"]) == 5
    assert [b["event_count"] for b in body["buckets"]] == [0, 0, 0, 1, 0]
    assert (body["buckets"][3]["bucket_start"].replace("Z", "+00:00")
            == "2026-09-20T08:00:00.300000+00:00")


def test_metrics_param_validation(client):
    base = {
        "source": "gw-01",
        "start_at": "2026-09-20T08:00:00Z",
        "end_at": "2026-09-20T09:00:00Z",
        "bucket_seconds": 60,
    }
    # 缺必填参数
    for missing in ("source", "start_at", "end_at", "bucket_seconds"):
        params = {k: v for k, v in base.items() if k != missing}
        assert _metrics(client, **params).status_code == 422
    # 空 source
    assert _metrics(client, **{**base, "source": ""}).status_code == 422
    # 桶长为 0 / 负数 / 非数字
    assert _metrics(client, **{**base, "bucket_seconds": 0}).status_code == 422
    assert _metrics(client, **{**base, "bucket_seconds": -10}).status_code == 422
    assert _metrics(client, **{**base, "bucket_seconds": "soon"}).status_code == 422
    # start_at 晚于 end_at
    assert _metrics(
        client,
        **{**base,
           "start_at": "2026-09-20T09:00:00Z",
           "end_at": "2026-09-20T08:00:00Z"},
    ).status_code == 422
    # 空时间范围（start == end）
    assert _metrics(
        client,
        **{**base,
           "start_at": "2026-09-20T08:00:00Z",
           "end_at": "2026-09-20T08:00:00Z"},
    ).status_code == 422
    # 非法时间字符串
    assert _metrics(client, **{**base, "start_at": "soon"}).status_code == 422
    # 桶数超过上限（10001）
    assert _metrics(
        client,
        **{**base,
           "start_at": "2026-09-20T08:00:00Z",
           "end_at": "2026-09-20T10:46:41Z",
           "bucket_seconds": 1},
    ).status_code == 422


def test_metrics_bucket_count_at_limit_allowed(client):
    # 恰好 10000 个桶允许
    resp = _metrics(
        client,
        source="gw-01",
        start_at="2026-09-20T08:00:00Z",
        end_at="2026-09-27T06:40:00Z",  # 08:00 起 10000 分钟
        bucket_seconds=60,
    )
    assert resp.status_code == 200
    assert len(resp.json()["buckets"]) == 10000


def test_metrics_is_read_only(client):
    _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z")
    segs = client.get("/segments").json()
    changes = client.get("/segment-changes").json()
    for _ in range(3):
        _metrics(
            client,
            source="gw-01",
            start_at="2026-09-20T08:00:00Z",
            end_at="2026-09-20T09:00:00Z",
            bucket_seconds=60,
        )
    assert client.get("/segments").json() == segs
    assert client.get("/segment-changes").json() == changes


def test_metrics_route_does_not_conflict_with_events_routes(client):
    # GET /events/metrics 与 GET/POST /events、DELETE /events/{source}/{event_id} 共存
    _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z")
    assert client.get("/events", params={"source": "gw-01"}).status_code == 200
    assert _metrics(
        client,
        source="gw-01",
        start_at="2026-09-20T08:00:00Z",
        end_at="2026-09-20T08:01:00Z",
        bucket_seconds=60,
    ).status_code == 200
    assert client.delete("/events/gw-01/e1").status_code == 200
    body = _metrics(
        client,
        source="gw-01",
        start_at="2026-09-20T08:00:00Z",
        end_at="2026-09-20T08:01:00Z",
        bucket_seconds=60,
    ).json()
    assert body["buckets"][0]["event_count"] == 0


# ---- 人工确认审计 POST/GET /segments/{id}/acknowledgements ----

def _ack(client, segment_id, *, operator="ops-alice", note="已确认",
         expected_revision=1, key="ack-1"):
    return client.post(f"/segments/{segment_id}/acknowledgements", json={
        "operator": operator,
        "note": note,
        "expected_revision": expected_revision,
        "idempotency_key": key,
    })


def test_acknowledge_segment_created(client):
    seg = _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z", "warning")["segment"]
    resp = _ack(client, seg["id"], operator="ops-alice", note="已确认，通知值班",
                expected_revision=1, key="ack-0001")
    assert resp.status_code == 200
    body = resp.json()
    assert body["ack_id"] == 1
    assert body["segment_id"] == seg["id"]
    assert body["operator"] == "ops-alice"
    assert body["note"] == "已确认，通知值班"
    assert body["expected_revision"] == 1
    assert body["segment_revision"] == 1
    assert body["idempotency_key"] == "ack-0001"
    assert body["created_at"]

    listed = client.get(f"/segments/{seg['id']}/acknowledgements")
    assert listed.status_code == 200
    assert listed.json() == [body]


def test_acknowledge_revision_mismatch_409_with_segment_snapshot(client):
    _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z")
    seg = _post(client, "gw-01", "e2", "2026-09-20T08:00:30Z", "error")["segment"]
    assert seg["revision"] == 2

    resp = _ack(client, seg["id"], expected_revision=1, key="ack-1")
    assert resp.status_code == 409
    body = resp.json()
    assert body["segment"]["id"] == seg["id"]
    assert body["segment"]["revision"] == 2  # 携带当前段快照
    assert body["segment"]["event_count"] == 2
    # 冲突不落库；用正确 revision 重试同一键可成功（冲突不占用幂等键）
    assert client.get(f"/segments/{seg['id']}/acknowledgements").json() == []
    ok = _ack(client, seg["id"], expected_revision=2, key="ack-1")
    assert ok.status_code == 200
    assert ok.json()["segment_revision"] == 2


def test_acknowledge_replay_same_payload_returns_original_record(client):
    seg = _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z")["segment"]
    first = _ack(client, seg["id"], expected_revision=1, key="ack-1")
    assert first.status_code == 200

    # 段随后变化：revision 升到 2，原 expected_revision 已过期
    _post(client, "gw-01", "e2", "2026-09-20T08:00:30Z", "critical")
    assert client.get(f"/segments/{seg['id']}").json()["revision"] == 2

    replay = _ack(client, seg["id"], expected_revision=1, key="ack-1")
    assert replay.status_code == 200
    assert replay.json() == first.json()  # 原记录原样返回，即使段已变化
    assert len(client.get(f"/segments/{seg['id']}/acknowledgements").json()) == 1


def test_acknowledge_same_key_different_payload_409(client):
    seg = _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z")["segment"]
    assert _ack(client, seg["id"], key="ack-1").status_code == 200
    # 任一载荷字段不同都视为不同载荷
    assert _ack(client, seg["id"], key="ack-1", operator="ops-bob").status_code == 409
    assert _ack(client, seg["id"], key="ack-1", note="另一条备注").status_code == 409
    assert _ack(client, seg["id"], key="ack-1", expected_revision=2).status_code == 409
    # 冲突不新增记录
    assert len(client.get(f"/segments/{seg['id']}/acknowledgements").json()) == 1


def test_acknowledge_key_scoped_per_segment(client):
    a = _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z")["segment"]
    b = _post(client, "gw-02", "e2", "2026-09-20T08:00:00Z")["segment"]
    r1 = _ack(client, a["id"], key="shared-key")
    r2 = _ack(client, b["id"], key="shared-key")  # 不同段复用同键互不干扰
    assert r1.status_code == r2.status_code == 200
    assert r1.json()["ack_id"] != r2.json()["ack_id"]


def test_acknowledge_unknown_segment_404(client):
    assert _ack(client, 9999).status_code == 404
    assert client.get("/segments/9999/acknowledgements").status_code == 404


def test_list_acknowledgements_ordered_by_ack_id(client):
    seg = _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z")["segment"]
    other = _post(client, "gw-02", "e2", "2026-09-20T08:00:00Z")["segment"]
    _ack(client, seg["id"], key="k1")
    _ack(client, other["id"], key="k2")  # 其他段的记录不出现
    _ack(client, seg["id"], key="k3")
    _ack(client, seg["id"], key="k2")
    rows = client.get(f"/segments/{seg['id']}/acknowledgements").json()
    assert [r["idempotency_key"] for r in rows] == ["k1", "k3", "k2"]
    ids = [r["ack_id"] for r in rows]
    assert ids == sorted(ids)


def test_acknowledge_does_not_change_segment_or_change_stream(client):
    seg = _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z")["segment"]
    before_seg = client.get(f"/segments/{seg['id']}").json()
    before_changes = client.get("/segment-changes").json()
    _ack(client, seg["id"], key="ack-1")
    assert client.get(f"/segments/{seg['id']}").json() == before_seg
    assert client.get("/segment-changes").json() == before_changes


def test_acknowledge_records_survive_segment_deletion(client):
    seg = _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z")["segment"]
    first = _ack(client, seg["id"], key="ack-1").json()
    client.delete("/events/gw-01/e1")  # 唯一事件撤回 → 段被删除
    assert client.get(f"/segments/{seg['id']}").status_code == 404
    # 幂等重放仍返回原记录（查重先于段状态校验）
    replay = _ack(client, seg["id"], key="ack-1")
    assert replay.status_code == 200
    assert replay.json() == first


def test_acknowledge_param_validation(client):
    seg = _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z")["segment"]
    url = f"/segments/{seg['id']}/acknowledgements"
    valid = {"operator": "ops", "note": "n",
             "expected_revision": 1, "idempotency_key": "k"}
    # 缺字段
    for missing in valid:
        payload = {k: v for k, v in valid.items() if k != missing}
        assert client.post(url, json=payload).status_code == 422
    # 空 operator / 空 idempotency_key / 非法 revision
    assert client.post(url, json={**valid, "operator": ""}).status_code == 422
    assert client.post(url, json={**valid, "idempotency_key": ""}).status_code == 422
    assert client.post(url, json={**valid, "expected_revision": 0}).status_code == 422


# ---- 变更水位历史快照 GET /segments/snapshot ----

def _snapshot(client, **params):
    return client.get("/segments/snapshot", params=params)


def test_snapshot_reconstructs_state_at_each_watermark(client):
    # silence=60：e1@08:00 → 段A；e2@08:02 → 段B；e3@08:01 桥接合并
    a = _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z", "warning")["segment"]
    b = _post(client, "gw-01", "e2", "2026-09-20T08:02:00Z", "error")["segment"]
    _post(client, "gw-01", "e3", "2026-09-20T08:01:00Z", "critical")

    # 水位 1：仅 A 的初始快照
    body = _snapshot(client, at_change_id=1).json()
    assert body["at_change_id"] == 1
    assert [s["id"] for s in body["segments"]] == [a["id"]]
    assert body["segments"][0]["revision"] == 1
    assert body["segments"][0]["event_count"] == 1
    assert body["segments"][0]["max_severity"] == "warning"

    # 水位 2：A、B 各自独立，按 segment_id 升序
    body = _snapshot(client, at_change_id=2).json()
    assert [s["id"] for s in body["segments"]] == [a["id"], b["id"]]

    # 水位 3：B 被 merged 墓碑移除；A 的最后一条变更仍是合并前快照
    body = _snapshot(client, at_change_id=3).json()
    assert [s["id"] for s in body["segments"]] == [a["id"]]
    assert body["segments"][0]["revision"] == 1

    # 水位 4（合并完成）：A 为合并后快照
    body = _snapshot(client, at_change_id=4).json()
    assert [s["id"] for s in body["segments"]] == [a["id"]]
    seg = body["segments"][0]
    assert seg["revision"] == 2
    assert seg["event_count"] == 3
    assert seg["max_severity"] == "critical"
    assert seg["started_at"].replace("Z", "+00:00") == "2026-09-20T08:00:00+00:00"
    assert seg["ended_at"].replace("Z", "+00:00") == "2026-09-20T08:02:00+00:00"


def test_snapshot_deleted_tombstone_removes_segment(client):
    seg = _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z")["segment"]
    client.delete("/events/gw-01/e1")  # 唯一事件撤回 → deleted 墓碑
    body = _snapshot(client, at_change_id=1).json()
    assert [s["id"] for s in body["segments"]] == [seg["id"]]
    body = _snapshot(client, at_change_id=2).json()
    assert body["segments"] == []


def test_snapshot_watermark_truncated_to_current_max_and_pinned(client):
    _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z")
    # 请求水位超过当前最大 change_id=1 → 截断并返回实际水位
    p1 = _snapshot(client, at_change_id=999, limit=1).json()
    assert p1["at_change_id"] == 1
    assert len(p1["segments"]) == 1

    _post(client, "gw-01", "e2", "2026-09-20T08:05:00Z")  # 新变更：最大水位变为 2

    # 后续页固定截断后的水位 1：快照不受新变更影响
    p2 = _snapshot(client, at_change_id=1, after=p1["next_after"]).json()
    assert p2["at_change_id"] == 1
    assert p2["segments"] == []
    assert p2["next_after"] == p1["next_after"]  # 空页游标保持不变
    # 水位条件与游标绑定的水位不再等价 → 422
    assert _snapshot(
        client, at_change_id=2, after=p1["next_after"]
    ).status_code == 422
    assert _snapshot(
        client, at_change_id=999, after=p1["next_after"]
    ).status_code == 422


def test_snapshot_empty_change_stream(client):
    assert _snapshot(client, at_change_id=10).json() == {
        "segments": [],
        "at_change_id": 0,
        "next_after": None,
    }


def test_snapshot_filters_by_source(client):
    a = _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z")["segment"]
    _post(client, "gw-02", "e2", "2026-09-20T08:00:00Z", "critical")
    body = _snapshot(client, at_change_id=2, source="gw-02").json()
    assert [s["source"] for s in body["segments"]] == ["gw-02"]
    assert body["segments"][0]["id"] != a["id"]
    body = _snapshot(client, at_change_id=2, source="ghost").json()
    assert body["segments"] == []
    assert body["next_after"] is None


def test_snapshot_pagination_walks_all_segments(client):
    # 间隔 120s > silence(60) → 5 个独立段
    ids = [
        _post(client, "gw-01", f"e{i}", f"2026-09-20T08:{i * 2:02d}:00Z")["segment"]["id"]
        for i in range(5)
    ]
    p1 = _snapshot(client, at_change_id=5, limit=2).json()
    assert [s["id"] for s in p1["segments"]] == ids[:2]
    p2 = _snapshot(client, at_change_id=5, after=p1["next_after"], limit=2).json()
    assert [s["id"] for s in p2["segments"]] == ids[2:4]
    p3 = _snapshot(client, at_change_id=5, after=p2["next_after"], limit=2).json()
    assert [s["id"] for s in p3["segments"]] == ids[4:]
    # 空页：游标保持不变
    p4 = _snapshot(client, at_change_id=5, after=p3["next_after"], limit=2).json()
    assert p4 == {"segments": [], "at_change_id": 5, "next_after": p3["next_after"]}
    seen = [s["id"] for p in (p1, p2, p3) for s in p["segments"]]
    assert seen == ids  # 不漏不重，按 segment_id 升序


def test_snapshot_cursor_binds_watermark_and_source(client):
    _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z")
    _post(client, "gw-02", "e2", "2026-09-20T08:00:00Z")
    cursor = _snapshot(client, at_change_id=1, source="gw-01").json()["next_after"]
    # 跨水位复用 → 422
    assert _snapshot(
        client, at_change_id=2, source="gw-01", after=cursor
    ).status_code == 422
    # 跨 source 复用 → 422
    assert _snapshot(
        client, at_change_id=1, source="gw-02", after=cursor
    ).status_code == 422
    # 去掉 source 过滤复用 → 422
    assert _snapshot(client, at_change_id=1, after=cursor).status_code == 422
    # 相同条件正常翻页
    assert _snapshot(
        client, at_change_id=1, source="gw-01", after=cursor
    ).status_code == 200


def test_snapshot_forged_cursor_rejected(client):
    _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z")
    assert _snapshot(
        client, at_change_id=1, after="@@not-base64@@"
    ).status_code == 422
    # 合法 base64 但结构非法
    assert _snapshot(client, at_change_id=1, after="WzFd").status_code == 422
    # 事件审计游标不能混用到快照接口
    events_cursor = client.get("/events", params={"source": "gw-01"}).json()[
        "next_after"
    ]
    assert _snapshot(
        client, at_change_id=1, after=events_cursor
    ).status_code == 422


def test_snapshot_is_read_only(client):
    _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z")
    segs = client.get("/segments").json()
    changes = client.get("/segment-changes").json()
    for _ in range(3):
        _snapshot(client, at_change_id=1)
        _snapshot(client, at_change_id=999)
    assert client.get("/segments").json() == segs
    assert client.get("/segment-changes").json() == changes


def test_snapshot_param_validation(client):
    # 缺 at_change_id
    assert client.get("/segments/snapshot").status_code == 422
    # 负水位 / 非数字水位
    assert _snapshot(client, at_change_id=-1).status_code == 422
    assert _snapshot(client, at_change_id="soon").status_code == 422
    # limit 越界
    assert _snapshot(client, at_change_id=0, limit=0).status_code == 422
    assert _snapshot(client, at_change_id=0, limit=1001).status_code == 422
    # 空 source
    assert _snapshot(client, at_change_id=0, source="").status_code == 422
    # 合法边界
    assert _snapshot(client, at_change_id=0, limit=1000).status_code == 200


def test_snapshot_route_not_captured_by_segment_id(client):
    # 静态路径必须优先于 /segments/{segment_id} 匹配，不能返回 422
    assert _snapshot(client, at_change_id=0).status_code == 200


# ---- 维护窗口抑制 ----


def _create_window(client, source, start_at, end_at, reason="maint"):
    return client.post("/maintenance-windows", json={
        "source": source, "start_at": start_at, "end_at": end_at,
        "reason": reason,
    })


def _post_event(client, source, event_id, occurred_at, severity="info"):
    resp = client.post("/events", json={
        "source": source, "event_id": event_id,
        "occurred_at": occurred_at, "severity": severity,
    })
    assert resp.status_code == 200
    return resp.json()


def test_create_maintenance_window_returns_record(client):
    resp = _create_window(
        client, "gw-01",
        "2026-09-20T08:00:00Z", "2026-09-20T09:00:00Z",
        "power maintenance",
    )
    assert resp.status_code == 200
    body = resp.json()
    assert isinstance(body["id"], int)
    assert body["source"] == "gw-01"
    assert body["reason"] == "power maintenance"
    assert body["start_at"].replace("Z", "+00:00") == "2026-09-20T08:00:00+00:00"
    assert body["end_at"].replace("Z", "+00:00") == "2026-09-20T09:00:00+00:00"
    assert "created_at" in body


def test_create_window_rejects_invalid_range(client):
    # end 早于 start
    resp = _create_window(
        client, "gw-01",
        "2026-09-20T09:00:00Z", "2026-09-20T08:00:00Z",
    )
    assert resp.status_code == 422
    # 空窗口（start == end）
    resp = _create_window(
        client, "gw-01",
        "2026-09-20T08:00:00Z", "2026-09-20T08:00:00Z",
    )
    assert resp.status_code == 422
    # 缺字段 / 空 source 由 schema 校验
    resp = client.post("/maintenance-windows", json={
        "source": "", "start_at": "2026-09-20T08:00:00Z",
        "end_at": "2026-09-20T09:00:00Z", "reason": "x",
    })
    assert resp.status_code == 422
    resp = client.post("/maintenance-windows", json={
        "source": "gw-01", "start_at": "2026-09-20T08:00:00Z",
    })
    assert resp.status_code == 422


def test_overlapping_windows_get_distinct_ids(client):
    w1 = _create_window(client, "gw-01", "2026-09-20T08:00:00Z",
                        "2026-09-20T09:00:00Z").json()
    w2 = _create_window(client, "gw-01", "2026-09-20T08:30:00Z",
                        "2026-09-20T10:00:00Z").json()
    assert w1["id"] != w2["id"]


def test_window_events_unknown_window_404(client):
    assert client.get("/maintenance-windows/9999/events").status_code == 404


def test_window_events_fully_and_partially(client):
    # 同一段内事件：08:00、08:00:30（silence=60s）
    _post_event(client, "gw-01", "e1", "2026-09-20T08:00:00Z")
    _post_event(client, "gw-01", "e2", "2026-09-20T08:00:30Z")

    # 窗口完整包住该段 → 两个事件都 fully
    w_full = _create_window(client, "gw-01",
                            "2026-09-20T07:00:00Z",
                            "2026-09-20T10:00:00Z").json()
    r = client.get(f"/maintenance-windows/{w_full['id']}/events").json()
    assert [e["event_id"] for e in r["fully"]] == ["e1", "e2"]
    assert r["partially"] == []
    assert all("segment_id" in e for e in r["fully"])

    # 窗口只覆盖段的尾部 [08:00:20, 09:00)：e2 在窗口内但段起点在窗口外
    w_part = _create_window(client, "gw-01",
                            "2026-09-20T08:00:20Z",
                            "2026-09-20T09:00:00Z").json()
    r = client.get(f"/maintenance-windows/{w_part['id']}/events").json()
    assert r["fully"] == []
    assert [e["event_id"] for e in r["partially"]] == ["e2"]
    assert r["window"]["id"] == w_part["id"]


def test_window_events_half_open_end_boundary(client):
    _post_event(client, "gw-01", "e1", "2026-09-20T08:00:00Z")
    _post_event(client, "gw-01", "e2", "2026-09-20T08:00:30Z")
    # 窗口终点恰为 08:00:30：该时刻事件不返回（左闭右开），只返回 e1
    w = _create_window(client, "gw-01",
                       "2026-09-20T07:00:00Z",
                       "2026-09-20T08:00:30Z").json()
    r = client.get(f"/maintenance-windows/{w['id']}/events").json()
    ids = [e["event_id"] for e in r["fully"] + r["partially"]]
    assert ids == ["e1"]


def test_window_events_source_exact_match(client):
    _post_event(client, "gw-01", "e1", "2026-09-20T08:00:00Z")
    _post_event(client, "GW-01", "e1", "2026-09-20T08:00:00Z")
    w = _create_window(client, "gw-01",
                       "2026-09-20T07:00:00Z",
                       "2026-09-20T10:00:00Z").json()
    r = client.get(f"/maintenance-windows/{w['id']}/events").json()
    assert len(r["fully"]) == 1
    assert r["fully"][0]["source"] == "gw-01"


def test_window_events_ordered_and_dedup_across_overlapping_windows(client):
    _post_event(client, "gw-01", "e3", "2026-09-20T08:00:40Z")
    _post_event(client, "gw-01", "e1", "2026-09-20T08:00:00Z")
    _post_event(client, "gw-01", "e2", "2026-09-20T08:00:20Z")
    w1 = _create_window(client, "gw-01",
                        "2026-09-20T08:00:00Z",
                        "2026-09-20T08:01:00Z").json()
    w2 = _create_window(client, "gw-01",
                        "2026-09-20T08:00:10Z",
                        "2026-09-20T08:00:50Z").json()
    r1 = client.get(f"/maintenance-windows/{w1['id']}/events").json()
    r2 = client.get(f"/maintenance-windows/{w2['id']}/events").json()
    # 每个窗口内同一事件只出现一次，且按时间升序
    for r, expected in [(r1, ["e1", "e2", "e3"]), (r2, ["e2", "e3"])]:
        ids = [e["event_id"] for e in r["fully"] + r["partially"]]
        assert ids == expected
        assert len(ids) == len(set(ids))


def test_window_events_segment_id_is_current(client):
    _post_event(client, "gw-01", "e1", "2026-09-20T08:00:00Z")
    _post_event(client, "gw-01", "bridge", "2026-09-20T08:00:40Z")
    _post_event(client, "gw-01", "e2", "2026-09-20T08:01:20Z")
    # 撤回桥接事件 → 拆成两段
    assert client.delete("/events/gw-01/bridge").status_code == 200
    w = _create_window(client, "gw-01",
                       "2026-09-20T07:00:00Z",
                       "2026-09-20T10:00:00Z").json()
    r = client.get(f"/maintenance-windows/{w['id']}/events").json()
    by_id = {e["event_id"]: e["segment_id"] for e in r["fully"]}
    assert by_id["e1"] != by_id["e2"]  # 当前归属为拆段后的两个段


def test_create_window_naive_datetime_assumed_utc(client):
    resp = _create_window(
        client, "gw-01",
        "2026-09-20T08:00:00", "2026-09-20T09:00:00",
    )
    assert resp.status_code == 200
    assert resp.json()["start_at"].replace("Z", "+00:00") == \
        "2026-09-20T08:00:00+00:00"


def test_create_window_invalid_range_persists_nothing(client):
    _create_window(client, "gw-01",
                   "2026-09-20T09:00:00Z",
                   "2026-09-20T08:00:00Z")
    _create_window(client, "gw-01",
                   "2026-09-20T08:00:00Z",
                   "2026-09-20T08:00:00Z")
    # 非法写入全部回滚；未知窗口仍 404（没有自增 id 被"占用"可见的影响）
    assert client.get("/maintenance-windows/1/events").status_code == 404


# ---- 严重性序列模式指纹 GET /segments/patterns ----

def _patterns(client, **params):
    return client.get("/segments/patterns", params=params)


def test_patterns_groups_by_fingerprint_across_segments(client):
    # silence=60：08:00 组与 08:02 组各成一段，序列同为 info→warning→error
    _post(client, "gw-01", "a1", "2026-09-20T08:00:00Z", "info")
    _post(client, "gw-01", "a2", "2026-09-20T08:00:10Z", "warning")
    _post(client, "gw-01", "a3", "2026-09-20T08:00:20Z", "error")
    _post(client, "gw-01", "b1", "2026-09-20T08:02:00Z", "info")
    _post(client, "gw-01", "b2", "2026-09-20T08:02:10Z", "warning")
    _post(client, "gw-01", "b3", "2026-09-20T08:02:20Z", "error")

    resp = _patterns(client, window_length=3)
    assert resp.status_code == 200
    body = resp.json()
    assert body["window_length"] == 3
    assert body["source"] is None
    assert body["start_at"] is None
    assert body["end_at"] is None
    assert body["scanned_event_count"] == 6
    assert body["scanned_segment_count"] == 2
    assert len(body["patterns"]) == 1
    p = body["patterns"][0]
    assert set(p) == {
        "fingerprint", "pattern", "occurrence_count",
        "segment_ids", "sources",
        "first_occurred_at", "last_occurred_at",
    }
    assert len(p["fingerprint"]) == 64
    assert p["pattern"] == ["info", "warning", "error"]
    assert p["occurrence_count"] == 2
    assert len(p["segment_ids"]) == 2
    assert p["sources"] == ["gw-01"]
    assert p["first_occurred_at"].replace("Z", "+00:00") == "2026-09-20T08:00:00+00:00"
    assert p["last_occurred_at"].replace("Z", "+00:00") == "2026-09-20T08:02:20+00:00"


def test_patterns_overlapping_windows_each_counted(client):
    for eid, sev in [("e1", "info"), ("e2", "info"), ("e3", "info")]:
        _post(client, "gw-01", eid, f"2026-09-20T08:00:{eid[1]}0Z", sev)
    body = _patterns(client, window_length=2).json()
    assert len(body["patterns"]) == 1
    assert body["patterns"][0]["pattern"] == ["info", "info"]
    assert body["patterns"][0]["occurrence_count"] == 2


def test_patterns_window_length_one(client):
    for eid, ts, sev in [
        ("e1", "08:00:00", "info"),
        ("e2", "08:00:10", "error"),
        ("e3", "08:00:20", "info"),
    ]:
        _post(client, "gw-01", eid, f"2026-09-20T{ts}Z", sev)
    body = _patterns(client, window_length=1).json()
    counts = {tuple(p["pattern"]): p["occurrence_count"] for p in body["patterns"]}
    assert counts == {("info",): 2, ("error",): 1}


def test_patterns_sequences_shorter_than_window_skipped(client):
    _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z")
    _post(client, "gw-01", "e2", "2026-09-20T08:00:10Z", "error")
    body = _patterns(client, window_length=3).json()
    assert body["patterns"] == []
    assert body["scanned_event_count"] == 2
    assert body["scanned_segment_count"] == 1


def test_patterns_source_filter(client):
    _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z", "info")
    _post(client, "gw-02", "e2", "2026-09-20T08:00:00Z", "critical")
    body = _patterns(client, source="gw-01", window_length=1).json()
    assert body["source"] == "gw-01"
    assert body["scanned_event_count"] == 1
    assert [p["pattern"] for p in body["patterns"]] == [["info"]]
    assert body["patterns"][0]["sources"] == ["gw-01"]
    # 未知 source：空结果
    body = _patterns(client, source="ghost", window_length=1).json()
    assert body["patterns"] == []
    assert body["scanned_segment_count"] == 0


def test_patterns_scans_all_sources_by_default_and_lists_sources(client):
    _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z", "info")
    _post(client, "gw-02", "e2", "2026-09-20T08:00:00Z", "info")
    body = _patterns(client, window_length=1).json()
    assert body["scanned_event_count"] == 2
    assert body["scanned_segment_count"] == 2
    assert len(body["patterns"]) == 1
    p = body["patterns"][0]
    assert p["sources"] == ["gw-01", "gw-02"]
    assert len(p["segment_ids"]) == 2
    assert p["occurrence_count"] == 2


def test_patterns_inclusive_time_window(client):
    for eid, ts in [("a", "08:00:00"), ("b", "08:00:30"), ("c", "08:02:00")]:
        _post(client, "gw-01", eid, f"2026-09-20T{ts}Z")
    # 闭区间：两端都包含
    body = _patterns(
        client,
        source="gw-01",
        start_at="2026-09-20T08:00:30Z",
        end_at="2026-09-20T08:02:00Z",
        window_length=1,
    ).json()
    assert body["scanned_event_count"] == 2
    assert body["start_at"].replace("Z", "+00:00") == "2026-09-20T08:00:30+00:00"
    assert body["end_at"].replace("Z", "+00:00") == "2026-09-20T08:02:00+00:00"
    # 只有单边
    body = _patterns(
        client, source="gw-01", start_at="2026-09-20T08:00:30Z", window_length=1
    ).json()
    assert body["scanned_event_count"] == 2


def test_patterns_same_timestamp_stable_order_and_stable_response(client):
    # 同刻乱序写入：序列按 event_id 稳定排序
    _post(client, "gw-01", "c", "2026-09-20T08:00:00Z", "critical")
    _post(client, "gw-01", "a", "2026-09-20T08:00:00Z", "info")
    _post(client, "gw-01", "b", "2026-09-20T08:00:00Z", "error")
    params = {"window_length": 3}
    first = _patterns(client, **params).json()
    second = _patterns(client, **params).json()
    assert first == second
    assert first["patterns"][0]["pattern"] == ["info", "error", "critical"]
    # patterns 按指纹升序
    fingerprints = [p["fingerprint"] for p in first["patterns"]]
    assert fingerprints == sorted(fingerprints)


def test_patterns_track_current_segment_after_withdraw(client):
    _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z", "info")
    _post(client, "gw-01", "e2", "2026-09-20T08:02:00Z", "error")
    _post(client, "gw-01", "bridge", "2026-09-20T08:01:00Z", "warning")
    body = _patterns(client, window_length=3).json()
    assert [p["pattern"] for p in body["patterns"]] == [["info", "warning", "error"]]
    # 撤回桥接事件拆段后，长度 3 的窗口消失
    assert client.delete("/events/gw-01/bridge").status_code == 200
    body = _patterns(client, window_length=3).json()
    assert body["patterns"] == []
    body = _patterns(client, window_length=1).json()
    assert {tuple(p["pattern"]) for p in body["patterns"]} == {("info",), ("error",)}


def test_patterns_param_validation(client):
    # 缺 window_length
    assert _patterns(client).status_code == 422
    # 0 / 负数 / 非数字
    assert _patterns(client, window_length=0).status_code == 422
    assert _patterns(client, window_length=-1).status_code == 422
    assert _patterns(client, window_length="soon").status_code == 422
    # 超上限 1000
    assert _patterns(client, window_length=1001).status_code == 422
    # 边界值合法
    assert _patterns(client, window_length=1000).status_code == 200
    # 空 source / 非法时间
    assert _patterns(client, source="", window_length=1).status_code == 422
    assert _patterns(
        client, window_length=1, start_at="soon"
    ).status_code == 422
    # start_at 晚于 end_at
    assert _patterns(
        client,
        window_length=1,
        start_at="2026-09-20T09:00:00Z",
        end_at="2026-09-20T08:00:00Z",
    ).status_code == 422


def test_patterns_empty_scan(client):
    body = _patterns(client, window_length=2).json()
    assert body == {
        "source": None,
        "start_at": None,
        "end_at": None,
        "window_length": 2,
        "scanned_event_count": 0,
        "scanned_segment_count": 0,
        "patterns": [],
    }


def test_patterns_naive_datetime_treated_as_utc(client):
    _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z")
    body = _patterns(
        client,
        start_at="2026-09-20 08:00:00",
        end_at="2026-09-20 09:00:00",
        window_length=1,
    ).json()
    assert body["scanned_event_count"] == 1


def test_patterns_route_not_captured_by_segment_id(client):
    # 静态路径必须优先于 /segments/{segment_id} 匹配，不能返回 404/422
    assert _patterns(client, window_length=1).status_code == 200


def test_patterns_is_read_only(client):
    _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z", "info")
    _post(client, "gw-01", "e2", "2026-09-20T08:00:10Z", "warning")
    segs = client.get("/segments").json()
    changes = client.get("/segment-changes").json()
    events = client.get("/events", params={"source": "gw-01"}).json()
    for _ in range(3):
        _patterns(client, window_length=2)
        _patterns(
            client, source="gw-01",
            start_at="2026-09-20T00:00:00Z", end_at="2026-09-21T00:00:00Z",
            window_length=1,
        )
    assert client.get("/segments").json() == segs
    assert client.get("/segment-changes").json() == changes
    assert client.get("/events", params={"source": "gw-01"}).json() == events


# ---- 持续时长分布报告 GET /segments/duration-report ----

def _duration_report(client, **params):
    return client.get("/segments/duration-report", params=params)


def _three_segments(client):
    # silence=60：gw-01 三段，持续时长分别为 30s、40s、0s
    _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z", "warning")
    _post(client, "gw-01", "e2", "2026-09-20T08:00:30Z", "error")
    _post(client, "gw-01", "e3", "2026-09-20T08:02:00Z", "info")
    _post(client, "gw-01", "e4", "2026-09-20T08:02:40Z", "critical")
    _post(client, "gw-01", "e5", "2026-09-20T08:05:00Z", "debug")


def test_duration_report_stats_and_fields(client):
    _three_segments(client)
    resp = _duration_report(client, source="gw-01", bucket_seconds=30)
    assert resp.status_code == 200
    body = resp.json()
    assert body["source"] == "gw-01"
    assert body["start_at"] is None
    assert body["end_at"] is None
    assert body["bucket_seconds"] == 30.0
    assert body["sample_count"] == 3
    # 时长 [0, 30, 40]
    assert body["min_seconds"] == 0.0
    assert body["max_seconds"] == 40.0
    assert body["mean_seconds"] == 23.333333  # 70/3 取整到微秒
    assert body["median_seconds"] == 30.0
    assert body["p90_seconds"] == 38.0  # 30 + 0.8 * (40 - 30)
    # 桶宽 30：[0,30) 含 0s 段；[30,60) 含 30s/40s 段（左闭右开）
    assert body["buckets"] == [
        {"bucket_start_seconds": 0.0, "bucket_end_seconds": 30.0,
         "segment_count": 1},
        {"bucket_start_seconds": 30.0, "bucket_end_seconds": 60.0,
         "segment_count": 2},
    ]


def test_duration_report_all_sources_even_sample_and_empty_buckets(client):
    _three_segments(client)
    _post(client, "gw-02", "x1", "2026-09-20T08:00:00Z")
    _post(client, "gw-02", "x2", "2026-09-20T08:01:00Z")  # 60s 段
    body = _duration_report(client, bucket_seconds=15).json()
    assert body["source"] is None
    assert body["sample_count"] == 4
    # 时长 [0, 30, 40, 60]
    assert body["mean_seconds"] == 32.5
    assert body["median_seconds"] == 35.0
    assert body["p90_seconds"] == 54.0  # 40 + 0.7 * (60 - 40)
    assert body["min_seconds"] == 0.0
    assert body["max_seconds"] == 60.0
    # 空桶（[15,30)、[45,60)）同样输出，桶按时长升序
    counts = [b["segment_count"] for b in body["buckets"]]
    assert counts == [1, 0, 2, 0, 1]
    starts = [b["bucket_start_seconds"] for b in body["buckets"]]
    assert starts == sorted(starts)
    assert sum(counts) == body["sample_count"]


def test_duration_report_time_window_overlap_closed(client):
    _three_segments(client)
    # 窗口 [08:01:00, 08:04:00]：只与 40s 段 [08:02:00, 08:02:40] 相交
    body = _duration_report(
        client, source="gw-01",
        start_at="2026-09-20T08:01:00Z", end_at="2026-09-20T08:04:00Z",
        bucket_seconds=60,
    ).json()
    assert body["sample_count"] == 1
    assert body["min_seconds"] == 40.0
    assert body["max_seconds"] == 40.0
    assert body["start_at"].replace("Z", "+00:00") == "2026-09-20T08:01:00+00:00"
    assert body["end_at"].replace("Z", "+00:00") == "2026-09-20T08:04:00+00:00"

    # 闭区间边界：段终点恰等于 start_at、段起点恰等于 end_at 都算相交
    body = _duration_report(
        client, source="gw-01",
        start_at="2026-09-20T08:00:30Z", end_at="2026-09-20T08:02:00Z",
        bucket_seconds=60,
    ).json()
    assert body["sample_count"] == 2  # 30s 段与 40s 段
    assert body["min_seconds"] == 30.0
    assert body["max_seconds"] == 40.0

    # 只有单边：段终点不早于 start_at
    body = _duration_report(
        client, source="gw-01", start_at="2026-09-20T08:04:00Z",
        bucket_seconds=60,
    ).json()
    assert body["sample_count"] == 1  # 只剩 0s 段 [08:05:00, 08:05:00]
    assert body["max_seconds"] == 0.0

    # 只有 end_at：段起点不晚于 end_at
    body = _duration_report(
        client, source="gw-01", end_at="2026-09-20T08:01:00Z",
        bucket_seconds=60,
    ).json()
    assert body["sample_count"] == 1  # 只剩 30s 段
    assert body["max_seconds"] == 30.0


def test_duration_report_empty_result(client):
    _three_segments(client)
    body = _duration_report(client, source="ghost", bucket_seconds=60).json()
    assert body == {
        "source": "ghost",
        "start_at": None,
        "end_at": None,
        "bucket_seconds": 60.0,
        "sample_count": 0,
        "min_seconds": None,
        "max_seconds": None,
        "mean_seconds": None,
        "median_seconds": None,
        "p90_seconds": None,
        "buckets": [],
    }


def test_duration_report_single_event_segment_zero_duration(client):
    _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z")
    body = _duration_report(client, bucket_seconds=60).json()
    assert body["sample_count"] == 1
    assert body["min_seconds"] == body["max_seconds"] == 0.0
    assert body["mean_seconds"] == 0.0
    assert body["median_seconds"] == body["p90_seconds"] == 0.0
    assert body["buckets"] == [
        {"bucket_start_seconds": 0.0, "bucket_end_seconds": 60.0,
         "segment_count": 1},
    ]


def test_duration_report_bucket_boundary_left_closed(client):
    # 段时长恰为桶宽整倍数：落入右侧桶
    _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z")
    _post(client, "gw-01", "e2", "2026-09-20T08:01:00Z")  # 时长 60s
    body = _duration_report(client, bucket_seconds=60).json()
    assert [b["segment_count"] for b in body["buckets"]] == [0, 1]
    assert body["buckets"][1]["bucket_start_seconds"] == 60.0
    assert body["buckets"][1]["bucket_end_seconds"] == 120.0


def test_duration_report_fractional_durations(client):
    _post(client, "gw-01", "e1", "2026-09-20T08:00:00.250Z")
    _post(client, "gw-01", "e2", "2026-09-20T08:00:00.750Z")  # 时长 0.5s
    body = _duration_report(client, bucket_seconds=0.25).json()
    assert body["sample_count"] == 1
    assert body["min_seconds"] == 0.5
    assert body["max_seconds"] == 0.5
    # 0.5/0.25 = 2 → 第 3 桶 [0.5, 0.75)
    assert [b["segment_count"] for b in body["buckets"]] == [0, 0, 1]
    assert body["buckets"][2]["bucket_start_seconds"] == 0.5
    assert body["buckets"][2]["bucket_end_seconds"] == 0.75


def test_duration_report_tracks_current_segments_after_withdraw(client):
    _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z")
    _post(client, "gw-01", "e2", "2026-09-20T08:02:00Z")
    _post(client, "gw-01", "bridge", "2026-09-20T08:01:00Z")  # 桥接 → 单段 120s
    body = _duration_report(client, bucket_seconds=60).json()
    assert body["sample_count"] == 1
    assert body["max_seconds"] == 120.0
    assert client.delete("/events/gw-01/bridge").status_code == 200  # 拆段
    body = _duration_report(client, bucket_seconds=60).json()
    assert body["sample_count"] == 2
    assert body["max_seconds"] == 0.0


def test_duration_report_deterministic_and_bucket_order(client):
    _three_segments(client)
    params = {"source": "gw-01", "bucket_seconds": 7}
    first = _duration_report(client, **params).json()
    second = _duration_report(client, **params).json()
    assert first == second  # 重复请求确定一致
    starts = [b["bucket_start_seconds"] for b in first["buckets"]]
    assert starts == sorted(starts)  # 按桶序稳定输出


def test_duration_report_naive_datetime_treated_as_utc(client):
    _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z")
    _post(client, "gw-01", "e2", "2026-09-20T08:00:45Z")
    body = _duration_report(
        client, start_at="2026-09-20 08:00:00", bucket_seconds=60
    ).json()
    assert body["sample_count"] == 1
    assert body["max_seconds"] == 45.0


def test_duration_report_timezone_offsets_normalized_to_utc(client):
    _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z")
    _post(client, "gw-01", "e2", "2026-09-20T08:00:45Z")
    body = _duration_report(
        client,
        start_at="2026-09-20T16:00:00+08:00",  # 等于 UTC 08:00
        end_at="2026-09-20T08:01:00+00:00",
        bucket_seconds=60,
    ).json()
    assert body["sample_count"] == 1
    assert body["start_at"].replace("Z", "+00:00") == "2026-09-20T08:00:00+00:00"


def test_duration_report_param_validation(client):
    # 缺 bucket_seconds
    assert client.get("/segments/duration-report").status_code == 422
    # 桶长为 0 / 负数 / 非数字 / 不足 1 微秒
    assert _duration_report(client, bucket_seconds=0).status_code == 422
    assert _duration_report(client, bucket_seconds=-5).status_code == 422
    assert _duration_report(client, bucket_seconds="soon").status_code == 422
    assert _duration_report(client, bucket_seconds=0.0000001).status_code == 422
    # 空 source
    assert _duration_report(
        client, source="", bucket_seconds=60
    ).status_code == 422
    # 非法时间
    assert _duration_report(
        client, start_at="soon", bucket_seconds=60
    ).status_code == 422
    # start_at 晚于 end_at
    assert _duration_report(
        client,
        start_at="2026-09-20T09:00:00Z",
        end_at="2026-09-20T08:00:00Z",
        bucket_seconds=60,
    ).status_code == 422
    # 合法边界
    assert _duration_report(client, bucket_seconds=60).status_code == 200


def test_duration_report_bucket_count_limit(client):
    # 链式事件构成时长 100s 的单段（相邻间隔 ≤ silence=60）
    _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z")
    _post(client, "gw-01", "e2", "2026-09-20T08:00:30Z")
    _post(client, "gw-01", "e3", "2026-09-20T08:01:00Z")
    _post(client, "gw-01", "e4", "2026-09-20T08:01:40Z")
    # 100/0.01 → 10001 个桶 → 422
    assert _duration_report(client, bucket_seconds=0.01).status_code == 422
    # 9901 个桶允许
    resp = _duration_report(client, bucket_seconds=0.0101)
    assert resp.status_code == 200
    assert len(resp.json()["buckets"]) == 9901
    # 无匹配段时不生成桶，极小桶长也不报错
    resp = _duration_report(client, source="ghost", bucket_seconds=0.000001)
    assert resp.status_code == 200
    assert resp.json()["buckets"] == []


def test_duration_report_is_read_only(client):
    _three_segments(client)
    segs = client.get("/segments").json()
    changes = client.get("/segment-changes").json()
    for _ in range(3):
        _duration_report(client, bucket_seconds=30)
        _duration_report(
            client, source="gw-01",
            start_at="2026-09-20T00:00:00Z", end_at="2026-09-21T00:00:00Z",
            bucket_seconds=10,
        )
    assert client.get("/segments").json() == segs
    assert client.get("/segment-changes").json() == changes


def test_duration_report_route_not_captured_by_segment_id(client):
    # 静态路径必须优先于 /segments/{segment_id} 匹配，不能返回 422
    assert _duration_report(client, bucket_seconds=60).status_code == 200
