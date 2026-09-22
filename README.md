# 设备告警事件段聚合服务

基于 FastAPI + SQLite 的告警汇聚服务：客户端上报离散告警事件，服务按 **source + 可配置静默间隔** 将乱序到达的事件归入同一事件段（segment），并保证 **event_id 幂等去重**——重复提交不改变任何计数。误报事件可通过 `DELETE /events/{source}/{event_id}` 撤回，服务在单事务内删除事件并按静默间隔重算其原段（删除/收缩/拆分）。`GET /events` 提供事件审计查询：在单一只读快照内按 `(occurred_at, event_id)` 稳定排序、支持时间窗与 severity 多值过滤及游标分页；`GET /events/metrics` 在同一只读快照内按 `occurred_at` 将事件划入**左闭右开**等宽时间桶，返回每桶各 severity 计数、事件总数与涉及的不同 segment_id 数（无事件桶照常输出、按时间升序）。每次段变更都会同事务写入**单调递增的变更流**，下游可通过 `GET /segment-changes` 做增量同步，并通过 `GET /segments/snapshot` 按**变更水位**重建任一历史时刻的段状态快照。运维人员可通过 `POST /segments/{id}/acknowledgements` 对事件段做**人工确认**：在事务内校验段的当前 revision 后追加**不可变**的确认审计记录（幂等键防重），并通过 `GET /segments/{id}/acknowledgements` 按确认序号升序查询。`POST /maintenance-windows` 可登记 **[start_at, end_at) 左闭右开、source 精确匹配的维护窗口**（允许重叠、事务化写入），`GET /maintenance-windows/{id}/events` 在**单一只读快照**内返回窗口覆盖的事件及其**当前 segment_id**，并按事件当前所在段划分为 `fully`（段整体在窗口内）与 `partially`（段还延伸到窗口之外）两类、按事件时间升序返回。`GET /segments/patterns` 在**单一只读快照**内扫描当前事件，提取每个 segment 内按发生时间排序的 **severity 序列**，以可配置的**连续窗口长度**生成规范化模式指纹，按指纹分组返回出现次数、涉及的 segment_id/source 及首次/最近发生时间。`GET /segments/duration-report` 则在**单一只读快照**内对**当前事件段**的持续时长（`ended_at - started_at`，秒）做分布统计：支持可选 `source` 与起止时间**闭区间**筛选（按段时间轴相交），返回样本数、最短/最长、均值、中位数、P90，以及按可配置 `bucket_seconds` 从 0 秒起划分的**左闭右开**时长桶（空桶照常返回、按桶序升序）。

## 快速开始

一条命令完成依赖安装、数据库初始化与服务启动：

```bash
docker compose up --build
```

服务地址：**http://localhost:8000**（交互式 API 文档：http://localhost:8000/docs）

数据库文件在首次启动时自动建表初始化，并通过命名卷 `alerts-data` 持久化。

## 配置项

均通过环境变量配置（可在 `docker-compose.yml` 的 `environment` 中修改）：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `SILENCE_INTERVAL_SECONDS` | `300` | 静默间隔（秒）。同一 source 下，事件时间与事件段边界相距不超过该值即归入同一段；超过则开新段 |
| `DATABASE_PATH` | `/data/alerts.db`（容器）/ `data/alerts.db`（本地） | SQLite 数据库文件路径 |

## API

### 提交事件 `POST /events`

```bash
curl -X POST http://localhost:8000/events \
  -H 'Content-Type: application/json' \
  -d '{
    "source": "edge-gateway-01",
    "event_id": "evt-0001",
    "occurred_at": "2026-09-20T08:00:00Z",
    "severity": "warning"
  }'
```

- `severity` 取值：`debug` / `info` / `warning` / `error` / `critical`（大小写不敏感，`warn`、`fatal` 为别名）
- `occurred_at` 为 ISO 8601 时间；不带时区时按 UTC 处理
- 响应：`{"deduplicated": false, "segment": {...}}`；重复提交同一 `(source, event_id)` 时 `deduplicated: true`，计数不变

### 撤回误报事件 `DELETE /events/{source}/{event_id}`

将误报事件撤回：在**单事务**内删除该事件，并按当前静默间隔对其**原段的剩余事件重新切分**。未知 `(source, event_id)` 返回 `404`；任一步失败整体回滚，事件、段与变更流都保持撤回前状态。

```bash
curl -X DELETE http://localhost:8000/events/edge-gateway-01/evt-0001
```

```json
{
  "source": "edge-gateway-01",
  "event_id": "evt-0001",
  "result": "split_segment",
  "segments": [ { "...": "最早子段（沿用原段 id，revision 递增）" },
                { "...": "其余子段（新建，revision=1）" } ]
}
```

`result` 与段的处理规则：

- **`deleted_segment`**：该事件是原段的唯一事件 → 删除事件与原段，`segments` 为空，变更流写一条 `deleted` 墓碑。
- **`shrunk_segment`**：移除后剩余事件仍为一段（边界/最高级别可能重算）→ 段**沿用原 id**，`revision` 一律 **+1**（即使边界未变），`segments` 含 1 个段。
- **`split_segment`**：该事件原本桥接了两侧事件（迟到事件造成的合并），移除后按静默间隔重新切成多段 → **最早子段沿用原段 id**（`revision` 在原值上 +1），**其余子段全部新建**（`revision=1`，不复用历史段 id），`segments` 按时间序返回全部子段。
- 重算内容：子段的起止时间、`event_count` 与 `max_severity` 均由剩余事件重新计算（例如撤回唯一的 `critical` 事件后最高级别回落）。撤回仅影响该事件所在段，不触碰同一 source 的其他段、更不影响其他 source。
- 撤回释放了 `(source, event_id)` 幂等键，之后可重新提交同一事件并照常归段。

### 事件审计查询 `GET /events`

分页审计查询已入库的原始事件。整个请求在**单一只读快照事务**（`query_only` 连接）内完成，排序键为 `(occurred_at, event_id)` **稳定升序**（同一时刻按 `event_id` 决定次序）；与 `POST /events` 同路径、HTTP 方法不同，互不影响。

```bash
curl 'http://localhost:8000/events?source=edge-gateway-01&start_at=2026-09-20T00:00:00Z&end_at=2026-09-21T00:00:00Z&severity=error&severity=critical&limit=100'
```

```json
{
  "events": [
    {
      "source": "edge-gateway-01",
      "event_id": "evt-0001",
      "occurred_at": "2026-09-20T08:00:00+00:00",
      "severity": "critical",
      "segment_id": 7
    }
  ],
  "next_after": "WzE3ODk4OTEyMDAuMCwiZXZ0LTAwMDEiXQ"
}
```

查询参数：

| 参数 | 必填 | 默认 | 说明 |
| --- | --- | --- | --- |
| `source` | 是 | — | 只返回该来源的事件（1–256 字符） |
| `start_at` | 否 | — | 发生时间下界（**含边界**），ISO 8601；无时区按 UTC 处理；不得晚于 `end_at` |
| `end_at` | 否 | — | 发生时间上界（**含边界**），ISO 8601；无时区按 UTC 处理 |
| `severity` | 否 | — | 级别多值过滤，**可重复传入**（`?severity=error&severity=critical`），大小写不敏感并接受 `warn`/`fatal` 别名；不传表示不过滤 |
| `after` | 否 | 从头开始 | 上一页返回的 `next_after` **不透明游标**（请勿自行构造或解析） |
| `limit` | 否 | `100` | 每页条数，范围 1–**1000** |

- **`segment_id` 是查询时刻的实时归属**：事件归段可能因迟到事件合并、误报撤回重算而变化，该字段始终反映当前快照中的所属段 id（不保证与事件首次入库时的段一致）。
- **分页约定**：用上一页的 `next_after` 作为下一页的 `after`；游标编码的是 `(occurred_at, event_id)` 复合位置，分页期间新写入的事件只会获得更靠后的位置——**不漏读、不重复**。
- **空页游标保持不变**：到达末尾时返回 `{"events": [], "next_after": "<同一个 after>"}`，可直接用该游标继续轮询；首次请求（不带 `after`）且无数据时 `next_after` 为 `null`。
- **快照一致性**：全部过滤与分页读取在同一只读事务快照内完成，请求期间的并发写入不影响本次结果；接口纯读，不会改写事件、段或变更流。
- 参数校验失败（缺 `source`、`limit` 越界、非法 `severity`/时间、`start_at > end_at`、伪造游标）统一返回 `422`。

### 事件审计时间桶统计 `GET /events/metrics`

对已入库事件做**只读聚合统计**。整个请求在**单一只读快照事务**（`query_only` 连接）内完成：按 `occurred_at` 把 `[start_at, end_at)`（**左闭右开**，`end_at` 不含）窗口内指定 `source` 的事件划入从 `start_at` 起等宽（`bucket_seconds`）对齐的时间桶（同样左闭右开），返回每桶各 severity 计数、事件总数与涉及的**不同 segment_id 数**。桶边界统一按 **UTC** 处理，桶按时间**升序**输出，**无事件桶照常返回**（各计数为 0），末个桶可短于桶宽。

```bash
curl 'http://localhost:8000/events/metrics?source=edge-gateway-01&start_at=2026-09-20T08:00:00Z&end_at=2026-09-20T08:04:00Z&bucket_seconds=60'
```

```json
{
  "source": "edge-gateway-01",
  "start_at": "2026-09-20T08:00:00+00:00",
  "end_at": "2026-09-20T08:04:00+00:00",
  "bucket_seconds": 60.0,
  "buckets": [
    {
      "bucket_start": "2026-09-20T08:00:00+00:00",
      "bucket_end": "2026-09-20T08:01:00+00:00",
      "severity_counts": {"debug": 0, "info": 1, "warning": 0, "error": 1, "critical": 0},
      "event_count": 2,
      "distinct_segment_count": 1
    },
    {
      "bucket_start": "2026-09-20T08:01:00+00:00",
      "bucket_end": "2026-09-20T08:02:00+00:00",
      "severity_counts": {"debug": 0, "info": 0, "warning": 0, "error": 0, "critical": 0},
      "event_count": 0,
      "distinct_segment_count": 0
    }
  ]
}
```

查询参数：

| 参数 | 必填 | 说明 |
| --- | --- | --- |
| `source` | 是 | 只统计该来源的事件（1–256 字符） |
| `start_at` | 是 | 窗口下界（**含边界**），ISO 8601；无时区按 UTC 处理；必须早于 `end_at` |
| `end_at` | 是 | 窗口上界（**不含边界**），ISO 8601；无时区按 UTC 处理 |
| `bucket_seconds` | 是 | 桶宽（秒，**正数**，支持小数，如 `0.5`）；桶从 `start_at` 等宽对齐 |

- **左闭右开**：窗口为 `[start_at, end_at)`，恰好等于 `end_at` 的事件不统计；桶为 `[bucket_start, bucket_end)`，恰好等于桶边界的事件落入下一个桶。
- **`severity_counts`** 固定包含五个 canonical 键 `debug`/`info`/`warning`/`error`/`critical`，缺省为 0（`warn`/`fatal` 等别名按归一化后的 canonical 级别计数）；`event_count` 等于五项之和。
- **`distinct_segment_count`** 为桶内事件**当前**涉及的不同 segment_id 数——与 `GET /events` 一样反映查询快照中的实时归属，迟到合并、误报撤回拆段会改变该值。
- **无事件桶覆盖**：整个 `[start_at, end_at)` 范围被桶完全覆盖（末桶截断到 `end_at`），没有事件的桶也按时间序输出零计数；未知 source 返回同样的全零桶序列。
- **UTC 边界**：带时区偏移的入参（如 `2026-09-20T16:00:00+08:00`）先换算到 UTC 再对齐桶边界，响应中的时间均为 UTC。
- **桶数上限 10000**：`ceil((end_at - start_at) / bucket_seconds)` 超过 10000 时返回 `422`，防止极大窗口/极小桶长导致输出无界。
- **快照一致性与纯读**：统计在同一只读事务快照内完成，请求期间的并发写入不影响本次结果，不会改写事件、段或变更流。
- 参数校验失败（缺任一必填参数、空 `source`、`bucket_seconds <= 0` 或非数字、非法时间、`start_at >= end_at`、桶数超上限）统一返回 `422`。

### 查询事件段详情 `GET /segments/{id}`

```bash
curl http://localhost:8000/segments/1
```

```json
{
  "id": 1,
  "source": "edge-gateway-01",
  "started_at": "2026-09-20T08:00:00+00:00",
  "ended_at": "2026-09-20T08:04:11+00:00",
  "max_severity": "critical",
  "event_count": 3,
  "revision": 2
}
```

### 列出事件段 `GET /segments`（可选 `?source=` 过滤）

```bash
curl 'http://localhost:8000/segments?source=edge-gateway-01'
```

### 人工确认事件段 `POST /segments/{id}/acknowledgements`

运维人员确认某个事件段时，追加一条**不可变**的人工确认审计记录。请求在 `BEGIN IMMEDIATE` 事务内先校验段的当前 `revision` 与 `expected_revision` 一致，再落库确认记录；确认序号 `ack_id` 全局单调递增。

```bash
curl -X POST http://localhost:8000/segments/1/acknowledgements \
  -H 'Content-Type: application/json' \
  -d '{
    "operator": "ops-alice",
    "note": "已确认，通知值班处理",
    "expected_revision": 2,
    "idempotency_key": "ack-20260921-0001"
  }'
```

```json
{
  "ack_id": 1,
  "segment_id": 1,
  "operator": "ops-alice",
  "note": "已确认，通知值班处理",
  "expected_revision": 2,
  "segment_revision": 2,
  "idempotency_key": "ack-20260921-0001",
  "created_at": "2026-09-21T08:00:00+00:00"
}
```

- 段不存在返回 `404`。
- **revision 冲突**：`expected_revision` 与段当前 `revision` 不一致（确认期间段被更新）返回 `409`，响应体携带当前段快照 `{"detail": "...", "segment": {...}}`，客户端可基于快照决定是否以新 revision 重试；冲突不落库、不占用幂等键。
- **幂等重放**：`idempotency_key` 在段内唯一。相同键 + **相同载荷**（`operator`/`note`/`expected_revision` 完全一致）重放返回**原记录**（`ack_id` 相同），即使段此后已变化也不再校验 revision；相同键 + **不同载荷**返回 `409`。
- 确认记录**仅追加、不可变**：不提供修改/删除接口；确认不改变段的 `revision`、不写变更流；段后续合并或被撤回删除不影响已落库的审计记录。

### 列出人工确认记录 `GET /segments/{id}/acknowledgements`

```bash
curl http://localhost:8000/segments/1/acknowledgements
```

按确认序号 `ack_id` **升序**返回该段的全部确认记录（字段同上）；段不存在返回 `404`。

### 创建维护窗口 `POST /maintenance-windows`

登记一个维护抑制窗口：窗口为 `[start_at, end_at)` **左闭右开**（`end_at` 不含），`source` **精确匹配**（大小写敏感，不做前缀/子串匹配）。**多个窗口允许互相重叠**，互不去重、互不影响。窗口写入在 `BEGIN IMMEDIATE` **单事务**内完成，失败整体回滚；窗口表独立于事件/段，创建窗口不改写任何事件、段或变更流。

```bash
curl -X POST http://localhost:8000/maintenance-windows \
  -H 'Content-Type: application/json' \
  -d '{
    "source": "edge-gateway-01",
    "start_at": "2026-09-20T08:00:00Z",
    "end_at": "2026-09-20T09:00:00Z",
    "reason": "机房电源检修"
  }'
```

```json
{
  "id": 1,
  "source": "edge-gateway-01",
  "start_at": "2026-09-20T08:00:00+00:00",
  "end_at": "2026-09-20T09:00:00+00:00",
  "reason": "机房电源检修",
  "created_at": "2026-09-21T02:00:00+00:00"
}
```

- `start_at` / `end_at` 为 ISO 8601 时间，无时区按 UTC 处理；**`start_at` 必须早于 `end_at`**，空窗口（相等）或颠倒范围返回 `422`；其余字段校验失败（空 `source`、缺字段、`reason` 超长等）同样返回 `422`。
- 窗口一经创建即生效，窗口不随事件或段的后续变化而修改；没有更新/删除接口。

### 查询维护窗口覆盖的事件 `GET /maintenance-windows/{id}/events`

返回指定维护窗口**抑制了哪些事件**。整个请求在**单一只读快照事务**（`query_only` 连接）内完成：source 精确匹配，事件 `occurred_at` 落在 `[start_at, end_at)` 窗口内（恰好等于 `end_at` 的事件不返回）。每条事件返回事件四要素与其**当前 segment_id**——与 `GET /events` 一样反映查询快照中的实时归属（迟到合并、误报撤回拆段后随之变化）。窗口不存在返回 `404`。

```bash
curl http://localhost:8000/maintenance-windows/1/events
```

```json
{
  "window": {
    "id": 1,
    "source": "edge-gateway-01",
    "start_at": "2026-09-20T08:00:00+00:00",
    "end_at": "2026-09-20T09:00:00+00:00",
    "reason": "机房电源检修",
    "created_at": "2026-09-21T02:00:00+00:00"
  },
  "fully": [
    {
      "source": "edge-gateway-01",
      "event_id": "evt-0002",
      "occurred_at": "2026-09-20T08:10:00+00:00",
      "severity": "info",
      "segment_id": 9
    }
  ],
  "partially": [
    {
      "source": "edge-gateway-01",
      "event_id": "evt-0001",
      "occurred_at": "2026-09-20T08:00:30+00:00",
      "severity": "critical",
      "segment_id": 8
    }
  ]
}
```

- **`fully`（完整抑制）**：事件**当前所在段**整体落在窗口内（段起点 ≥ 窗口起点且段终点 < 窗口终点）——段内全部事件都发生在维护期间。
- **`partially`（部分抑制）**：事件本身发生在窗口内，但其当前段还包含**窗口之外**的事件（段起点早于窗口起点，或段终点不早于窗口终点）。
- 两类结果都按 `(occurred_at, event_id)` **事件时间升序**返回；事件按主键唯一，即使同一事件被**多个重叠窗口**命中，对每个窗口的响应中也只出现**一次**。
- 接口纯读：不创建、修改任何窗口、事件、段或变更流。

### 静默间隔重分段预演 `GET /segments/resegmentation-preview`

用**候选静默间隔**对某来源的全部原始事件重新归段，预览"如果改了间隔会变成什么样"。全程**只读**：在同一只读事务（`query_only` 连接）内读取该来源全部事件并按 `occurred_at`、`event_id` 稳定排序后重算，**不会改写事件、现有段或变更流**；模拟段仅以本次响应内有效的临时编号 `temp_id` 标识。

```bash
curl 'http://localhost:8000/segments/resegmentation-preview?source=edge-gateway-01&silence_interval_seconds=120'
```

```json
{
  "source": "edge-gateway-01",
  "silence_interval_seconds": 120.0,
  "event_count": 4,
  "simulated_segments": [
    {
      "temp_id": 1,
      "source": "edge-gateway-01",
      "started_at": "2026-09-20T08:00:00+00:00",
      "ended_at": "2026-09-20T08:02:30+00:00",
      "max_severity": "critical",
      "event_count": 4
    }
  ],
  "mappings": [
    {"segment_id": 1, "relation": "merged", "simulated_temp_ids": [1], "event_count": 2},
    {"segment_id": 2, "relation": "merged", "simulated_temp_ids": [1], "event_count": 2}
  ],
  "summary": {
    "changed_existing_segment_count": 2,
    "split_existing_segment_count": 0,
    "merged_existing_segment_count": 2,
    "unchanged_existing_segment_count": 0,
    "existing_segment_count": 2,
    "simulated_segment_count": 1
  }
}
```

- **`simulated_segments`**：按候选间隔重算出的模拟段（不落库），`temp_id` 从 1 起按时间顺序编号。
- **`mappings`**：每个**现有段**到模拟段的映射，按现有段 `id` 升序，依据事件成员关系判定：
  - `unchanged`：现有段与唯一一个模拟段的事件集合完全相同（候选间隔与现状等价）；
  - `split`：该现有段的事件被拆散到**多个**模拟段（候选间隔**更小**，切分更细）；
  - `merged`：**多个**现有段的事件并入同一个模拟段（候选间隔**更大**，段被合并）。
- **`summary`**：受影响（`split` + `merged`）现有段数，以及 split/merged/unchanged 各自计数、现有段总数与模拟段总数。
- 切分规则：事件按 `(occurred_at, event_id)` 排序后，相邻事件间隔 **大于**候选间隔处切分（恰好等于间隔仍归同段，与归段规则一致）。
- 相同入参的重复请求结果**确定一致**；`silence_interval_seconds >= 0`（`0` 表示仅同一时刻的事件归同段），未知来源返回各列表为空、计数为 0 的结果。

### 增量同步变更流 `GET /segment-changes?after=&limit=`

下游系统可按 `change_id` 升序增量拉取事件段变更，实现增量同步：

```bash
curl 'http://localhost:8000/segment-changes?after=0&limit=100'
```

```json
{
  "changes": [
    {
      "change_id": 1,
      "type": "upsert",
      "segment_id": 1,
      "merged_into": null,
      "revision": 1,
      "segment": {
        "id": 1,
        "source": "edge-gateway-01",
        "started_at": "2026-09-20T08:00:00+00:00",
        "ended_at": "2026-09-20T08:00:00+00:00",
        "max_severity": "warning",
        "event_count": 1,
        "revision": 1
      }
    },
    {
      "change_id": 4,
      "type": "merged",
      "segment_id": 2,
      "merged_into": 1,
      "revision": null,
      "segment": null
    },
    {
      "change_id": 5,
      "type": "deleted",
      "segment_id": 3,
      "merged_into": null,
      "revision": null,
      "segment": null
    }
  ],
  "next_after": 5
}
```

- **何时产生记录**：每次创建段、边界或最高级别变化（`revision` 递增）、多段合并，都会在事件写入的**同一事务**内追加变更记录；`change_id` 全局单调递增。
- **`upsert`**：段的新状态，`segment` 为变更时刻的完整快照（含 `event_count`、`revision`）。
- **`merged`**：合并墓碑，表示 `segment_id` 段已被删除并并入 `merged_into` 指向的保留段；每个被删除段各产生一条。
- **`deleted`**：撤回墓碑，表示 `segment_id` 段因其**唯一事件被撤回**而删除（没有并入目标，`merged_into` 为 `null`）。
- **撤回（`DELETE /events/{source}/{event_id}`）产生的记录**：
  - 唯一事件移除 → 一条 `deleted` 墓碑；
  - 未拆段 → 一条保留段 `upsert`（沿用原 id，`revision` 递增）；
  - 桥接事件移除导致拆段 → 先一条保留段（最早子段，沿用原 id、`revision` 递增）`upsert`，再按时间序为每个新建子段各写一条 `upsert`（`revision=1`）。记录间 `change_id` 严格有序，且与事件/段的删除更新在同一事务内，要么全部可见、要么整体回滚。
- **何时不产生记录**：仅事件数增加（`revision` 不变）不写 `upsert`；重复提交 `(source, event_id)` 的幂等重放不写任何记录；撤回未知事件返回 404，不写任何记录。
- **分页约定**：用上一页返回的 `next_after` 作为下一页的 `after` 继续拉取；`change_id` 单调递增保证分页期间新写入的变更只会出现在后续页——不漏读、不重复。返回空页时 `next_after` 与入参 `after` 相同，可直接继续轮询。
- `after` 默认 `0`（从头开始）；`limit` 默认 `100`，最大 `1000`。

### 按变更水位读取历史快照 `GET /segments/snapshot`

以变更流为唯一事实来源，重建**指定变更水位时刻**的事件段状态：每个 `segment_id` 取 `change_id` 不晚于水位的**最后一条**变更——`upsert` 保留其快照，`merged`/`deleted` 墓碑移除该段。整个请求在**单一只读快照事务**（`query_only` 连接）内完成，纯读不改写任何表或变更流。

```bash
curl 'http://localhost:8000/segments/snapshot?at_change_id=4&source=edge-gateway-01&limit=100'
```

```json
{
  "segments": [
    {
      "id": 1,
      "source": "edge-gateway-01",
      "started_at": "2026-09-20T08:00:00+00:00",
      "ended_at": "2026-09-20T08:02:00+00:00",
      "max_severity": "critical",
      "event_count": 3,
      "revision": 2
    }
  ],
  "at_change_id": 4,
  "next_after": "WyJzZWdtZW50LXNuYXBzaG90Iiw0LCJlZGdlLWdhdGV3YXktMDEiLDFd"
}
```

查询参数：

| 参数 | 必填 | 默认 | 说明 |
| --- | --- | --- | --- |
| `at_change_id` | 是 | — | 变更水位（`change_id`，≥ 0）。重建不晚于该水位的最后状态；`0` 表示任何变更之前（快照为空） |
| `source` | 否 | 不过滤 | 只返回该来源的段（1–256 字符） |
| `after` | 否 | 从头开始 | 上一页返回的 `next_after` **不透明游标**（请勿自行构造或解析） |
| `limit` | 否 | `100` | 每页段数，范围 1–**1000** |

- **重建规则**：对每个 `segment_id` 取水位内最后一条变更；`upsert` 时刻的完整快照（含当时的 `event_count`、`revision`）原样返回，`merged`/`deleted` 墓碑使该段从快照中消失。结果按 `segment_id` **升序**返回。
- **水位截断**：请求水位超过当前最大 `change_id` 时按当前最大水位**截断**，响应的 `at_change_id` 为实际生效水位（变更流为空时为 `0`）；后续页须以该实际水位继续，游标已将其固定。
- **游标绑定水位与 source**：`next_after` 游标编码了实际水位、`source` 过滤与页末 `segment_id`；伪造游标、篡改或跨条件（不同水位/不同 source）复用一律返回 `422`。空页时 `next_after` 与入参 `after` 相同（首页无 `after` 时为 `null`），游标保持不变。
- **快照一致性**：水位截断、游标校验与分页读取在同一只读事务内完成，请求期间的并发写入不影响本次结果。
- 参数校验失败（缺 `at_change_id`、水位为负、`limit` 越界、空 `source`）统一返回 `422`。

### 严重性序列模式指纹 `GET /segments/patterns`

扫描**当前事件段**内的告警严重性演化模式：按事件**当前 segment_id**（查询快照中的实时归属，撤回/合并重算后随之变化）分组，段内按 `(occurred_at, event_id)` **稳定升序**（同一时刻按 `event_id` 决定次序）提取 canonical severity 序列，再以可配置的**连续窗口长度** `window_length` 对每个段序列取全部长度为该值的连续子序列（滑动窗口，重叠重复出现分别计数），规范化为稳定指纹（SHA-256）后**按指纹分组**。整个请求在**单一只读快照事务**（`query_only` 连接）内完成，纯读不改任何表。

```bash
curl 'http://localhost:8000/segments/patterns?source=edge-gateway-01&start_at=2026-09-20T00:00:00Z&end_at=2026-09-21T00:00:00Z&window_length=3'
```

```json
{
  "source": "edge-gateway-01",
  "start_at": "2026-09-20T00:00:00+00:00",
  "end_at": "2026-09-21T00:00:00+00:00",
  "window_length": 3,
  "scanned_event_count": 6,
  "scanned_segment_count": 2,
  "patterns": [
    {
      "fingerprint": "a1b2c3…",
      "pattern": ["info", "warning", "error"],
      "occurrence_count": 2,
      "segment_ids": [7, 9],
      "sources": ["edge-gateway-01"],
      "first_occurred_at": "2026-09-20T08:00:00+00:00",
      "last_occurred_at": "2026-09-20T08:12:20+00:00"
    }
  ]
}
```

查询参数：

| 参数 | 必填 | 默认 | 说明 |
| --- | --- | --- | --- |
| `source` | 否 | 不过滤（扫描全部来源） | 精确匹配该来源（1–256 字符）；`sources` 返回该模式涉及的所有来源 |
| `start_at` | 否 | 不限制 | 发生时间下界（**含边界**，闭区间），ISO 8601；无时区按 UTC 处理；不得晚于 `end_at` |
| `end_at` | 否 | 不限制 | 发生时间上界（**含边界**，闭区间），ISO 8601；无时区按 UTC 处理 |
| `window_length` | 是 | — | 连续窗口长度（整数，范围 **1–1000**），即每个模式包含的连续 severity 个数 |

- **序列提取**：按事件当前 `segment_id` 分组成段，段内事件按 `(occurred_at, event_id)` 升序排列后取其 canonical severity（别名/大小写已在入库时归一化）；因此迟到事件、撤回重算都会改变序列。
- **指纹规范化**：`fingerprint` 为 severity 序列的稳定 SHA-256 哈希，相同级别序列（含长度与各级别次序）无论出现在哪个段、哪个来源都得到同一指纹；响应同时回传可读的 `pattern` 序列。**窗口沿序列滑动**：一个含 `n` 个事件的段产生 `n - window_length + 1` 个窗口，相邻窗口可重叠，重复窗口分别计数。
- **分组聚合**：`occurrence_count` 为该指纹的窗口出现总次数；`segment_ids` / `sources` 为涉及的不同段 id / 来源（去重升序）；`first_occurred_at` / `last_occurred_at` 为各窗口**首/末事件**发生时间的最早/最晚值（`window_length=1` 时两者为同一次出现）。
- **输出次序与稳定性**：`patterns` 按指纹升序，段在段内按确定次序扫描；相同输入的重复请求结果逐字节一致。短于 `window_length` 的段不产生模式；没有匹配事件（含未知 source）时 `patterns` 为空，`scanned_event_count` / `scanned_segment_count` 为 0。
- **快照一致性与纯读**：全部扫描在同一只读事务快照内完成，请求期间的并发写入不影响本次结果，不会改写事件、段或变更流。
- 参数校验失败（缺 `window_length`、长度非正/非整数/超过 1000、非法时间、`start_at > end_at`、空 `source`）统一返回 `422`。

### 当前事件段持续时长分布报告 `GET /segments/duration-report`

对**当前事件段**（segments 表实时状态；合并的被删段、撤回删除的段不计入）做只读的持续时长分布统计。整个请求在**单一只读快照事务**（`query_only` 连接）内完成：可按 `source`（可选，精确匹配）与起止时间**闭区间**（两端均含，按段时间轴 `[started_at, ended_at]` 与窗口**相交**筛选——单点接触也算相交）圈定样本段，对每段计算 `ended_at - started_at` 的**持续秒数**（`>= 0`，单事件段为 `0` 秒），返回样本数、最短/最长/均值、中位数与 P90，以及从 0 秒起按 `bucket_seconds` 等宽划分的**左闭右开**时长桶（`[bucket_start_seconds, bucket_end_seconds)`）。桶按桶序号**升序稳定输出，空桶照常返回**（`segment_count` 为 0）。

```bash
curl 'http://localhost:8000/segments/duration-report?source=edge-gateway-01&start_at=2026-09-20T00:00:00Z&end_at=2026-09-21T00:00:00Z&bucket_seconds=60'
```

```json
{
  "source": "edge-gateway-01",
  "start_at": "2026-09-20T00:00:00+00:00",
  "end_at": "2026-09-21T00:00:00+00:00",
  "bucket_seconds": 60.0,
  "sample_count": 2,
  "min_seconds": 10.0,
  "max_seconds": 30.0,
  "mean_seconds": 20.0,
  "median_seconds": 20.0,
  "p90_seconds": 28.0,
  "buckets": [
    {"bucket_index": 0, "bucket_start_seconds": 0.0,  "bucket_end_seconds": 60.0, "segment_count": 2},
    {"bucket_index": 1, "bucket_start_seconds": 60.0, "bucket_end_seconds": 120.0, "segment_count": 0}
  ]
}
```

查询参数：

| 参数 | 必填 | 默认 | 说明 |
| --- | --- | --- | --- |
| `source` | 否 | 不过滤（统计全部来源） | 精确匹配该来源（1–256 字符） |
| `start_at` | 否 | 不限制 | 时间窗下界（**含边界**，闭区间），ISO 8601；无时区按 UTC 处理；不得晚于 `end_at` |
| `end_at` | 否 | 不限制 | 时间窗上界（**含边界**，闭区间），ISO 8601；无时区按 UTC 处理 |
| `bucket_seconds` | 是 | — | 时长桶宽（秒，**正数**，支持小数，最小粒度 1 微秒）；桶从 `0` 秒起等宽划分 |

- **闭区间时间筛选**：段时间轴 `[started_at, ended_at]` 与 `[start_at, end_at]` 有交集即计入——判定条件为段起点 `<= end_at` 且段终点 `>= start_at`；段终点恰好等于窗口起点、或段起点恰好等于窗口终点（单点接触）都计入。不传时间参数表示不限制。
- **统计量**：`min_seconds`/`max_seconds` 为最短/最长持续秒数；`mean_seconds` 为算术平均；`median_seconds` 与 `p90_seconds` 采用线性插值百分位（R-7 法，与 numpy 默认 `linear` 一致），偶数样本的中位数为中间两值的均值。**无样本时** `sample_count` 为 `0`，五个统计量均为 `null`。
- **左闭右开时长桶**：桶 `i` 覆盖 `[i*bucket_seconds, (i+1)*bucket_seconds)`，持续时长恰好等于桶上限的段落入**下一个桶**；桶从 0 秒开始（不存在负时长），覆盖到最长样本所在桶为止，中间的空桶也按序输出。无样本（含未知 source、窗口不命中）时仍返回 **1 个** `[0, bucket_seconds)` 空桶。
- **实时归属**：统计直接读当前 segments 表，迟到事件合并、误报撤回拆段后结果随之变化；0 秒单事件段计入 `[0, bucket_seconds)` 首桶。
- **桶数上限 10000**：`floor(最长持续时长 / bucket_seconds) + 1` 超过 10000 时返回 `422`，防止极短桶长导致输出无界。
- **快照一致性与纯读**：全部统计在同一只读事务快照内完成，请求期间的并发写入不影响本次结果，不会改写事件、段或变更流。
- 参数校验失败（缺 `bucket_seconds`、空 `source`、`bucket_seconds <= 0`/非数字/小于 1 微秒、非法时间、`start_at > end_at`、桶数超上限）统一返回 `422`。

### 健康检查 `GET /healthz`

## 归段与 revision 规则

- 新事件的 `occurred_at` 落在某段 `[start - silence, end + silence]` 窗口内即并入该段；同时命中多个段（迟到事件桥接）时自动合并为一段，事件数累加。
- 段的 `revision` 初始为 1；**仅当**新事件导致段的起止边界或最高级别发生变化时递增，仅事件数增加不递增。
- `(source, event_id)` 唯一约束保证幂等：重复提交直接返回原段，不改计数、不改 revision。
- **误报撤回**（`DELETE /events/{source}/{event_id}`）在单事务内删除事件并对原段剩余事件重新切分：无剩余事件则删段（`deleted` 墓碑）；未拆段则沿用原 id 且 `revision` +1；拆段时最早子段沿用原 id（`revision` +1）、其余子段新建（`revision=1`）。
- 上述每次段创建、边界/最高级别变化、合并与撤回都会写入变更流（见 `GET /segment-changes`），供下游增量同步；历史段（变更流上线前已存在）不补录，订阅从部署后的新变更开始。
- 需要回看历史时，用 `GET /segments/snapshot` 按变更水位重建任一时刻的段状态（`upsert` 快照保留、`merged`/`deleted` 墓碑移除该段），与增量同步共用同一条变更流，互不影响。
- 人工确认（`POST /segments/{id}/acknowledgements`）仅追加审计记录，不改变段的 `revision`、不写变更流；确认记录独立于段的生命周期，段被合并或撤回删除后审计记录仍完整保留。
- 维护窗口（`POST /maintenance-windows` / `GET /maintenance-windows/{id}/events`）是独立的抑制元数据：左闭右开、source 精确匹配、允许重叠，事务化写入、纯读查询，绝不修改事件、段与变更流；覆盖事件的 `segment_id` 按查询时刻实时归属返回，并随撤回/重分段自动反映最新结果。
- 想评估调整 `SILENCE_INTERVAL_SECONDS` 的影响时，调用重分段预演接口（见 `GET /segments/resegmentation-preview`），可在不改动任何数据的前提下查看模拟段与现有段的 split/merged 映射。

## 本地开发与测试

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
pytest tests/ -v                 # 全部测试（核心逻辑 + API）
uvicorn app.main:app --reload    # 本地起服务，默认 http://127.0.0.1:8000
```

核心归段逻辑（`app/service.py`）仅依赖标准库，无第三方依赖也可验证：

```bash
python3 -m unittest tests.test_service -v
```

## 项目结构

```
app/
  main.py      # FastAPI 路由与生命周期（启动时幂等建表）
  service.py   # 归段/合并/去重/撤回核心逻辑、事件审计查询、时间桶统计、严重性序列模式指纹、段持续时长分布报告与变更水位快照
  schemas.py   # 请求/响应模型与校验
  severity.py  # 告警级别定义、排序与 canonical 级别表
  db.py        # SQLite 连接、schema、事务
  config.py    # 环境变量配置
tests/         # 单元测试与 API 测试
Dockerfile / docker-compose.yml
```
