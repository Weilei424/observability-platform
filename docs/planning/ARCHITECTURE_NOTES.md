# Architecture Notes

## Stack

| Layer | Technology | Rationale |
|---|---|---|
| Backend API | Go | Strong infrastructure signal; good concurrency; matches Prometheus/Loki/Mimir ecosystem |
| Metrics model | Prometheus-style labels | Industry-standard time-series model; required for Grafana Prometheus datasource compatibility |
| Metrics storage | Custom WAL + chunks + immutable time blocks | Demonstrates TSDB internals instead of CRUD storage |
| Metrics index | Custom label and series index | Enables label filtering, metadata discovery, and efficient queries |
| Logs model | Loki-style streams | Label-based log organization; aligns with Grafana log workflows |
| Log storage | Custom WAL + compressed chunks + stream index | Demonstrates log aggregation internals without building Elasticsearch |
| Query APIs | Prometheus-compatible and Loki-compatible subsets | Allows real Grafana to query the custom backend |
| Dashboard | Grafana | Avoids custom UI and proves interoperability |
| Local runtime | Docker Compose | Fast local development and repeatable demos |
| Kubernetes deployment | Helm + Kubernetes manifests | Cloud-native deployment signal |
| Performance testing | k6 and Go benchmarks | Repeatable ingest/query benchmarks |
| Optional object storage | MinIO/S3-compatible abstraction | Future long-term block/chunk storage path |
| Optional GitOps | ArgoCD | Declarative deployment management after Helm works |
| Secrets | Environment variables locally; Kubernetes Secrets/Vault later | No secrets in git; simple locally, hardenable later |

---

## Key Decisions

### Backend is Go

The core backend is Go. Storage engine logic, ingestion, query execution, compaction, and API compatibility belong in the Go service. Avoid moving core logic into Python scripts or shell glue because that weakens the infrastructure signal.

### Grafana is the UI

Do not build a custom dashboard UI. The project should expose APIs that real Grafana can query. Grafana compatibility is one of the strongest proof points of the project.

### Single-node comes before distributed mode

The first working version must be a correct single-node backend. Distributed mode only starts after ingestion, WAL, blocks/chunks, indexes, queries, and Grafana integration work locally.

### Metrics come before logs

Metrics are the first data type because the TSDB path demonstrates the strongest storage-engine value: labels, series identity, samples, WAL, chunks, time blocks, compaction, retention, and range queries.

### Prometheus/Loki compatibility is a subset

The platform should expose enough compatible behavior for Grafana, not full Prometheus or full Loki. Unsupported PromQL/LogQL features must return explicit errors instead of silently producing wrong results.

### WAL before block storage

Writes must be durable before block storage is introduced. WAL replay is the correctness foundation for restart recovery.

### Blocks and chunks over generic SQL storage

Do not store all metrics/logs in PostgreSQL as the primary storage engine. This project is meant to demonstrate custom storage internals. SQL can be used later for metadata if needed, but samples and log lines belong in WAL/chunk/block storage.

### Compaction is background maintenance, not the first milestone

Compaction is important, but it depends on block layout and index correctness. Implement compaction only after blocks and indexes are queryable.

### Distributed mode is optional until the single-node path is strong

A weak distributed demo is worse than a strong single-node TSDB/log backend. Distributed ingesters, query fanout, replication, and tenant boundaries should only be added after the core engine is reliable.

---

## Component Responsibilities

| Component | Owns |
|---|---|
| Backend API | HTTP request handling, Prometheus/Loki-compatible endpoints, validation |
| Metrics ingester | Metric sample validation, series lookup, WAL append, memory buffer |
| Metrics store | Chunks, immutable blocks, block metadata, block reads |
| Metrics index | Metric names, label names/values, label pair → series IDs, series ID → chunk references |
| Metrics query engine | Selector parsing, range query, instant query, rate, sum, grouped sum |
| Logs ingester | Loki push parsing, stream lookup, WAL append, log buffering |
| Logs store | Compressed log chunks, chunk reads, stream metadata |
| Logs index | Label pair → stream IDs, stream ID → chunk references, time range filtering |
| Logs query engine | Loki-style selector parsing, time-range query, text filter scanning |
| Compactor | Block merging, index rebuild, retention cleanup |
| Gateway | Future distributed request routing and query fanout |
| Grafana | Visualization only; not a source of truth |

---

## Source of Truth Per Concern

| Concern | Source of Truth |
|---|---|
| Recent unflushed metric samples | WAL + in-memory series store |
| Persisted metric samples | Metrics blocks and chunks |
| Metric label discovery | Metrics index |
| Recent unflushed logs | WAL + in-memory stream buffers |
| Persisted logs | Log chunks |
| Log label discovery | Logs index |
| Dashboards | Grafana provisioning files under repo |
| Deployment config | Docker Compose, Helm, Kubernetes manifests |
| Planning and sequencing | `docs/planning/` |

---

## Storage Layout

Recommended local data layout:

```text
data/
  metrics/
    wal/
      000001.wal
      000002.wal
    blocks/
      <block-id>/
        meta.json
        index
        chunks
  logs/
    wal/
      000001.wal
      000002.wal
    chunks/
      <chunk-id>
    index/
      streams.index
  tmp/
```

### Metrics block metadata

Each metrics block should include:

```json
{
  "block_id": "example-block-id",
  "min_time": 1710000000,
  "max_time": 1710003600,
  "num_series": 1200,
  "num_samples": 90000,
  "created_at": "2026-05-15T00:00:00Z"
}
```

### Index expectations

Metrics index should support:

```text
metric name -> series IDs
label name -> label values
label pair -> series IDs
series ID -> chunk references
```

Logs index should support:

```text
label pair -> stream IDs
stream ID -> chunk references
time range -> candidate chunks
```

### Introduced in Phase 4.3

- **Log chunk format** (`internal/storage/logchunk`): `(tsNs, line)` entries with
  first-absolute / signed-varint-delta timestamps and uvarint-length lines, the
  whole entry block DEFLATE-compressed. Self-validating `Bytes()`/`FromBytes()`.
- **Chunk files** (`data/logs/chunks/<streamIDhex>-<minTsNs>-<rand4>.chunk`):
  a header embedding stream ID + labels, followed by the chunk bytes, written
  tmp → fsync → atomic rename → dir fsync. Self-describing, so the index can be
  rebuilt by scanning them.
- **Stream index manifest** (`data/logs/index/streams.index`): a persisted cache of
  `label pair → stream IDs` (via the shared `index.MemPostings`) and
  `stream ID → chunk refs` with per-chunk min/max. Rebuilt from chunk headers if
  missing or corrupt (chunks are authoritative).
- **Flush + checkpoint model**: `logs.Store` buffers to a WAL-backed head and, at a
  size threshold (`LogsFlushThresholdBytes`, default 8 MiB) and on shutdown, flushes
  the whole head to chunks + index and checkpoints the log WAL. Merged reads dedup
  by `(streamID, tsNs, line)` to neutralize the flush crash window.

---

## API Boundaries

### Internal metrics ingestion API

```http
POST /api/v1/ingest/metrics
```

This endpoint is for the project's sample app and load generator. Prometheus remote write can be added later, but it is not required for the minimum resume-worthy version.

### Prometheus-compatible metrics API

```http
GET /api/v1/query
GET /api/v1/query_range
GET /api/v1/labels
GET /api/v1/label/{name}/values
GET /api/v1/series
```

### Loki-compatible logs API

```http
POST /loki/api/v1/push
GET /loki/api/v1/query
GET /loki/api/v1/query_range
GET /loki/api/v1/labels
GET /loki/api/v1/label/{name}/values
```

---

## Supported Query Scope

### Metrics query subset

Required:

```text
metric_name
metric_name{label="value"}
rate(metric_name[5m])
sum(metric_name)
sum by (label)(metric_name)
```

Explicitly unsupported in v1:

```text
joins
subqueries
histogram functions
recording rules
alert rules
complex binary operators
regex matchers unless added deliberately later
```

### Logs query subset

Required:

```text
{service="api"}
{service="api", level="error"}
{service="api"} |= "timeout"
```

Explicitly unsupported in v1:

```text
regex filters
line formatting
JSON parsing pipeline
metric queries from logs
complex LogQL aggregations
```

---

## Design Constraints

1. **Grafana compatibility is sacred** — do not replace the Grafana integration with a custom UI.
2. **Do not fake storage internals** — metrics/logs should use WAL, chunks, blocks, and indexes.
3. **Single-node first** — no distributed implementation before single-node correctness.
4. **Explicit unsupported behavior** — unsupported PromQL/LogQL features must return clear errors.
5. **Boring durability over clever complexity** — WAL and safe block writes matter more than fancy distributed features.
6. **No secrets in git** — credentials must come from env vars, Kubernetes Secrets, or Vault later.
7. **Demo-first discipline** — each phase should keep the project runnable.
8. **Testing is required** — storage and query code must be covered by unit and integration tests.

---

## Observability Standards

The backend itself must emit:

- Structured logs.
- Request IDs on every request.
- Component names on relevant log lines.
- `/metrics` endpoint for internal service metrics.

Required internal metric categories:

- Ingestion request rate.
- Ingestion error rate.
- Samples ingested per second.
- Log lines ingested per second.
- Query latency p50/p95/p99.
- WAL segment count and size.
- Metrics block count.
- Log chunk count.
- Compaction duration.
- Retention deletion count.
- Active series count.
- Active stream count.

---

## Performance Benchmarks

Two complementary harnesses, split by what each can control:

- **Go `testing.B` benchmarks** live beside the code they measure
  (`internal/metrics/ingest_bench_test.go`, `internal/metrics/query_bench_test.go`,
  `internal/storage/chunk/compression_bench_test.go`, plus the existing
  `*_bench_test.go` select benchmarks). Being in-process, they control storage
  state directly — `MemoryStore` vs `WALStore`, fsync policy, in-memory vs
  persisted reads, block count. Benchmarks importing `internal/compactor` use
  `package metrics_test` to avoid the `compactor → metrics` import cycle.
- **k6 HTTP load tests** live under `bench/k6/` and measure end-to-end API
  p50/p95/p99 latency and throughput. `bench/run.sh` (`make bench-k6`) starts a
  throwaway backend on a temp data dir, seeds it, runs every scenario, and tears
  down. Curated numbers live in `PERFORMANCE.md`; raw JSON in `bench/results/`
  (gitignored).

## Environments

| Environment | Purpose |
|---|---|
| Local Docker Compose | Fast correctness, Grafana compatibility, demo workflow |
| Local Kubernetes | Helm validation and pod restart behavior |
| Cloud Kubernetes | Optional stronger deployment signal after local demo is complete |

**Development order:** local single-node correctness → Grafana metrics → storage engine hardening → logs → Docker/Kubernetes demo → distributed mode.
