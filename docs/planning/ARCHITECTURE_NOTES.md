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

- **Log chunk format** (`internal/storage/logchunk`, on-disk **version 2**):
  `(tsNs, line)` entries with first-absolute / signed-varint-delta timestamps and
  uvarint-length lines, the whole entry block DEFLATE-compressed. Two CRC-32/Castagnoli
  checksums — a header CRC (over the timestamp bounds + counts, so a header-only read
  can authenticate them) and a payload CRC — plus a decoded-vs-header min/max check
  make `Bytes()`/`FromBytes()` self-validating.
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

#### Accepted decision: log chunk format break (v1 → v2)

The log chunk format was revised during Phase 4.3 development to add the header CRC
(header grew 37 → 41 bytes), bumping the on-disk version from 1 to 2. **This is a
deliberate, accepted one-time break, not a migration.** Because chunks are the
durable source of truth once their WAL records are checkpointed away, a v1 chunk
holds data with no WAL fallback — so the break is recorded here rather than left
implicit. It is safe because:

- No released version ever shipped v1; a v1 chunk can only exist in a local
  `data/logs/chunks/` from a mid-development run of an unreleased build.
- It matches the existing precedent for the metrics chunk format
  (`internal/storage/chunk`), which rejects superseded layouts outright rather than
  carrying multi-version decoders.

`FromBytes`/`PeekBounds` reject a v1 chunk with an explicit *"unsupported chunk
version 1 (expected 2)"* error (the version byte at offset 4 is the discriminator),
and a rebuild fails closed rather than misreading it. **Recovery for a local dev
data dir:** back up and remove the pre-v2 files under `data/logs/chunks/` (and the
stale `data/logs/index/streams.index`); metrics storage under `data/metrics/` is
unaffected and must not be touched. If preserving pre-v2 log data ever becomes a
requirement, add a version-1 decode path in `logchunk.FromBytes` (v1 bounds were not
checksummed, so a v1 rebuild must fully decode rather than peek).

### Loki query path (introduced in 4.4)

- `internal/logs/logql.go` — LogQL subset parser: equality stream selector `{k="v"}`
  plus chained line filters `|=` / `!=` / `|~` / `!~`. Pipelines, formatters,
  metric/aggregation queries, and regex/negative label matchers return explicit errors.
  String literals use Go lexing rules (`"a\"b"` and backtick raw strings, unescaped via
  `strconv.Unquote`) because ingest accepts any UTF-8 label value, so a value containing
  a quote must stay queryable. The selector is scanned rather than split, so malformed
  input errors instead of silently becoming a different query.
- `internal/logs/scalar.go` — constant metric queries (`vector(N)` with `+ - * /`),
  accepted **only** on instant `/query`. This exists solely so Grafana's Loki datasource
  health check (`vector(1)+vector(1)` must equal 2) passes; it reads no stored data.
  *(As of 4.4 that made it the only metric-shaped query accepted anywhere;
  `rate`/`sum`/`count_over_time` returned the explicit unsupported error. Phase 4.6
  added those as real, data-reading queries — `ParseMetricQuery` now runs first on the
  instant path and falls back to this shim on `ErrNotMetricQuery`, so the health-check
  behavior described here is unchanged.)* The
  envelope's **result type follows the expression shape**, as upstream derives it from the
  AST: a literal-only expression (`1+1`) is a LogQL LiteralExpr and answers
  `resultType: "scalar"` with a bare `[ts, "value"]` pair, while anything mentioning
  `vector()` is a VectorExpr and answers `resultType: "vector"`. Both carry the same
  number, so collapsing them is invisible until a client switches on the type —
  `ScalarResult.HasVector` keeps them apart.
- `internal/logs/query.go` — `QueryEngine` over a `Reader` interface (`*logs.Store`
  satisfies it): match streams by label → read entries → line-filter → cap per stream →
  global order-by-direction + limit → regroup by stream. `QueryRange` is half-open
  `[start, end)` per Loki; `QueryInstant`'s `time` is inclusive. The per-stream cap is
  lossless (a global top-N never draws more than N from one stream) and bounds what is
  carried *across* streams at O(streams × limit) — but only for `limit > 0`, which every
  HTTP request satisfies (`parseLokiLimit` defaults to 100 and rejects `<= 0`). `limit
  <= 0` means "no cap" and is reachable only by calling the engine directly; it retains
  every match, so peak is O(all matching entries). Neither case bounds the transient
  per-stream working set: `StreamEntries` still materializes every in-range entry for a
  stream, so peak with a positive limit is O(largest matching stream + streams × limit)
  and one hot stream can still dominate. Bounding that needs selection pushed into the
  read (a lazy per-stream cursor feeding a k-way merge) — deferred; see design §9. `ctx`
  flows from the request into the store for cancellation.
- `internal/logs/diskstore.go` — `StreamEntries` snapshots index refs + head entries
  under `s.mu`, then decodes chunk files **outside** the lock, so cold-chunk queries do
  not block ingestion. This relies on log chunk files being immutable and never deleted
  — revisit when logs retention/compaction lands.
- `internal/api/loki_query.go` + `loki_response.go` — `GET /loki/api/v1/{query,query_range,
  labels,label/{name}/values}`. Loki-native nanosecond timestamps, `limit`/`direction`
  defaults (100 / backward), and **plain-text** error bodies (deliberately distinct from
  the Prometheus JSON error envelope). Label endpoints accept but ignore `start`/`end`
  this phase. `GET`-only is sufficient: Grafana's Loki backend never posts.
- `query_range` time bounds follow upstream precedence (`determineBounds` in
  `pkg/loghttp/params.go`): an explicit `start` beats `since` (a duration), which beats
  the one-hour default. A relative start is measured from `min(end, now)`, so `end` in
  the future still means "the last hour of data" rather than an empty future window.
  `since` uses the **Prometheus** duration grammar, not Go's — Loki parses it with
  `model.ParseDuration`, so `1d`/`1w`/`1y` and a bare `0` are valid while `150ns` and
  `1.5h` are not. `metrics.ParsePromDurationNanos` is that grammar, already in the tree for
  PromQL range selectors and `step`, so the Loki path reuses it rather than promoting
  `prometheus/common` to a direct dependency.
- `step` has **no effect** on a stream response, but it is still parsed and validated,
  because upstream runs one `ParseRangeQuery` across both log and metric queries: float
  seconds or a Prometheus duration, non-positive rejected, and the 11,000-points-per-
  timeseries safety limit enforced. Accepting `step=bogus` with a 200 would be the
  divergence, not the leniency. An absent `step` needs no check — upstream then derives
  it from the range (`max(floor(rangeSeconds/250), 1)` seconds), which cannot trip
  either rule. A span wider than int64 nanoseconds (~292 years) is **saturated** to the
  maximum duration before the division, as `time.Time.Sub` does upstream — rejecting the
  wrapped value outright would 400 a full-range query that a coarse step makes one point.
  `interval` is **rejected** with a 400, because ignoring it would return more entries
  than asked for while looking like it worked.
- `step` is parsed at **nanosecond** resolution (`parseLokiStep`), not the millisecond
  resolution the Prometheus query path uses. Loki keeps a `time.Duration`, so a
  sub-millisecond step is legal and decides whether the points limit trips: `0.0001`
  over a 1s range is a valid 10,000 points, and `0.0005` over 6s is an invalid 12,000.
  Rounding to milliseconds gets *both* wrong — the first becomes a zero step and is
  rejected, the second doubles to 1ms and is allowed. This is why
  `metrics.ParsePromDuration` is a thin millisecond wrapper over
  `ParsePromDurationNanos` rather than the other way round; the nanosecond form is also
  what makes the grammar's range checks match upstream (`106752d` fits in int64
  milliseconds but overflows int64 nanoseconds, and upstream rejects it).
- `direction` is matched case-insensitively, as upstream does by upper-casing the value
  before looking up its protobuf enum. Grafana sends lowercase, so this only matters to
  hand-written clients — but the API advertises Loki compatibility.

### Grafana demo assets (introduced in 4.5)

Both Grafana datasources point at the **same backend port**: `obs-prometheus`
(type `prometheus`) and `obs-loki` (type `loki`), each `http://backend:8080`. One Go
process serves both compatibility subsets; nothing proxies or translates between them.

Provisioned dashboards: `obs-metrics-v1` (Observability Platform Metrics, 4.5's
predecessor in 2.5) and `obs-logs-v1` (Observability Platform Logs). The logs dashboard's
label variables are single-select by necessity — a multi-value selection interpolates a
regex label matcher, which the equality-only index does not serve.

The compose demo sets `OBS_LOGS_FLUSH_THRESHOLD_BYTES=16384` on the backend. This is a
**demo-only** override of the 8 MiB default: at demo volume the head buffer would never
cross the default threshold, so every query would be answered from memory and the 4.3
chunk/index read path would never execute.

Known gaps against a real Loki, all returning explicit errors or 404 rather than wrong
answers: `unwrap` and the label-extraction range aggregations, vector aggregations
other than `sum`, binary operations, live tail (`/loki/api/v1/tail`, needs WebSocket),
and `/loki/api/v1/index/stats` (query size estimate).

### Demo stack (introduced in 5.1)

Two producers, split by signal ownership rather than by container convenience:

| Producer | Metric names | Logs |
|---|---|---|
| `examples/load-generator` | `http_requests_total`, `http_errors_total`, `http_request_duration_seconds`, `active_connections` | none |
| `examples/sample-app` | `sample_app_requests_total`, `sample_app_errors_total`, `sample_app_request_duration_seconds`, `sample_app_active_workers` | five streams over `service` × `level`, all `env=local` |

**The namespaces are separated by metric name, not by a `service` label.** The metrics
dashboard aggregates `sum by (method)(rate(http_requests_total[1m]))` with no service
filter, so a second writer under that name would fold into panels it has nothing to do
with, whatever labels it carried. The sample app's values are an independent simulation:
they are not derived from the log lines it pushes, and no panel or runbook claims the two
signals correlate.

Three provisioned dashboards: `obs-metrics-v1` (load generator), `obs-logs-v1` (sample
app logs), `obs-sample-app-v1` (sample app metrics). Phase 5.3 adds a fourth for backend
internals.

**Health-gated startup.** The backend runtime image is distroless — no shell, curl, or
wget — so its Compose healthcheck execs the server binary itself: `/server -healthcheck`
probes `/readyz` over loopback and exits 0 or 1. `/readyz` creates and removes a temp
file in the data directory, so a passing probe means the process is serving *and* its
storage is writable. Both producers wait on `condition: service_healthy`; Grafana does
not, because its datasources are `access: proxy` and resolved on first query. Phase 5.2's
Kubernetes probes use `httpGet` instead of this exec command — see "Kubernetes topology"
below for why the two environments correctly differ here.

### Kubernetes topology (introduced in 5.2)

Three separate Helm charts under `deployments/helm/` — `backend`, `grafana`,
`producers` — rather than one umbrella chart, because they scale and fail
independently: the backend is stateful and singular, Grafana is stateless and singular,
and the producers are optional demo traffic an operator might disable or scale without
touching either of the other two.

**Cross-chart contract.** The grafana and producers charts both default `backend.url` to
`http://observability-backend:8080` — the literal Service name the backend chart creates
via its pinned `fullnameOverride: observability-backend`. Helm renders each chart in
isolation and never checks a claim one chart makes about another, so two charts silently
pointing at a Service the third no longer creates is possible with every chart still
linting clean. `tests/e2e/helm_test.go`'s `TestCrossChartBackendURLResolves` is the
Kubernetes analogue of `TestLokiDatasourceURLMatchesComposeBackend` (which
cross-references `docker-compose.yml` instead of trusting a literal): it renders all
three charts and fails if any `backend.url` doesn't resolve to a Service name and port
the backend chart's own rendered output actually defines.

**StatefulSet, not Deployment.** The backend owns a WAL and on-disk chunks/blocks on a
`ReadWriteOnce` volume. A Deployment's rolling update starts the new pod before
terminating the old one; the new pod would wait forever for a volume the old pod still
holds, which presents as a hung rollout rather than a clear error. A StatefulSet
terminates the old pod first. `volumeClaimTemplates` gives each pod (there is only one)
its own PVC, which also seeds Phase 6 — sharding needs one PVC per shard, and the
StatefulSet's stable per-pod identity (via a headless Service) is what a ring assigns
shards to.

**The `httpGet`-over-exec probe correction.** Kubernetes probes are performed by the
kubelet from outside the container, unlike Docker Compose's healthcheck, which execs a
command *inside* the container. The backend's runtime image is distroless — no shell —
which is exactly why the Compose healthcheck has to exec the server binary's own
`-healthcheck` mode instead of a normal `curl`. That constraint does not apply to a
kubelet probe, because it never enters the container to run anything; it just makes an
HTTP request from the node. So the Kubernetes StatefulSet uses plain `httpGet` probes —
startup and readiness against `/readyz`, liveness against `/healthz` — while Compose
correctly keeps its exec-based healthcheck. Neither is a mistake; each is right for the
mechanism its platform actually uses to probe. The startup probe carries a 150-second
budget (`periodSeconds: 5` × `failureThreshold: 30`), because WAL replay on a large
volume can outlast a readiness deadline, and without that budget a slow replay would be
killed and simply restart into the same replay, forever. Liveness is pinned to
`/healthz` rather than `/readyz` specifically because `/readyz` touches disk (it creates
and removes a temp file to prove the data directory is writable); pointing liveness at
it would turn a full volume into an endless restart loop instead of a clear "not ready."

**The dashboards ConfigMap is operator-created, not chart-shipped.** Helm can only read
files inside its own chart directory, so copying
`observability/grafana/dashboards/` into the grafana chart would fork a second copy that
drifts from the one Compose provisions. Instead the chart mounts a ConfigMap
(`dashboards.configMapName`, default `grafana-dashboards`) that the operator creates
directly from that directory before installing the chart — one source of truth instead
of two. The mount is deliberately **not** `optional`: a missing ConfigMap leaves the pod
in `ContainerCreating`, naming exactly what's absent, which is louder than a Grafana
that starts happily with three empty dashboards. `tests/e2e/kind_smoke.sh` runs the
documented `kubectl create configmap` command verbatim in CI, so the runbook step is a
tested path rather than a hope.

**Single-replica ceiling.** The backend StatefulSet runs one replica, deliberately, not
as a temporary gap. Each replica would own a private PVC and a private WAL; with more
than one, a query would see whichever shard happened to receive it, with no merge across
replicas. That ceiling is real and is recorded here rather than hidden — resolving it is
exactly what Phase 6's ring-based sharding and query fanout are for.

### LogQL metric queries (introduced in 4.6)

- `internal/logs/metricql.go` — the metric subset: `count_over_time`, `rate`,
  `bytes_over_time`, `bytes_rate` over the 4.4 selector and line-filter grammar,
  optionally wrapped in `sum`, `sum by (...)`, or `sum without (...)`. Range
  durations follow upstream LogQL's own `parseDuration`: the Prometheus grammar
  first (so `1d`/`1w` work), then Go's `time.ParseDuration` (so `1.5h`/`150ns`
  work) — a wider grammar than `since`, which is Prometheus-only, and that
  asymmetry is upstream's. `ErrNotMetricQuery` distinguishes "belongs to another
  parser" (a selector, a literal, `vector()`) from "unsupported", which is what
  lets the instant endpoint keep its constant-expression shim.
- **`| drop <labels>` is the one supported pipeline stage**, accepted only in last
  position and only with bare label names. It exists because Grafana 11.1.0 appends
  `| drop __error__` to every Explore log-volume query before wrapping it
  (`getSupplementaryQuery` in `public/app/plugins/datasource/loki/datasource.ts`)
  — so without it the histogram this phase exists to serve still returned 400. The
  stage is *implemented*, not waved through: dropped names are removed from the output
  label set in both the metric path (`groupOf`, before grouping, so `sum by (level)` on
  a query that dropped `level` sees it as absent) and the log path
  (`StreamResult.Labels`). For `__error__` that is exactly a no-op, since no parser
  stage runs and the label is never set. Dropping never affects stream *matching*,
  which happens on stored labels before the pipeline. Every other stage — `| json`,
  `| logfmt`, `line_format`, `| unwrap` — still returns the explicit pipeline error.
- Because `drop` mutates the labels that *define* a stream, the log path groups results
  by the **post-drop label set** rather than by stream ID: two streams differing only by
  a dropped label come back merged, with their entries interleaved in global time order,
  as Loki returns them. Absent a drop stage the two groupings are equivalent, since
  stream label sets are unique by fingerprint. The metric path needed no change — it
  already groups by output labels.
- `internal/logs/metriceval.go` — ticks at `start, start+step, … ≤ end`; window
  `(t − range, t]`, start-exclusive and **end-inclusive at every tick including the
  last**, so entries read `[start − range, end]`. Upstream lands in the same place
  from the other direction: its sample reads are half-open, so it adds a leap
  nanosecond to the selected end — *"add leap nanosecond to endTs to include lines
  exactly at endTs. range iterators work on start exclusive, end inclusive ranges"*
  (`pkg/logql/evaluator.go`). Reading inclusively is that without the off-by-one.
  This deliberately differs from the **log** path's `QueryRange`, which is half-open
  `[start, end)` — also upstream's behavior, because a log query returns entries in
  a range while a metric query evaluates windows that close on their tick. Instant
  `query` is simply the single-tick case; it needs no special boundary rule. Empty
  windows emit no point — a gap, as Prometheus and Loki do, not a zero.
  *(4.6 originally excluded entries at exactly `end`, reasoning from upstream's
  half-open reads without accounting for the leap nanosecond. That made the final
  tick narrower than every other tick; corrected after review.)*
- Evaluation is a two-pointer sliding window per stream, `O(entries + ticks)`,
  which is the shape of upstream's `batchRangeVectorIterator`. Nothing is
  allocated in proportion to the tick count except the emitted points; the HTTP
  layer's 11,000-point limit is what bounds the loop.
- Output labels: a bare range aggregation keeps the stream's label set verbatim;
  `sum` drops all labels; `by` keeps the listed labels the stream carries; `without`
  drops the listed ones. Absent labels are **omitted**, per Prometheus — unlike
  `internal/metrics`'s aggregator, which emits `label: ""`. An empty label value
  groups as absent, because both render to the same output label set and splitting
  them would put two identically-labelled series in one response.
- `step` finally does something. `resolveLokiStep` returns it in nanoseconds and
  derives an absent one as upstream's `ParseRangeQuery` does — `max(floor(
  rangeSeconds/250), 1)` seconds, up to ~500 points (floor division holds the step
  at 1 second for any span in `[250s, 500s)`, not 250 as the denominator alone
  would suggest), matching upstream's `defaultQueryRangeStep`. Log queries still
  call it for validation and discard the value. `limit` and `direction` are parsed
  by the shared parameter parser and then ignored on the metric path, as upstream
  ignores them.
- A metric query ignores `limit` and reads every matching entry in its window, so
  the 4.4 note about the transient per-stream working set applies with more force:
  `StreamEntries` still materializes a stream's whole in-range slice. The evaluator
  itself is single-pass; bounding the read needs the same deferred lazy cursor.
  The **output** side is unbounded the same way: a metric query fans out over
  every matching stream (or every distinct group, under `sum by`/`without`) with
  no cap, so a response can carry one series per stream, each with up to the
  11,000-point ceiling, fully materialized before serialization. The log path
  caps its response at `limit`; the metric path has no equivalent — upstream's
  `max_query_series` (default 500) is not implemented. Only the query's own time
  bounds and `step` constrain the response size today.

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
{service="api"} != "healthz"
{service="api"} |~ "timeout|deadline"
{service="api"} !~ "^debug"
```

Line filters take all four operators (`|=`, `!=`, `|~`, `!~`) and chain, so **regex
applies to log *lines***. Label matchers inside `{...}` remain equality-only.

Metric queries over logs arrived in Phase 4.6 — `count_over_time`, `rate`,
`bytes_over_time`, and `bytes_rate`, optionally wrapped in `sum` / `sum by` /
`sum without`, on both query endpoints. See "LogQL metric queries (introduced in 4.6)"
above for the semantics.

Explicitly unsupported:

```text
regex label matchers — {service=~"api|web"} and {service!~"..."}
non-equality label matchers — {service!="api"}
line formatting
JSON parsing pipeline — and every other pipeline stage except final-position
  | drop <labels>, which Grafana appends to every log-volume query
unwrap and the label-extraction range aggregations — avg_over_time,
  quantile_over_time, max_over_time, ...
vector aggregations other than sum — count, avg, min, max, topk, ...
binary operations — sum(...) / sum(...), count_over_time(...) * 2
the offset modifier
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
