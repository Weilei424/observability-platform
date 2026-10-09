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
| Compactor | Compaction planning, cadence, and retention policy; in split, the store executes |
| Gateway | The public entry point in the split topology: proxies write routes to the ingester and read routes to the querier (6.1); ring-based write routing arrives in 6.2 |
| Querier | The public read routes over the ingester and store, merged ingester-first |
| Store | Persisted blocks and log chunks; flush-in; executes the compactor's plans |
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

**The authoritative on-disk layout is
[`docs/architecture/storage-layout.md`](../architecture/storage-layout.md)**,
which is verified against a directory the code actually produced by
`TestStorageLayoutDocMatchesDisk`. This section keeps only the decisions behind
the layout.

The Phase-0 version of this section was headed "Recommended local data layout"
and was written before any of it existed. It fell behind without anyone
noticing: `blocks/<id>/postings` and `metrics/checkpoint` were on disk and in no
document. A layout description that nothing executes is a layout description that
drifts, which is why the replacement is a tested document and this one is a
pointer.

**Metrics and logs get separate subtrees and separate write-ahead logs.** They
have different record shapes, different flush triggers, and different failure
modes; sharing a WAL would couple their durability and make a replay failure in
one a startup failure for both.

**Blocks are immutable, and their IDs are random rather than derived.** Nothing
rewrites a block in place, so a reader never observes a partial mutation; updates
arrive as new blocks and compaction merges them. The ID is 8 bytes of
`crypto/rand` — not a content hash and not a counter. A counter would have to
survive restarts to avoid reusing the name of a block that was just deleted, and
content addressing would buy deduplication this system has no use for.

**Immutable files are published by rename; the WAL is not.** A block directory
and a log chunk file are written to a temporary path, fsynced, atomically renamed
into place, and their parent directory fsynced, so a half-written one is never
visible under its final name. The WAL is necessarily the exception: records are
appended to the open segment and fsynced in place — every record by default, or
every Nth under `wal_sync_every_n`. A log that could only be published by rename
could not acknowledge a write until its segment rolled, which is the opposite of
what a write-ahead log is for.

**Publication order is not a substitute for validation, and readers do not skip
it.** A log chunk is checked on every read against its magic, version, header
CRC, uncompressed-size cap, declared compressed length, payload CRC, and whether
the decoded entries' bounds match the header. WAL replay tolerates exactly one
fault: a torn record at the tail of the final segment, which it truncates and
fsyncs before opening a newer one. Anything else is an error rather than a
repair, because anything else means something other than a crash-at-write.

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

`deployments/docker/docker-compose.split.yml` runs the split topology (6.1) with the gateway
aliased as `backend`, so every datasource, producer, and URL that names the backend reaches
it unchanged. Its ingester and store get a 60 s `stop_grace_period` (the ingester's graceful stop is bounded by a 50 s shutdown budget: a 10 s HTTP drain, then the drain of its head).

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

### Kubernetes topology (introduced in 5.2; `prometheus` chart added in 5.3; split in 6.1)

Four separate Helm charts under `deployments/helm/` — `backend`, `prometheus`,
`grafana`, `producers` — rather than one umbrella chart, because they scale and fail
independently: the backend is stateful and singular, the self-observability Prometheus
and Grafana are each stateless and singular, and the producers are optional demo traffic
an operator might disable or scale without touching any of the other three.

**Cross-chart contract.** The grafana and producers charts both default `backend.url` to
`http://observability-backend:8080` — the literal Service name the backend chart creates
via its pinned `fullnameOverride: observability-backend`. The prometheus chart carries
the same URL under its own `backend.url` (its scrape target), and the grafana chart's
`internals.url` in turn defaults to `http://observability-prometheus:9090` — the
prometheus chart's own pinned `fullnameOverride`. Helm renders each chart in isolation
and never checks a claim one chart makes about another, so a rename in any one of these
is possible with every chart still linting clean. `tests/e2e/helm_test.go` checks every
edge of this: `TestCrossChartBackendURLResolves` (the Kubernetes analogue of
`TestLokiDatasourceURLMatchesComposeBackend`, which cross-references
`docker-compose.yml` instead of trusting a literal) covers grafana and producers against
the backend chart; `TestPrometheusChartScrapesTheBackendService` covers the prometheus
chart against the backend chart; `TestGrafanaInternalsURLResolvesToThePrometheusService`
covers the grafana chart against the prometheus chart. Each renders the charts on both
ends of a claim and fails if the URL doesn't resolve to a Service name and port the
other chart's own rendered output actually defines.

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
that starts happily with four empty dashboards. `tests/e2e/kind_smoke.sh` runs the
documented `kubectl create configmap` command verbatim in CI, so the runbook step is a
tested path rather than a hope.

**Single-replica ceiling.** The backend StatefulSet runs one replica, deliberately, not
as a temporary gap. Each replica would own a private PVC and a private WAL; with more
than one, a query would see whichever shard happened to receive it, with no merge across
replicas. That ceiling is real and is recorded here rather than hidden — resolving it is
exactly what Phase 6's ring-based sharding and query fanout are for.

**Split topology (6.1).** The backend chart's `topology: split` renders StatefulSets for the
ingester and store (one PVC each) and Deployments for the gateway, querier, and compactor.
The gateway inherits `observability-backend`, so the cross-chart contract holds unchanged.
The single-replica ceiling now applies per component to the ingester, store, and compactor,
and the chart refuses more than one until 6.2. The ingester and store set
`terminationGracePeriodSeconds: 60` so a graceful stop can finish an in-flight flush.

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

### Component split (introduced in 6.1)

`OBS_TARGET` selects one of `all-in-one` (the default, unchanged), `gateway`, `ingester`,
`querier`, `store`, or `compactor` from the same binary. The reference for what each
serves, owns, and calls is `docs/architecture/components.md`, and the internal wire
format is `docs/api/internal.md`; this section records only the decisions that shaped
them.

Deliberate change to all-in-one's output: the compactor's "flush failed" and
"final flush failed" lines (`internal/compactor/compactor.go`) went from WARN to
ERROR in every target, all-in-one included, not just the split components. A
failed block flush is an operator-visible fault, and the split topology needed
it at ERROR; the level is not made target-dependent.

- **One owner per data directory.** The ingester owns `metrics/wal`, `metrics/checkpoint`,
  `metrics/genfloor`, and `logs/wal`; the store owns `metrics/blocks`, `metrics/tmp`,
  `logs/chunks`, and `logs/index`. No volume is shared, so no process reasons about another
  mutating a directory under it. all-in-one owns everything and writes no `genfloor`.
- **HTTP + JSON internal API** under `/internal/v1`, with string-encoded sample values
  and no new dependency. It is unauthenticated, like the public API, and reachable in
  Kubernetes only through ClusterIP Services. The gateway never proxies it.
- **The gateway is a route-level proxy.** It forwards write routes to the ingester and
  read routes to the querier without parsing bodies. Write parsing moves into the gateway
  in 6.2, when the ring needs a series or stream key to route on.
- **Policy versus mechanism for compaction.** The compactor owns the plan, the cadence, the
  retention clock, and the maintenance metrics; the store owns execution and re-checks each
  group before running it, under the same lock a flush takes.
- **Bulk `Select` reads, merged ingester-first.** The querier reads the ingester to
  completion, then the store. A flush is invisible to that order: the store registers a
  block before it acknowledges the flush, the ingester discards chunks only after the
  acknowledgement, and the querier finishes the ingester before it starts on the store, so
  a sample absent from the ingester's answer was already in the store. Overlap resolves by
  one rule per signal, shared with all-in-one: `sortAndDedup` for metrics (timestamp, then
  highest generation) and `mergeEntries` for logs (timestamp and line, persisted ahead of
  head).
- **The persisted generation floor.** Last-write-wins between two samples at one timestamp
  is decided by write generation, and the blocks that would seed the ingester's counter
  live in the store. The ingester writes `metrics/genfloor` before sending anything, so no
  block it ships can outrank a write it accepts after a restart.
- **The per-chunk WAL fence.** Each in-memory chunk records the WAL segment current when it
  was allocated, and the checkpoint boundary is the minimum over every chunk still in
  memory, sealed or not. One value per series was not enough: a chunk sealed during a flush
  stays in memory, and the head chunk allocated after it would move the fence past its
  segments and let the checkpoint delete WAL still needed for replay. Because a failed
  batch skips the checkpoint, the fence keeps a later one correct.
- **Flush batching.** Log flush batches are sized by encoded bytes, so escaped lines still
  land under the store's body limit; metrics batches are additionally capped by
  `block.MaxChunksPerSeries` per series, so a large backlog cannot produce a block the
  reader refuses. Flush failures log at ERROR.
- **Reads fail closed.** A query never answers from one source while the other is down. A
  peer outage or a deadline answers `503`; a caller that cancels answers `499`, which is
  not a server error.
- **Readiness is local only.** A process reports its own data directory, never its peers',
  so one outage does not mark its dependents unready.
- **Strict peer URLs.** A target refuses to start if a peer URL it needs
  (`OBS_INGESTER_URL`, `OBS_STORE_URL`, `OBS_QUERIER_URL`) is missing or one it does not use
  is set.

**Resolved in 6.2: clock generations.** The per-ingester counters 6.1 left behind are
replaced by hybrid clock generations; see "Ring-based sharding (introduced in 6.2)".

### Ring-based sharding (introduced in 6.2)

`OBS_INGESTER_URL` takes a comma-separated list on the gateway and querier, and the
gateway routes each write to the ingester that owns it. The design is
`docs/superpowers/specs/2026-10-01-phase-6.2-ring-sharding-design.md`; this section
records the decisions and the one place the code diverged from it.

- **A token ring, not `hash % N`.** Each member holds 128 tokens; a key is mixed with
  splitmix64 and owned by the first token at or after it, clockwise. This is the shape
  Cortex and Loki use, and it is what 6.3 needs: replication walks the ring to the next
  distinct members. Ties between equal tokens go to the member sorting first, and
  placement ignores the order of the configured list (`TestPlacementIgnoresListOrder`,
  `TestRingHashIgnoresOrder`, `TestTokenTieGoesToTheMemberSortingFirst`). Balance is
  within +-25% for 3 and 5 members (`TestBalance`); adding a member moves 1/(N+1) +- 0.05
  of the keys and only to that member (`TestAddingAMemberMovesOnlyItsShare`).
  `TestGoldenPlacement` pins the placement, so a change to it fails loudly.
- **Keys.** Series route by series fingerprint; streams by stream fingerprint, so one
  stream stays on one ingester.
- **Static membership.** A change takes a restart of the gateway and querier. Both log
  a `ring` line with the member count and a hash of the sorted list; Compose and the
  runbook compare the two. Only Helm renders both lists from one helper; in Compose they
  are two hand-edited literals, checked by that hash.
- **The gateway validates with all-in-one's code.** The write proxy is gone: the
  gateway decodes and validates with the same handler code as all-in-one
  (`TestGatewayValidationMatchesAllInOne`), then sends each owner its group over
  `POST /internal/v1/metrics/push` and `/logs/push`. Groups go out concurrently and the
  worst outcome wins: `500` over `503` over `499`. The gateway owns the ingest rejection
  counters. An already-expired write deadline is routed as an outage (`503`), matching
  `Client.do` (`TestRouterExpiredDeadlineIsUnavailable`).
- **Divergence from the spec.** Spec 5.2 said the ingester push route keeps no
  rejection counter. The code counts append failures there under rejection reason
  `append`: the gateway already accepted the sample, so a failure to store it would
  otherwise be invisible. On an append failure the push route stops and counts the
  remainder under `append`, while all-in-one's metrics handler continues past a failure
  and its logs handler counts abandoned lines under `batch`; totals match, the reason
  split differs.
- **`MergeHeads` and the no-gap order.** The querier reads every ingester to
  completion, sequentially, then the store; `metrics.MergeHeads` / `logs.MergeHeads`
  combine the ingester answers with the 6.1 rules, then the existing merge adds the store.
  The 6.1 argument still holds per ingester: a flush registers its block before the
  ingester discards, and all ingesters finish before the store starts, so a sample
  missing from every ingester answer was already in the store. Any ingester failing
  fails the whole read (`503`). Parallel fanout and pruning stay with 6.4.
- **Hybrid clock generations.** A write's generation is `max(previous + 1, now in Unix
  microseconds)` in every target, with an injectable clock. It is monotonic per series
  even if the clock steps back, and across ingesters it orders a moved series' writes by
  wall clock, which resolves 6.1's hand-off. The persisted floor (`genfloor`) still keeps
  a restarted ingester ahead of anything it shipped.
- **WAL record type 2.** A new record type carries the generation; type-1 records
  replay as generation 0, and replay restores generations exactly and raises the floor
  past them (`TestReplayRestoresExactGenerations`,
  `TestReplayRaisesFloorPastRestoredGenerations`). A truncated or padded type-2 record is
  corrupt like any other. After upgrading to 6.2, a type-1 record replays with a fresh
  generation, so while pre-6.2 segments are still past the checkpoint a pre-upgrade sample
  can outrank a post-upgrade overwrite at the same series and timestamp; let the head
  flush (the checkpoint passes those segments) before relying on same-timestamp
  overwrites across a restart.
- **Measured cost.** `TestGenerationEncodingCost` (120-sample chunk, 15 s interval):
  4.64 bytes/sample with the pre-6.2 counter, 5.63 with microsecond clock generations,
  +0.99 bytes/sample.
- **Membership changes.** Adding a member moves about 1/(N+1) of the keys; older data
  stays on the previous owner until it flushes, every read covers both, and generations
  order any overlap. The integration test (3 to 4) checks this: the old owner's graceful
  stop flushes its write into a store block with its original generation; exact replay
  restoration is proven at unit level. A change is staged so the gateway never writes to
  an ingester the querier does not read: adding, the querier gains it before the
  gateway; removing, the gateway drops it, the ingester stops, then the querier drops
  it. Helm stages this with `split.ingester.writeReplicas` (the gateway's list is a
  prefix of the querier's) and, because the two Deployments roll independently, reads
  with `lookup` on an upgrade the ingester counts the running pods loaded (a
  pod-template annotation on each Deployment), refusing a change whose old and new
  pods could overlap unsafely and any ring change while either Deployment is still
  rolling out (`TestRingStagingReadsTheRunningRelease` over fixture objects;
  `split.ingester.previous` stands in for a preview, and a real upgrade refuses it);
  `TestRingClusterStagedMembershipChangeNeverHidesWrites` drives both stages with
  writes in every window.
- **Ingesters drain, with an acknowledgment.** `POST /internal/v1/drain` first
  closes a write barrier (`internal/drain`): every write route, public and internal,
  refuses with `503` from then until a restart, and the drain waits for writes
  already admitted. It then seals every open head chunk and flushes the whole head —
  metrics and logs — within 40 s, lock waits included, answering `200` only when it
  all reached the store and the head was empty after, so the `200` covers every
  acknowledged write (`TestRingClusterDrainIsAWriteBarrier`). Removal waits for that
  `200` before the querier drops the ingester. A graceful stop runs the same drain in
  place of the maintenance loop's final flush; the loop's own flush is cancelled at
  shutdown, and the whole stop shares one 50 s budget (`cmd/server`) inside the 60 s
  grace period. Its outcome is only a log line. A restart therefore writes one more small block (compaction merges it) and
  normally replays nothing; all-in-one is unchanged.
- **Failure semantics.** At RF=1 (6.2's behaviour, still the binary default): one
  ingester down, batches with keys it owns answer `503`, other batches `204`, reads
  `503`; a retry after recovery reads back once. Replication (6.3) changes this for
  RF above 1.
- **Observability and checks.** The kind and Compose spread checks read per-instance
  counters through the self-observability Prometheus, because the backend image is
  distroless.

**Hand-off to 6.3.** The ring already orders members around each key. Replication takes
the next R distinct members clockwise, and the write outcome rule becomes a quorum rule.
Reads already cover every member, so replicated duplicates are a dedup problem, not a
completeness one; the merge already deduplicates, so 6.3 proves it rather than adding
it (6.4 keeps parallel fanout and pruning).

### Replication (introduced in 6.3)

`OBS_REPLICATION_FACTOR` (RF) makes the gateway write every series or stream to RF
ingesters and the querier tolerate failed ones. The design is
`docs/superpowers/specs/2026-10-06-phase-6.3-replication-design.md`; this section
records the decisions and where the code corrected the spec.

- **Gateway-side, per-key quorum.** Quorum is `W = RF/2 + 1` (RF=1 -> 1, 2 -> 2, 3 -> 2,
  5 -> 3). `ring.Replicas(key, n)` takes the next n distinct members clockwise from the
  key's position, so `Replicas(key, 1)[0]` is `Owner(key)` and RF=1 places exactly as
  6.2 does (`TestReplicasOfOneIsTheOwner`, `TestGoldenReplicas`, `TestGoldenPlacement`).
  The router sends each member one push carrying every key it replicates, all
  concurrently, each bounded by `OBS_INGESTER_TIMEOUT`. Rejected: ingester-chained
  replication (an extra failure point and recovery questions) and a per-member quorum
  (keys in one batch have different replica sets).
- **Answer at quorum, finish in the background.** The router answers `204` as soon as
  every key has W acks. Pushes still running finish on a context detached from the
  request (`context.WithoutCancel`), still bounded by the timeout and still counted,
  so a hung replica costs one bounded goroutine per push. The moment a key can no
  longer reach W, the router answers `503` if every failure seen so far was an
  outage or `500` if any was a protocol error, without waiting for replicas still
  running (a Codex review found the first cut waiting for every push, so two down
  plus one hung took the full timeout to fail). A protocol error that lands after the
  decision is still counted but does not change the answer
  (`TestRouterAnswersAtOnceWhenQuorumIsImpossible`). The `503` body is `write quorum not met: <k> of <n> series
  could not reach <W> of <RF> ingesters` (`streams` for Loki); the `500` body stays
  `internal error` and the cause goes to the log at `error`. At RF=1 a failed
  outage write carries that same quorum message. A plain `rpc.ErrUnavailable` elsewhere
  keeps `ingester
  unavailable`. A caller that cancels first gets `499`; the pushes go on regardless.
- **Deadlines folded in.** `OBS_INGESTER_TIMEOUT` (default 10s, at least 100ms) bounds
  every routed write and every ingester read; a timeout is an outage, so a hung
  ingester is a failed replica and costs at most the timeout. This closes 6.2's deferred
  request-deadline item for ingesters; the querier still sets no overall request timeout.
- **Reads skip up to W-1 outages.** `metrics.MergeHeadsWith` / `logs.MergeHeadsWith`
  take `Tolerate = W-1` and a `Skippable` predicate (the querier passes
  `errors.Is(err, rpc.ErrUnavailable)`). An outage is a refusal, a `503`, or a
  per-request timeout while the caller's own read is still live; a cancelled caller is
  never a skip. The W-th outage, or any protocol error, fails the read; a read where
  every ingester was skipped fails with the last outage. Label-name and label-value
  reads use the same tolerance. A read that relied on it logs `read answered by
  replication` at `warn`.
- **Why the read is complete.** Every acknowledged write is on at least W replicas.
  With at most W-1 ingesters skipped, at least one replica of every acknowledged write
  is read. This needs no ring lookup and holds across membership changes, because older
  writes were also acknowledged by W replicas. The 6.2 no-gap argument still holds per
  ingester read, and a skipped ingester's flushed data is already in the store. RF=1
  gives `Tolerate = 0`: 6.2's fail-closed reads. The argument does not cover an RF
  change while heads hold data (below).
- **Duplicates.** Each ingester still assigns its own generation, so copies of one
  sample differ only in generation and collapse to one value on read (`sortAndDedup`);
  log lines dedup by `(timestamp, line)`. Dedup is proven for `sum`, `rate`, an instant
  series count and `count_over_time` plus raw lines, from heads, from the store after a
  flush has put RF copies there, and after compaction (`count()` is not in the PromQL
  engine). Metrics compaction merges the copies; log chunks stay RF x on disk.
- **Overwrite skew.** Two writes to one series and timestamp are ordered by the highest
  generation across all replicas. An overwrite can lose to the older value only when the
  replica whose clock runs furthest ahead missed the overwrite (a partial-quorum write)
  and that replica's old write carries a higher generation than the overwrite's on the
  replicas that got it. If every replica holds both writes, the overwrite always wins.
  The design spec words this as any overwrite inside the clock skew; the code is
  narrower, and `TestMergeHeadsOverwriteSkewWindow` pins the narrower rule.
- **RF changes.** `OBS_REPLICATION_FACTOR` must be equal on the gateway and querier
  at rest; compare the `replication_factor` and `quorum` fields of the two `ring ready`
  lines. A change is staged so the querier's quorum never exceeds the gateway's while
  an old pod of one runs beside a new pod of the other (a read skips up to quorum−1
  ingesters): raise RF gateway first, then the querier once the heads have flushed,
  because heads still hold data acknowledged under the smaller quorum; lower it
  querier first, then the gateway. Helm stages it with `split.querier.replicationFactor`
  and enforces the order as it does 6.2 membership stages: the render refuses a querier
  quorum above the gateway's, and an upgrade reads each running pod template's
  `observability-platform.dev/replication-factor` annotation and refuses the wrong
  order or an RF change mid-rollout. Compose stages it by editing the shared
  `x-ring-env` anchor and recreating one service at a time (`up -d --no-deps`).
  Both processes refuse an RF above their own ingester list at startup.
- **Surfacing.** `obs_gateway_write_quorum_total{outcome=full|degraded|failed}` counts
  each routed batch; per-replica results stay in `obs_gateway_ingester_requests_total`,
  including background pushes. `obs_querier_ingester_reads_total{ingester,outcome}`
  counts each ingester read (`ok`, `unavailable`, `error`). A degraded batch is only
  counted, so an outage does not log once per batch; a `503` quorum failure logs `write
  quorum not met` at `warn` with the failing ingesters. A read the caller cancels is
  not counted. The self-observability dashboard gains "Write
  quorum" and "Ingester reads" panels.
- **Deployment.** Compose split runs RF=3 through one env anchor shared by the gateway
  and querier (smoke 108/0 on 2026-10-07). Helm `split.replicationFactor` (default 3)
  renders into the gateway and querier ConfigMaps only, so only those two roll, and is
  refused above `split.ingester.writeReplicas`; `config.OBS_REPLICATION_FACTOR` is
  chart-owned. Removing an ingester from a 3-ingester RF=3 ring first needs RF lowered
  (`split.replicationFactor=2`) or a fourth ingester.
- **Failure semantics (RF=3).** One ingester down, refused, crashed or hung: writes
  `204` (degraded), reads complete `200`. Two down: writes `503` and reads `503`
  (with 3 members every key has both among its replicas). A protocol error that breaks
  a key's quorum: `500`. Store down: unchanged from 6.2.
- **Where the tests live.** `internal/ring` (`Replicas`, golden placement);
  `internal/rpc` router tests (quorum met and missed, per-key quorum, early answer
  while a replica hangs, outcomes counted); `internal/metrics` and `internal/logs`
  `merge_heads_test.go` (tolerance, cancellation, timeout, skew); `internal/app`
  `querier_tolerance_test.go`; `internal/config` (bounds); `tests/integration/
  replication_test.go` (one down, two down, hung, dedup); `tests/e2e` Helm tests;
  the Compose and kind smoke scripts.

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

Prometheus and Loki compatibility is a deliberate subset, not an attempt at
completeness. The rule that shapes it: an unsupported form must fail loudly with
a `400` rather than be ignored or approximated, because a query that quietly
returns a plausible-looking wrong answer is worse than one that refuses. That is
why the Loki `interval` parameter is rejected instead of dropped — honouring the
request partially would return *more* entries than asked for while looking like a
working filter.

Label matchers inside `{...}` are equality-only because they are index-backed: a
label pair maps directly to a posting list, and regex matching would mean scanning
every value. Line filters carry no such constraint, so they take all four
operators (`|=`, `!=`, `|~`, `!~`) and chain. Regex therefore applies to log
*lines*, never to label values.

`| drop <labels>` in final position is the one pipeline stage implemented, because
Grafana appends it to every log-volume query; supporting it is the difference
between a working log-volume panel and an error.

**The authoritative list of supported and unsupported forms is
[`docs/api/limitations.md`](../api/limitations.md)**, where every row is executed
against the real parsers by `TestDocumentedQueryFormsMatchTheParser`. Do not
re-enumerate it here — two lists drift, and only one of them is tested.

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

### Internal Metrics

The backend exposes the following metrics at `/metrics`, scraped by a separate Prometheus instance (not the backend's own TSDB). All metrics are prefixed `obs_` and use Prometheus naming conventions.

**Cardinality:**
- `obs_active_series` — active series in the backend's TSDB
- `obs_label_names_total` — distinct label names
- `obs_label_pairs_total` — distinct label name=value pairs

**Storage:**
- `obs_blocks_total` — persisted metric blocks
- `obs_blocks_bytes` — total metric block size in bytes
- `obs_wal_bytes{wal}` — WAL size in bytes; `wal="metrics"` and `wal="logs"`, one per WAL
- `obs_wal_segments{wal}` — WAL segment count, same two series
- `obs_log_streams_total` — distinct log streams
- `obs_log_chunks_total` — persisted log chunk files
- `obs_log_chunk_bytes` — total log chunk size in bytes

**Ingestion:**
- `obs_samples_ingested_total` — accepted metric samples
- `obs_samples_rejected_total{reason}` — rejected samples (closed-set reason labels: `name`, `timestamp`, `value`, `labels`, `other`, `append`, `batch` — see `internal/api/reject_reason.go`; `batch` covers a sample that was itself valid but discarded only because a sibling in the same atomically-rejected batch was invalid. The metrics handler attempts every append even after one fails, so a failed write is always `append` there, never `batch`; only the Loki push path abandons a tail after an append error)
- `obs_log_lines_ingested_total` — accepted log lines
- `obs_log_lines_rejected_total{reason}` — rejected log lines (closed-set reason labels: `values`, `timestamp`, `line`, `labels`, `other`, `append`, `batch` — same `batch` semantics as above)

**Queries:**
- `obs_http_requests_total{route,method,status}` — HTTP requests (route is chi pattern, never raw path; status is the numeric status code as a string, e.g. `"200"` or `"404"` — via `strconv.Itoa`, not a `1xx`/`2xx` class)
- `obs_http_request_duration_seconds{route,method}` — request latency histogram

**Maintenance:**
- `obs_compactions_total` — completed block compactions
- `obs_compaction_failures_total` — failed compactions
- `obs_compaction_duration_seconds` — compaction duration histogram
- `obs_retention_deleted_blocks_total` — blocks deleted by retention
- `obs_flushes_total` — successful metrics head flushes, incremented by whichever maintenance loop flushes: the compactor loop in all-in-one, the ingester's flush loop in split
- `obs_flush_failures_total` — failed metrics head flushes
- `obs_log_flushes_total` — successful log-store flushes
- `obs_log_flush_failures_total` — failed log-store flushes

**Ring (gateway and querier):**
- `obs_ring_members` — ingesters in the configured ring
- `obs_gateway_ingester_requests_total{ingester, outcome}` — gateway push requests per ingester and outcome
- `obs_gateway_write_quorum_total{outcome}` — gateway write batches by quorum outcome: `full`, `degraded` (quorum met, a replica failed), `failed`
- `obs_querier_ingester_reads_total{ingester, outcome}` — querier reads per ingester and outcome (`ok`, `unavailable`, `error`)

In split, `obs_active_series` counts the ingester's head series and `obs_log_streams_total` the store's persisted streams.

**Errors:**
- `obs_collector_errors_total{collector}` — scrape-time collector failures

### Architectural Decisions

#### Separate Prometheus for Platform Telemetry

Platform telemetry (metrics **about** the backend) is stored by a separate Prometheus instance, not by the backend's own TSDB. This separation prevents shared-fate failures: if the backend's metrics storage becomes unavailable, the platform's self-observability signals remain queryable and can diagnose the problem. The shared-fate scenario (using the same TSDB) creates a blind spot exactly when it matters most. The cardinality cost is also lower: the backend's TSDB holds workload metrics from the sample app and load generator (hundreds of series); the platform Prometheus holds only backend internals (tens of series) and stores them for 24 hours.

#### Collector Error Policy

A failed collector read (e.g., WAL directory permissions error) emits **a gap plus an incremented `obs_collector_errors_total` counter, never a zero.** A zero in a storage gauge means the size was measured and is truly zero; a gap means the read failed. This distinction is critical for correct dashboard interpretation: if a storage panel suddenly goes from 100MB to zero, the operator needs to know whether the backend shed storage (zero) or whether a permissions issue caused the collector to fail (gap). Using `prometheus.NewInvalidMetric` was rejected because it causes the entire `/metrics` scrape to return HTTP 500, blanking all panels and hiding other metrics that scraped successfully.

The counter says a read failed; it cannot say why, and both WALs share `collector="wal"`. So each collector also logs the underlying error, with a `source` field giving the logical source (`metrics` or `logs`, read together with `component`; the directory path itself is in `error`) — but only on state transitions: one line when a source starts failing and one when it recovers. Collectors run on every scrape, and logging each failure would repeat the same line every scrape interval for as long as a directory stays unreadable, burying the one line that says why. The transition is taken with `atomic.Bool.Swap`, so concurrent scrapes cannot log it twice.

#### Route Label Rule

HTTP request metrics (`obs_http_requests_total`, `obs_http_request_duration_seconds`) are labeled by chi **route pattern**, never by the raw resolved path. Examples: `/api/v1/query`, `/loki/api/v1/push`, `<unmatched>` for 404s. This prevents unbounded cardinality — a malicious client requesting `/api/v1/query?a=1&b=2&c=3...` would not create infinite metric series.

#### Component Name Set

Request-scoped and startup loggers carry a fixed `component` name. The set actually emitted is: `api`, `compactor`, `flush`, `gateway`, `logs`, `logs_push`, `logs_query`, `logwal`, `main`, `metrics_ingest`, `rpc`, `wal`. Each component name appears at most once per log line. Nothing enforces this set in code — `observability.Component()` accepts any string — so it is a call-site convention, not a constraint the logging helpers check. Every line also carries `target`, the process's `OBS_TARGET`; it is a field, not a component.

`main` covers `cmd/server/main.go`'s own generic startup/lifecycle lines (data directory creation, binding the listener, starting and shutting down the HTTP server) that are not specific to any one storage subsystem. Its startup lines that ARE specific to a subsystem reuse that subsystem's existing component name instead: WAL checkpoint/replay/open/close logs carry `wal`, and logs-store open/ready/close logs carry `logs` — the same names those subsystems already use for their own runtime log lines. `cmd/server/main.go`'s `log` value itself is `api.Deps.Logger` and stays component-free per that field's doc comment; every startup line goes through a separate, derived logger instead.

#### Datasource UIDs

Three Grafana datasources with pinned UIDs:

- `obs-prometheus`: Prometheus datasource at `http://backend:8080` (the backend's own TSDB; workload metrics)
- `obs-loki`: Loki datasource at `http://backend:8080` (the backend's log endpoints)
- `obs-internals`: Prometheus datasource at the platform Prometheus URL (the separate internals Prometheus; backend self-observability)

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
