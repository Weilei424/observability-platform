# Backlog

## Status Legend
- [ ] Not started
- [x] Complete
- [~] In progress

---

## Phase 0 Execution Checklist — Repository Foundation and Local Runtime

### Phase 0.1 — Repository Layout and Planning Docs
- [x] Create repository layout from `CLAUDE.md`
- [x] Add `docs/planning/BACKLOG.md`
- [x] Add `docs/planning/IMPLEMENTATION_PLAN.md`
- [x] Add `docs/planning/ARCHITECTURE_NOTES.md`
- [x] Add top-level `CLAUDE.md`
- [x] Add top-level `AGENTS.md`
- [x] Add initial `README.md` with project goal and architecture summary
- [x] Verify docs use `phase` consistently for implementation sequencing
- [x] Verify no implementation checklist item is marked complete before code exists

### Phase 0.2 — Go Service Skeleton
- [x] Initialize Go module
- [x] Add `cmd/server/main.go`
- [x] Add config package (`internal/config`)
- [x] Add structured logging package (`internal/observability/logger.go`)
- [x] Add request ID middleware
- [x] Add HTTP router package (`internal/api/router.go`)
- [x] Add `GET /healthz`
- [x] Add `GET /readyz`
- [x] Add writable data directory readiness check
- [x] Unit test config loading
- [x] Unit test health/readiness handlers
- [x] Verify: `go build ./...` passes
- [x] Verify: `go test ./...` passes

### Phase 0.3 — Local Runtime and Tooling
- [x] Add backend Dockerfile
- [x] Add `docker-compose.yml` with backend and Grafana placeholders
- [x] Add Makefile target: `build`
- [x] Add Makefile target: `test`
- [x] Add Makefile target: `lint`
- [x] Add Makefile target: `run`
- [x] Add Makefile target: `local-up`
- [x] Add Makefile target: `local-down`
- [x] Add GitHub Actions workflow for build/test/lint
- [x] Verify: `make run` starts the backend
- [x] Verify: `make local-up` starts backend + Grafana containers

---

## Phase 1 Execution Checklist — Single-Node Metrics TSDB

### Phase 1.1 — Metrics Data Model
- [x] Create `internal/metrics/model.go` — `SeriesID` (named uint64), `Label`, `Labels` (opaque with cached fingerprint), `Sample`
- [x] Create `internal/metrics/labels.go` — `NewLabels` constructor, FNV-1a fingerprinting, `Get`, `Map`
- [x] Create `internal/metrics/validation.go` — `ValidationError`, `validateLabelMap`, `ValidateSample`
- [x] Metric name stored as `__name__` label (Prometheus convention); required on every `Labels` value
- [x] Label normalization: sort pairs by name on construction; fingerprint is computed once and cached
- [x] Fingerprinting: FNV-1a 64-bit, length-prefixed binary encoding (8-byte big-endian length + field bytes, per name/value pair)
- [x] Validate `__name__` value: `[a-zA-Z_:][a-zA-Z0-9_:]*`
- [x] Validate label names: `[a-zA-Z_][a-zA-Z0-9_]*`; `__` prefix reserved (only `__name__` permitted)
- [x] Validate label values: must be valid UTF-8 (checked via `utf8.ValidString`); empty string accepted
- [x] `ValidateSample` accepts all float64 (NaN, ±Inf) and all int64 timestamps
- [x] Unit tests: same labels in different map order → same `SeriesID`
- [x] Unit tests: different values / names / extra label → different `SeriesID`
- [x] Unit tests: missing `__name__`, invalid metric name, invalid label name, reserved prefix → typed `ValidationError`
- [x] Unit tests: empty label value, NaN, ±Inf, valid timestamp → accepted
- [x] Unit tests: `Labels.Get` binary search, `Labels.Map` returns copy

### Phase 1.2 — Metrics Ingestion API
- [x] Add metrics ingestion handler: `POST /api/v1/ingest/metrics`
- [x] Add request DTO and validation errors
- [x] Add in-memory series registry
- [x] Add in-memory sample append path
- [x] Define duplicate sample behavior
- [x] Define out-of-order sample behavior
- [x] Integration test: ingest valid sample
- [x] Integration test: reject invalid sample
- [x] Integration test: repeated writes append to same series

### Phase 1.3 — In-Memory Query Engine
- [x] Add selector parser for `metric_name{label="value"}`
- [x] Add equality label matcher support
- [x] Add instant query execution over in-memory samples
- [x] Add range query execution over in-memory samples
- [x] Wire `GET /api/v1/query`
- [x] Wire `GET /api/v1/query_range`
- [x] Unit tests: selector parser
- [x] Unit tests: label matcher behavior
- [x] Integration test: ingest → instant query
- [x] Integration test: ingest → range query

### Phase 1.4 — WAL Durability
- [x] Design WAL record format for metric samples
- [x] Implement WAL segment writer
- [x] Implement WAL segment reader
- [x] Write WAL record before acknowledging ingestion
- [x] Implement WAL replay on startup
- [x] Handle partial trailing WAL records safely
- [x] Add fsync policy configuration
- [x] Unit tests: WAL encode/decode round trip
- [x] Unit tests: WAL replay restores series/samples
- [x] Integration test: ingest → restart → query

### Phase 1.5 — Phase 1 End-to-End Metrics Path
- [x] Add sample metrics load generator
- [x] Add E2E test for ingest/query/WAL restart path (in-process via httptest; real-process smoke script deferred)
- [x] Add README section for local metrics demo
- [x] Verify: backend can ingest metrics and query them before restart
- [x] Verify: backend can query metrics after restart
- [x] Verify: Phase 1 DoD is reflected in `BACKLOG.md`

---

## Phase 2 Execution Checklist — Grafana-Compatible Metrics API

### Phase 2.1 — Prometheus Response Envelope
- [x] Implement Prometheus-compatible success response envelope
- [x] Implement Prometheus-compatible error response envelope
- [x] Format matrix/vector/scalar response values correctly
- [x] Unit tests: instant vector response serialization
- [x] Unit tests: range matrix response serialization
- [x] Unit tests: error response serialization

### Phase 2.2 — Prometheus Instant and Range Query Endpoints
- [x] Add POST support for `GET /api/v1/query` and `GET /api/v1/query_range` (register both GET and POST; use `r.Form` after `r.ParseForm()` in handlers)
- [x] Confirm `GET /api/v1/query` supports all Prometheus-compatible query params (`query`, `time`)
- [x] Confirm `GET /api/v1/query_range` supports all Prometheus-compatible query params (`query`, `start`, `end`, `step`)
- [x] Add parameter validation for invalid time ranges and step values (step=0, end<start, NaN/±Inf — completed in Phase 1)
- [x] Integration test: instant query response shape — assert full Prometheus wire format (envelope fields, float-seconds timestamps, string values)
- [x] Integration test: range query response shape — assert full Prometheus wire format (matrix envelope, values as `[float64, string]` pairs)
- [x] Verify Grafana can issue query requests to the backend — `TestGrafanaStylePOSTQuery` exercises POST with `application/x-www-form-urlencoded` body

### Phase 2.3 — Prometheus Metadata Endpoints
- [ ] Implement `GET /api/v1/labels`
- [ ] Implement `GET /api/v1/label/{name}/values`
- [ ] Implement `GET /api/v1/label/__name__/values`
- [ ] Implement `GET /api/v1/series`
- [ ] Integration test: list metric names
- [ ] Integration test: list label names
- [ ] Integration test: list label values
- [ ] Integration test: series discovery with match selector

### Phase 2.4 — Minimal Query Functions
- [ ] Support raw selector query
- [ ] Support `rate(metric[window])`
- [ ] Support `sum(metric)`
- [ ] Support `sum by (label)(metric)`
- [ ] Return explicit error for unsupported functions
- [ ] Unit tests: rate over counter samples
- [ ] Unit tests: sum across series
- [ ] Unit tests: grouped sum by label
- [ ] Unit tests: unsupported function error

### Phase 2.5 — Grafana Metrics Dashboard Demo
- [ ] Add Grafana datasource provisioning for Prometheus-compatible endpoint
- [ ] Add metrics dashboard JSON
- [ ] Add sample app or load generator metrics
- [ ] Add Docker Compose wiring for dashboard provisioning
- [ ] Verify: Grafana datasource connects successfully
- [ ] Verify: dashboard displays live metrics
- [ ] Add docs/screenshots placeholder path for demo evidence

---

## Phase 3 Execution Checklist — Metrics Storage Engine Improvements

### Phase 3.1 — Chunked Sample Storage
- [ ] Define metric chunk format
- [ ] Implement chunk append behavior
- [ ] Implement chunk encoding/decoding
- [ ] Add Snappy or Zstd compression
- [ ] Track min/max timestamp per chunk
- [ ] Unit tests: chunk boundary behavior
- [ ] Unit tests: compression round trip
- [ ] Unit tests: query samples from chunk

### Phase 3.2 — Immutable Time Blocks
- [ ] Define block directory layout
- [ ] Define `meta.json` schema
- [ ] Implement block writer
- [ ] Implement block reader
- [ ] Implement atomic block write with temp directory + rename
- [ ] Flush in-memory chunks into blocks
- [ ] Query from persisted blocks
- [ ] Integration test: ingest → flush → restart → query persisted block

### Phase 3.3 — Label Index
- [ ] Implement metric name → series IDs index
- [ ] Implement label name → label values index
- [ ] Implement label pair → series IDs index
- [ ] Implement series ID → chunk references index
- [ ] Persist index in block storage
- [ ] Use index in query planner
- [ ] Unit tests: index build/load
- [ ] Integration test: indexed label query
- [ ] Benchmark: indexed lookup vs full scan

### Phase 3.4 — Compaction and Retention
- [ ] Implement block compactor
- [ ] Merge adjacent compatible blocks
- [ ] Preserve index correctness after compaction
- [ ] Implement retention cleanup by time window
- [ ] Add safe deletion behavior
- [ ] Emit compaction metrics
- [ ] Unit tests: compaction does not lose data
- [ ] Unit tests: retention boundary behavior
- [ ] Integration test: compacted data remains queryable

### Phase 3.5 — Performance Benchmarks
- [ ] Add k6 or Go benchmark for metrics ingestion throughput
- [ ] Add benchmark for instant query latency
- [ ] Add benchmark for range query latency
- [ ] Track p50/p95/p99 latency
- [ ] Track samples/sec ingestion throughput
- [ ] Add `PERFORMANCE.md`
- [ ] Link `PERFORMANCE.md` from README
- [ ] Verify benchmark commands are reproducible locally

---

## Phase 4 Execution Checklist — Mini Loki-Style Logs Backend

### Phase 4.1 — Log Stream Data Model
- [ ] Define `StreamID`, `StreamLabels`, `LogEntry`
- [ ] Implement deterministic stream fingerprinting
- [ ] Validate stream labels
- [ ] Validate log timestamps
- [ ] Validate log line size
- [ ] Define out-of-order log behavior
- [ ] Unit tests: stream identity
- [ ] Unit tests: invalid logs rejected

### Phase 4.2 — Loki-Compatible Push API
- [ ] Implement `POST /loki/api/v1/push`
- [ ] Parse Loki-style `streams` payload
- [ ] Write log records to WAL before acknowledgment
- [ ] Buffer logs into per-stream chunks
- [ ] Unit tests: push payload parsing
- [ ] Integration test: push logs successfully
- [ ] Integration test: logs survive restart through WAL replay

### Phase 4.3 — Log Chunk Storage and Index
- [ ] Define log chunk format
- [ ] Implement log chunk encoding/decoding
- [ ] Add compression for log chunks
- [ ] Persist stream ID → chunk references index
- [ ] Persist label pair → stream IDs index
- [ ] Track min/max timestamp per chunk
- [ ] Unit tests: log chunk round trip
- [ ] Integration test: label index narrows candidate streams

### Phase 4.4 — Loki-Compatible Query API
- [ ] Implement `GET /loki/api/v1/query`
- [ ] Implement `GET /loki/api/v1/query_range`
- [ ] Implement `GET /loki/api/v1/labels`
- [ ] Implement `GET /loki/api/v1/label/{name}/values`
- [ ] Support selector query `{service="api"}`
- [ ] Support text filter `|= "text"`
- [ ] Return explicit error for unsupported LogQL features
- [ ] Integration test: label-only query
- [ ] Integration test: time-range query
- [ ] Integration test: text filter query

### Phase 4.5 — Grafana Logs Demo
- [ ] Add Grafana datasource provisioning for Loki-compatible endpoint
- [ ] Add sample app log generator
- [ ] Add docs for Grafana Explore workflow
- [ ] Verify: Grafana Loki datasource connects
- [ ] Verify: logs appear in Grafana Explore
- [ ] Verify: user can filter logs by service/level and search text

---

## Phase 5 Execution Checklist — Packaging, Kubernetes, and Operational Demo

### Phase 5.1 — Docker Compose Demo
- [ ] Backend container runs from local image
- [ ] Grafana container starts with provisioned datasources
- [ ] Sample app container emits metrics and logs
- [ ] Load generator container produces repeatable traffic
- [ ] `make local-up` starts complete demo
- [ ] `make local-down` cleans up demo containers
- [ ] Verify: dashboards populate after startup

### Phase 5.2 — Kubernetes Manifests and Helm Chart
- [ ] Add Helm chart for backend
- [ ] Add Kubernetes manifests for Grafana demo
- [ ] Add PersistentVolumeClaim support
- [ ] Add ConfigMap support
- [ ] Add Secret support
- [ ] Add backend Service
- [ ] Add Grafana Service
- [ ] Verify: Helm install deploys backend
- [ ] Verify: data persists across pod restart
- [ ] Verify: Grafana queries backend inside Kubernetes

### Phase 5.3 — Platform Self-Observability
- [ ] Add `/metrics` endpoint for backend internals
- [ ] Emit ingestion rate metrics
- [ ] Emit query latency metrics
- [ ] Emit WAL size metrics
- [ ] Emit block count metrics
- [ ] Emit compaction duration metrics
- [ ] Emit log chunk count metrics
- [ ] Emit error count metrics
- [ ] Add Grafana dashboard for backend internals
- [ ] Verify: platform dashboard shows ingest/query/storage health

### Phase 5.4 — Documentation and Demo Runbook
- [ ] Add architecture diagram for metrics path
- [ ] Add architecture diagram for logs path
- [ ] Add architecture diagram for query path
- [ ] Add storage layout documentation
- [ ] Add local demo runbook
- [ ] Add Kubernetes deployment runbook
- [ ] Add API reference docs
- [ ] Add limitations section for unsupported PromQL/LogQL
- [ ] Verify: fresh reviewer can run demo from README

---

## Phase 6 Execution Checklist — Distributed Mode

### Phase 6.1 — Component Split
- [ ] Add `all-in-one` mode
- [ ] Add `gateway` mode
- [ ] Add `ingester` mode
- [ ] Add `querier` mode
- [ ] Add `store` mode
- [ ] Add `compactor` mode
- [ ] Refactor component wiring behind interfaces
- [ ] Verify: all existing single-node tests pass in `all-in-one` mode
- [ ] Verify: each component mode starts independently

### Phase 6.2 — Ring-Based Sharding
- [ ] Implement ring assignment for series IDs
- [ ] Implement ring assignment for stream IDs
- [ ] Add ingester membership configuration
- [ ] Route metric writes through ring
- [ ] Route log writes through ring
- [ ] Unit tests: stable placement
- [ ] Unit tests: membership change remaps partial keyspace

### Phase 6.3 — Replication and Failure Handling
- [ ] Add configurable replication factor
- [ ] Write each series/stream record to N ingesters
- [ ] Define quorum behavior
- [ ] Surface partial write failures clearly
- [ ] Deduplicate replicated samples/log lines
- [ ] Failure test: one ingester unavailable but quorum succeeds
- [ ] Failure test: quorum unavailable causes write failure

### Phase 6.4 — Query Fanout and Merge
- [ ] Implement metrics query fanout
- [ ] Implement logs query fanout
- [ ] Merge metrics by series/time
- [ ] Merge logs by timestamp
- [ ] Deduplicate replicated query results
- [ ] Integration test: multi-ingester metrics ingest → query
- [ ] Integration test: multi-ingester logs ingest → query

### Phase 6.5 — Multi-Tenant Boundaries
- [ ] Read tenant ID from request header
- [ ] Add tenant-aware metrics series identity
- [ ] Add tenant-aware log stream identity
- [ ] Add tenant-aware query filtering
- [ ] Add tenant-aware retention configuration
- [ ] Add per-tenant active series limit
- [ ] Add per-tenant active stream limit
- [ ] Test: Tenant A cannot query Tenant B metrics
- [ ] Test: Tenant A cannot query Tenant B logs
