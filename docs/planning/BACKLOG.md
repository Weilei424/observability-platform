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
- [x] Implement `GET /api/v1/labels` (also POST; `handleLabels` in `internal/api/metadata.go`)
- [x] Implement `GET /api/v1/label/{name}/values` (also POST; `handleLabelValues`)
- [x] Implement `GET /api/v1/label/__name__/values` (covered by `{name}` wildcard route)
- [x] Implement `GET /api/v1/series` (also POST; `handleSeries` with match[] dedup)
- [x] Integration test: list metric names (`TestMetadata_LabelValues_ReturnsMetricNames`)
- [x] Integration test: list label names (`TestMetadata_Labels_ReturnsSortedLabelNames`)
- [x] Integration test: list label values (`TestMetadata_LabelValues_ExistingLabel_ReturnsSortedValues`)
- [x] Integration test: series discovery with match selector (`TestMetadata_Series_ReturnsMatchingSeriesLabelSets`)

### Phase 2.4 — Minimal Query Functions
- [x] Create `internal/metrics/duration.go` — export `ParsePromDuration` (move from `internal/api/query.go`)
- [x] Update `internal/api/query.go` — `parseDurationParam` calls `metrics.ParsePromDuration`; remove local `promDurationUnit` and `parsePromDuration`
- [x] Create `internal/metrics/expr.go` — `Expr` interface, `SelectorExpr`, `RateExpr`, `SumExpr` node types
- [x] Add `newOutputLabels` to `internal/metrics/labels.go` — construct aggregation output labels without requiring `__name__`
- [x] Create `internal/metrics/expr_parser.go` — `ParseExpr` with bracket-matching recursive descent; `parseRateExpr`, `parseSumExpr`, `parseLabelList`, `extractFirstParen`
- [x] Create `internal/metrics/expr_parser_test.go` — unit tests for `ParseExpr`: bare selector, rate, sum, sum-by single label, sum-by multiple labels, sum(rate(...)), unknown function, malformed input
- [x] Create `internal/metrics/eval.go` — `EvalInstant` and `EvalRange` on `QueryEngine`; `rateInstant`, `rateRange`, `aggregateInstant`, `aggregateRange`, `groupKey`, `sortPoints`
- [x] Create `internal/metrics/eval_test.go` — unit tests for rate (≥2 samples, <2 samples, per-tick re-evaluation), sum (ungrouped, grouped by single label, grouped by multiple labels), sum(rate(...)) composition
- [x] Modify `internal/api/query.go` — `handleQuery` and `handleQueryRange` use `metrics.ParseExpr` / `engine.EvalInstant` / `engine.EvalRange` instead of `ParseSelector` / `InstantQuery` / `RangeQuery`
- [x] Modify `internal/api/query_test.go` — replace stale `TestQuery_PromQLFunctionCall_Returns400` with empty-vector test; add HTTP integration tests for rate instant, rate range, sum-by range, unknown function → 400

### Phase 2.5 — Grafana Metrics Dashboard Demo
- [x] Create `tests/e2e/smoke.sh` — API smoke test for all 5 dashboard queries
- [x] Enrich `examples/load-generator/main.go` — add `http_errors_total`, `active_connections`, method label on duration, `OBS_BACKEND_ADDR` env var
- [x] Add `loadgen` build target to `deployments/docker/Dockerfile`
- [x] Add `load-generator` service to `deployments/docker/docker-compose.yml`
- [x] Create `observability/grafana/datasources/prometheus.yml` — provision Prometheus datasource (uid: obs-prometheus)
- [x] Create `observability/grafana/dashboards/dashboards.yml` — dashboard provider config
- [x] Create `observability/grafana/dashboards/metrics.json` — 5-panel dashboard (Request Rate, Error Rate, Total RPS, Duration, Active Connections)
- [x] Add `smoke` target to `Makefile`
- [x] Create `docs/runbooks/grafana-demo.md` — manual test steps
- [x] Verify: `make local-up` starts all three services
- [x] Verify: Grafana datasource "Save & test" returns success
- [x] Verify: all 5 dashboard panels show live data
- [x] Verify: `make smoke` exits 0

---

## Phase 3 Execution Checklist — Metrics Storage Engine Improvements

### Phase 3.1 — Chunked Sample Storage
- [x] Define metric chunk format (`internal/storage/chunk/chunk.go` — Gorilla/XOR encoding)
- [x] Implement chunk append behavior (seal at 120 samples or 2-hour span)
- [x] Implement chunk encoding/decoding (delta-of-delta timestamps + XOR values, pure Go)
- [x] Track min/max timestamp per chunk
- [x] Replace flat `[]Sample` in MemoryStore with `[]*chunk.Chunk` per series
- [x] Unit tests: chunk boundary behavior (seal-by-count, seal-by-time)
- [x] Unit tests: compression round trip (varied values, constant, monotonic, NaN/Inf, irregular)
- [x] Unit tests: query samples from chunk (cross-chunk QueryRange, QueryInstant, duplicate-ts across boundary)
- [x] Add `Bytes()` / `FromBytes()` serialization API with eager decode validation (Phase 3.2 persistence contract)

### Phase 3.2 — Immutable Time Blocks

**`internal/storage/block/` package**
- [x] Add `Meta` struct and JSON marshal/unmarshal (`internal/storage/block/meta.go`)
- [x] Add `LabelPair`, `SeriesEntry`, `ChunkRef` types (`internal/storage/block/reader.go`)
- [x] Add `Writer` with `AddSeries`, `Commit` (atomic temp-dir + rename), `Abort` (`internal/storage/block/writer.go`)
- [x] Add `Reader` with `OpenReader`, `Series`, `ReadChunk` (lazy `ReadAt`), `Close` (`internal/storage/block/reader.go`)
- [x] Unit test: Writer/Reader round-trip (2 series, 3 chunks each — meta, index, chunks files valid)
- [x] Unit test: `Abort` removes temp dir
- [x] Unit test: `Commit` atomic rename — block not visible in `blocks/` until rename completes
- [x] Unit test: `OpenReader` returns error on missing `meta.json`
- [x] Unit test: `ReadChunk` returns error on corrupt payload (propagated from `chunk.FromBytes`)

**`internal/metrics/` integration**
- [x] Add `BlockStore` wrapping `*MemoryStore` + `[]*block.Reader` (`internal/metrics/blockstore.go`)
- [x] `BlockStore.FlushBlock`: snapshot sealed chunks (under read lock), no-op if none, write block outside lock, drain memory and register reader (under write lock), abort on failure
- [x] `BlockStore.QueryRange` / `QueryInstant` / `SelectSeries` fan out to memory + all block readers; deduplicate by timestamp (memory wins)
- [x] Update `WalStore` to wrap `*BlockStore` instead of `*MemoryStore`; update `NewWalStore` signature
- [x] Add `WalStore.FlushBlock`: record current WAL segment, call `BlockStore.FlushBlock`, write `checkpoint` file, delete WAL segments ≤ checkpointed segment
- [x] Implement checkpoint file read/write (`data/metrics/checkpoint` — decimal WAL segment number)
- [x] Update startup sequence: load blocks → read checkpoint (default 0) → replay WAL segments with number > checkpoint
- [x] Clean up orphaned directories in `data/metrics/tmp/` on startup
- [x] Unit test: `FlushBlock` drains sealed chunks; `MemoryStore` retains only head chunk; block reader registered
- [x] Unit test: `QueryRange` returns samples from both block and memory across full time range
- [x] Unit test: duplicate timestamps across block and memory are deduplicated in query result
- [x] Unit test: `SelectSeries` includes series from persisted block
- [x] Integration test: ingest → `WalStore.FlushBlock` → new `WalStore` from same dataDir → `QueryRange` returns all flushed samples (`TestBlockPersistence_IngestFlushRestartQuery`)

### Phase 3.3 — Label Index
Design: `docs/superpowers/specs/2026-06-18-phase-3.3-label-index-design.md` · Plan: `docs/superpowers/plans/2026-06-18-phase-3.3-label-index.md`
- [x] Create `internal/storage/index` package — `MemPostings` (sorted postings, `Add`, `Postings`, intersection-based `Select`) covering metric name → series IDs and label pair → series IDs
- [x] Extend `MemPostings` — `Delete`, `LabelNames`/`LabelValues` (label name → values), cardinality accessors (`SeriesCount`/`LabelNameCount`/`LabelPairCount`)
- [x] Integrate index into `MemoryStore` — index series on first append; back `SelectSeries` with `Select`; add `LabelNames`/`LabelValues`/`Cardinality`
- [x] Persist per-block postings — new `postings` file (magic+version, postings lists + **offset table** + footer) written in `block.Writer.Commit`; `block.Reader` seeks individual lists via `ReadAt` (allRefs sentinel for empty matchers), with in-memory rebuild fallback for pre-existing blocks; add `Reader.Postings`/`LabelNames`/`LabelValues` (series ID → chunk refs stays in the existing forward index)
- [x] Use index in query planner — `BlockStore.SelectSeries`/`LabelNames`/`LabelValues` via head index + block postings; `BlockStore.Cardinality` snapshot; `QueryEngine` metadata delegates to store (extend `queryStore`; add `WALStore` delegation)
- [x] Add Prometheus `/metrics` endpoint — `prometheus/client_golang` dep; `internal/observability/metrics.go` registry + pull-model cardinality collector (`obs_active_series`, `obs_label_names_total`, `obs_label_pairs_total`); wire `Server.New`, router, `cmd/server/main.go`
- [x] Unit tests: index build/load (`index` package, block postings round-trip + rebuild fallback)
- [x] Integration test: indexed label query (ingest → indexed `SelectSeries`/metadata; ingest → flush → restart → indexed query; `/metrics` scrape)
- [x] Benchmark: indexed lookup vs full scan (`internal/metrics/index_bench_test.go`) + index/scan agreement guard test
- [x] Metadata filtering (deferred from Phase 2.3): `metrics.MetadataFilter` adds `match[]` + time-range filtering to `QueryEngine.LabelNames`/`LabelValues`/`Series`; handlers build the filter in `internal/api/metadata.go`

### Phase 3.4 — Compaction and Retention
Design: `docs/superpowers/specs/2026-06-25-phase-3.4-compaction-retention-design.md` · Plan: `docs/superpowers/plans/2026-06-25-phase-3.4-compaction-retention.md`
- [x] Extend `block.Meta` with `Level` + `Sources` (`EffectiveLevel`, `BlockInfo`, exported `ReadMeta`); `Writer.SetCompaction` writes them (flush blocks are level 1)
- [x] Add `block.Compact(blocksDir, tmpDir, sources)` pure merge primitive — union series, sort+dedup samples (highest per-sample generation wins), re-chunk (120/2h) preserving generations, regenerate index+postings via `Writer`
- [x] Per-sample write generations for exact last-write-wins: `MemoryStore` assigns a monotonic generation per appended sample; chunks store generations behind a multi-byte magic/version header (any non-matching, pre-generation chunk is rejected with a clear error — a one-time storage-format break, no silent misread); generation decoding and `Append` are range-checked and bounded below `MaxInt64`, and the ingest path fails explicitly on generation exhaustion rather than silently rejecting writes; the startup counter is reconstructed from the generations actually stored in every loaded block's chunks (never trusted from a possibly-corrupt `Meta.MaxGen`), while a compaction survivor additionally requires its `Meta.MaxGen` to agree with those generations before its `Sources` are trusted for deletion; memory, cross-block queries (`QueryRange`/`QueryInstant`), and `block.Compact` all dedup equal timestamps by highest generation — correct even when compaction merges a partial group that leaves an overlapping, intermediate-generation block behind
- [x] Add flush-threshold accessors — `wal.DirSize`/`WALStore.WALBytes`, `MemoryStore.SealedChunkCount`/`BlockStore.SealedChunkCount`
- [x] Add `BlockStore.BlockInfos` + `StorageStats` (block count + on-disk bytes)
- [x] Hold `BlockStore` read lock across block reads so compaction/retention can safely close+reclaim readers; add `CompactOnce`, `readerByID`, crash-safe `safeDeleteBlock` (rename-to-tmp + RemoveAll)
- [x] Add `BlockStore.ApplyRetention` (whole-block, `MaxTime < now-retention`); startup GC of superseded compaction sources in `NewBlockStore`
- [x] Add config — `maintenance_interval`, `flush_interval`, `flush_sealed_chunks`, `flush_wal_bytes`, `compaction_base_range`, `compaction_multiplier`, `compaction_levels`, `retention` (default 0 = disabled) with validation
- [x] Refactor `observability.NewRegistry` → `(card, storage) (*Registry, *Metrics)`; add pull gauges `obs_blocks_total`/`obs_blocks_bytes` and push instruments (compactions, compaction duration, failures, retention deletions, flushes)
- [x] Add `internal/compactor` tiered time-aligned planner (`Ranges`, `Plan`) — merge ≥2 aligned blocks below the tier range, smallest tier first
- [x] Add `internal/compactor` maintenance scheduler (`RunOnce`/`Run`: flush-if-due → compact-to-stable → retention) with metrics
- [x] Wire graceful lifecycle in `cmd/server/main.go` — signal context, `http.Server.Shutdown`, background compactor goroutine, final flush, close WAL + block readers
- [x] Unit tests: compaction does not lose data (planner; `block.Compact` merge/dedup, generation-ordered last-write-wins, re-chunk seal boundaries; `CompactOnce` query- and label-index-equivalence)
- [x] Unit tests: retention boundary behavior (exact cutoff, `retention=0` no-op, safe-delete leaves no partial dir, rename-failure keeps the block readable with an accurate count, post-rename cleanup failure is surfaced not swallowed)
- [x] Concurrency test: queries during `CompactOnce` and `ApplyRetention` never error (lock-drain)
- [x] Unit tests: flush triggers fire per-condition (interval, sealed-chunk threshold, WAL-bytes threshold)
- [x] Unit tests: last-write-wins consistent across runtime/restart/compaction, including a partial compaction that leaves an overlapping newer-generation block out of the group; chunk generation round-trip; startup preserves source blocks when a compacted survivor is corrupt in its index OR chunks (and reclaims a corrupt source under a valid survivor)
- [x] Integration test: compacted data remains queryable, including across restart + startup GC convergence

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
