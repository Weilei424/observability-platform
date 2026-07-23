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
- [x] Unit tests: compaction does not lose data (planner window/tier rules + multi-tier promotion across calls; `block.Compact` shared- and disjoint-series merge/dedup, generation-ordered last-write-wins, re-chunk seal boundaries by both 120-sample count and 2h span; `CompactOnce` query- and label-index-equivalence)
- [x] Unit tests: retention boundary behavior (exact cutoff, `retention=0` no-op, safe-delete leaves no partial dir, rename-failure keeps the block readable with an accurate count, post-rename cleanup failure is surfaced not swallowed)
- [x] Concurrency test: queries during `CompactOnce` and `ApplyRetention` never error (lock-drain) and never miss samples (a query under concurrent compaction always returns the full set)
- [x] Unit tests: flush triggers fire per-condition (interval, sealed-chunk threshold, WAL-bytes threshold); a no-op flush is not counted as a successful flush; flush/compaction/retention counters hold expected values after a known maintenance run
- [x] Unit tests: last-write-wins consistent across runtime/restart/compaction, including a partial compaction that leaves an overlapping newer-generation block out of the group; chunk generation round-trip; startup preserves source blocks when a compacted survivor is corrupt in its index OR chunks (and reclaims a corrupt source under a valid survivor)
- [x] Integration test: compacted data remains queryable, including across restart + startup GC convergence

### Phase 3.5 — Performance Benchmarks
Design: `docs/superpowers/specs/2026-06-29-phase-3.5-performance-benchmarks-design.md` · Plan: `docs/superpowers/plans/2026-06-29-phase-3.5-performance-benchmarks.md`

**Go benchmarks (in-process engine; `go test -bench`, deterministic)**
- [x] `internal/metrics/ingest_bench_test.go` — ingestion throughput: `MemoryStore.Append` encode-only (samples/sec via `b.ReportMetric`), `WALStore.Append` at `wal_sync_every_n=1` (durability cost), fsync-policy sweep {1,16,128}, compaction-on-vs-off during ingest (labeled approximate)
- [x] `internal/metrics/query_bench_test.go` — instant latency in-memory head vs persisted (flush + reopen, memory drained); range latency across step widths (~60/360/1440 ticks); instant vs block count {1,4,16}; driven through `QueryEngine.EvalInstant`/`EvalRange`; persisted bench `b.Fatal`s on empty match set
- [x] `internal/storage/chunk/compression_bench_test.go` — encode/decode throughput + bytes/sample ratio (monotonic counter, gauge random-walk, constant)
- [x] Reference existing `blockstore_bench_test.go` / `index_bench_test.go` / `reader_bench_test.go` (indexed vs full-scan select) results in the report — no duplication

**k6 HTTP load tests (end-to-end; real p50/p95/p99)**
- [x] `bench/k6/lib.js` — shared base URL, label scheme (query scripts select what `seed.js` seeds), payload builders (random + deterministic), cardinality knobs, `handleSummary()` → `bench/results/` (JSON + correctness `.status` marker)
- [x] `bench/k6/seed.js` — deterministic `shared-iterations` seed: `CARDINALITY` series × 1 sample at a fixed timestamp, so query scenarios run against a reproducible dataset
- [x] `bench/k6/ingest.js` — concurrent VUs POST batched samples to `/api/v1/ingest/metrics`; req/s, samples/s, p50/p95/p99; `thresholds`; random live-load throughput (runs after the seed, not the seeder); `timestamp_ms = Date.now()`
- [x] `bench/k6/instant_query.js` — instant-query p50/p95/p99 against seeded series; `check()` on every response
- [x] `bench/k6/range_query.js` — range-query p50/p95/p99 (1h window / 15s step); `check()` on every response
- [x] `bench/k6/README.md` — standalone k6 run instructions

**Orchestration & tooling**
- [x] `bench/run.sh` — resolve k6 (PATH then `$(go env GOPATH)/bin`, else print `go install go.k6.io/k6@latest` and exit non-zero), build server, start on a free ephemeral port + fresh temp data dir + wait `/readyz` (aborts if our PID died — no benchmarking a foreign backend), deterministic seed, run k6 query then ingest scenarios → JSON summaries, hard gate on correctness + latency thresholds (`BENCH_ALLOW_THRESHOLD_BREACH=1` to tolerate), trap-based teardown
- [x] Makefile targets: `bench-go`, `bench-k6`, `bench`
- [x] `.gitignore` += `bench/results/`

**Capture & report**
- [x] Install k6 via `go install go.k6.io/k6@latest` (fall back to documented k6 template in `PERFORMANCE.md` if the install can't reach the network; note the fallback)
- [x] Run Go benchmarks + k6 on this machine and capture real numbers
- [x] `PERFORMANCE.md` — overview, hardware/env (4 vCPU/~6 GB, WSL2, go1.26, date), methodology + layer split, reproduce commands, results tables with real numbers, interpretation, caveats
- [x] Link `PERFORMANCE.md` from `README.md`
- [x] `ARCHITECTURE_NOTES.md` — note the Go-bench-vs-k6 split and `bench/` layout under testing/observability

**Verify (Phase 3.5 DoD)**
- [x] `make bench-go` runs green and prints the custom samples/sec and bytes/sample metrics
- [x] `bench/run.sh` completes a short smoke profile and produces non-empty `bench/results/*.json`
- [x] `go build ./...` and `go test ./...` remain green (benchmarks excluded from the default `-run`)
- [x] Benchmark commands are reproducible locally and documented in `PERFORMANCE.md`

---

## Phase 4 Execution Checklist — Mini Loki-Style Logs Backend

### Phase 4.1 — Log Stream Data Model
Design: `docs/superpowers/specs/2026-07-18-phase-4.1-log-stream-data-model-design.md` · Plan: `docs/superpowers/plans/2026-07-18-phase-4.1-log-stream-data-model.md`

**Shared `internal/labels` package (ecosystem match — one labels type for metrics + logs)**
- [x] Create `internal/labels/labels.go` — `Label`, `Labels` (sorted pairs + cached `hash uint64`), `New` (generic validation, no `__name__` required), `NewUnvalidated`, `Hash`, `Get`, `Map`, `Len`; move FNV-1a length-prefixed `fingerprint` verbatim (preserves persisted `SeriesID`s)
- [x] Create `internal/labels/validation.go` — shared `ValidationError`, generic `validateLabelMap` (≤255 labels; name charset + `__` reserved except `__name__`; value UTF-8 + size limits)
- [x] Unit tests (`internal/labels/labels_test.go`): order-independent hash, different name/value/extra-label → different hash, generic validation cases, `__name__` accepted as ordinary label, `Get`/`Map`, **pinned golden hash** (migration guard)

**Refactor `internal/metrics` onto `internal/labels` (public API preserved)**
- [x] `model.go` — `type Labels = labels.Labels`, `type Label = labels.Label`, `type ValidationError = labels.ValidationError`; keep `SeriesID`, `Sample`
- [x] `labels.go` — `NewLabels` wraps `labels.New` after `validateMetricName`; `newOutputLabels` wraps `labels.NewUnvalidated`; remove moved `fingerprint`/methods
- [x] `validation.go` — `validateMetricName` (`__name__` present + `[a-zA-Z_:][a-zA-Z0-9_:]*` charset); keep `ValidateSample`; generic label validation moved to shared (`labelNameRe` retained — still used by `expr_parser.go`/`selector.go`)
- [x] Replace `.Fingerprint()` with `SeriesID(x.Hash())` (and `uint64(x.Fingerprint())` → `x.Hash()`) across `blockstore.go`, `eval.go`, `query.go`, `store.go` + affected tests (also added `sortedPairs` helper in `query.go` since `Labels.pairs` is now unexported in the shared package)
- [x] Verify: full existing metrics suite (`go test ./...`) stays green; keep pinned `SeriesID` golden `{__name__:"http_requests",service:"api"}` = `9696857623413696903`

**Logs model (`internal/logs`)**
- [x] Create `internal/logs/model.go` — `StreamID` (uint64), `type StreamLabels = labels.Labels`, `LogEntry{StreamID, TimestampNs int64, Line string}` (Loki-native nanoseconds)
- [x] Create `internal/logs/labels.go` — `NewStreamLabels` (generic rules + ≥1 label required), `StreamIDOf` (`StreamID(l.Hash())`)
- [x] Create `internal/logs/validation.go` — `MaxLineBytes = 256*1024`, `ValidateEntry` (`TimestampNs > 0`; line ≤ `MaxLineBytes`, empty accepted); document out-of-order policy (accepted, resolved at query time)
- [x] Unit tests (`internal/logs/model_test.go`): stream identity (order-independent same ID, different labels differ), empty `{}` rejected, timestamp `≤0` rejected / `>0` accepted, line at/over/at-empty size, typed `*ValidationError` on rejection

### Phase 4.2 — Loki-Compatible Push API
Design: `docs/superpowers/specs/2026-07-18-phase-4.2-loki-push-api-design.md` · Plan: `docs/superpowers/plans/2026-07-18-phase-4.2-loki-push-api.md`

**`internal/storage/logwal` package (dedicated log WAL — separate package from the metrics WAL; note: later crash-durability hardening did extend into shared/metrics WAL and filesystem code — see the design doc's "Post-Implementation Scope Note")**
- [x] `record.go` — `LabelPair`, `RecordWriter` interface, `encodeRecord`/`decodeRecord` (`[len][type=0x01][labelcount][labels][8B tsNs][4B lineLen][line]`), `validateLabels`, `maxRecordBodyBytes`
- [x] `logwal.go` — `LogWAL`: `Open`, `WriteRecord(labels, tsNs, line)`, `Sync`, `SegmentIndex`, `Close` (segment rotation at `segMaxBytes`, fsync-every-N, `broken`-state guard, `%06d.wal` naming — mirrors `wal.WAL`)
- [x] `replay.go` — `Replay(dir, fn)`: ascending segments, partial trailing record on last segment discarded, corrupt mid-segment record errors, oversized-length guard
- [x] Unit tests: record encode/decode round trip (empty line, max line, multi-byte UTF-8, truncated/trailing-byte rejection)
- [x] Unit tests: `LogWAL` write→reopen, rotation, fsync boundary, `Close`
- [x] Unit tests: replay restores order; partial/oversized trailing discarded; corrupt non-final record errors

**`internal/logs` store**
- [x] `store.go` — `Ingester` interface (`Append(StreamLabels, tsNs int64, line string) error`)
- [x] `store.go` — `MemoryStore` (per-stream `[]LogEntry` buffer, `Append`, `StreamEntries` copy, `StreamCount`), concurrency-safe
- [x] `store.go` — `WALStore` (WAL-write-before-buffer; `NewWALStore(w, store)`; `var _ Ingester`)
- [x] Unit tests: `MemoryStore` append/read, order-independent stream identity; `WALStore` writes WAL then buffers; WAL-failure leaves buffer empty (fake writer)

**`internal/api` push handler + wiring**
- [x] `loki_push.go` — `handleLokiPush` + `lokiPushRequest`/`lokiStream` DTOs; validate-all-first; 204 success, 400 error list, 500 on append failure; 4 MiB `MaxBytesReader`; reject protobuf content-type + 3-element `values` explicitly
- [x] `server.go` — add `logIngester logs.Ingester` field; extend `api.New(...)` signature; update all `api.New(` call sites (main.go, server_test.go, others via grep)
- [x] `router.go` — register `POST /loki/api/v1/push`
- [x] Unit tests: valid multi-stream push → 204 + entries reach ingester; empty streams / malformed JSON / empty `{}` labels / bad timestamp / oversize line / 3-element values / protobuf → 400

**`cmd/server/main.go` wiring**
- [x] Open `data/logs/wal`, replay into a `logs.MemoryStore`, open `logwal.LogWAL`, build `logs.WALStore`, pass to `api.New`, close logs WAL on shutdown (reuse `cfg.WALSegmentMaxBytes`/`WALSyncEveryN`, no new config)

**Integration + verify**
- [x] Integration test: push logs through router → entries buffered (query-ready storage)
- [x] Integration test: push → close WAL → fresh `MemoryStore` + replay from same dir → entries present (survives restart)
- [x] Verify: `go build ./...`, `go vet ./...`, `go test ./...` green

### Phase 4.3 — Log Chunk Storage and Index
Design: `docs/superpowers/specs/2026-07-21-phase-4.3-log-chunk-storage-index-design.md`

**`internal/storage/logchunk` package (compressed chunk format — dep-free, `compress/flate`)**
- [x] `logchunk.go` — `Chunk`: `Append(tsNs, line)`, `MinTs`/`MaxTs`/`NumEntries`/`UncompressedBytes`, `Iterator`
- [x] Entry block: first ts absolute (zigzag varint), rest zigzag-varint deltas (out-of-order tolerant), lines uvarint-len + bytes
- [x] `Bytes()` (on-disk **version 2**): `magic|version|minTs|maxTs|numEntries|uncompressedLen|compressedLen|headerCRC|payloadCRC|DEFLATE(entryblock)` — two CRC-32/Castagnoli: `headerCRC` over bytes `[0:33]` (bounds + counts, so a header-only read can authenticate them), `payloadCRC` over the compressed block
- [x] `FromBytes()`: validate magic/version, verify `headerCRC`, verify `payloadCRC` (before decompressing), decompress, decode exactly `numEntries`, verify header min/max, reject trailing bytes
- [x] Unit tests: round trip (empty/single/many/out-of-order/multibyte-UTF8/large line); compression shrinks repetitive block; corruption/truncation/min-max-mismatch rejected

**`internal/logs` chunk file + stream index**
- [x] `chunkfile.go` — `ChunkRef`; file = `header{magic,version,streamID,labels}` + `logchunk.Bytes()`; `writeChunkFile` (tmp→fsync→rename→dir fsync), `readChunkFileHeader`, `readChunkFile`; name `<streamIDhex>-<minTsNs>-<rand4>.chunk`
- [x] `streamindex.go` — `streamIndex{postings *index.MemPostings, refs map[StreamID][]ChunkRef, labels map[StreamID]StreamLabels}`; `add`, `matchingStreamIDs`, `chunkRefs(id,minTs,maxTs)` overlap filter
- [x] `streamindex.go` — `streams.index` manifest write (atomic) + `loadManifest`; `rebuildFromScan(chunksDir)` from chunk headers (self-heal)
- [x] Unit tests: `chunkfile` write/read round trip, header-only read, no temp left; `streamIndex` label filter narrows + time filter; manifest round trip; rebuild-from-scan == manifest load; corrupt manifest → rebuild

**`internal/storage/logwal` checkpoint**
- [x] `logwal.go` — `Checkpoint()`: sync+close current, delete all `.wal` segments, open fresh, fsync dir (under `w.mu`)
- [x] Unit test: `Checkpoint()` drops flushed segments; replay after checkpoint returns only post-checkpoint records

**`internal/logs` production `Store`**
- [x] `store.go` — `Store` composing head (`MemoryStore`) + WAL + `streamIndex` + chunks dir; implements `Ingester`
- [x] `Append`: WAL-write → head buffer → `headBytes += encodedSize`; flush at `LogsFlushThresholdBytes`
- [x] `flush()` (under `mu`): per stream build `logchunk.Chunk` → `writeChunkFile` → `index.add`; write manifest; `wal.Checkpoint()`; reset head
- [x] `Close()`: flush (drain head) + close WAL; `NewStore`: load manifest (or rebuild-from-scan) + WAL replay into head
- [x] Read surface: `MatchingStreamIDs(matchers)`, `StreamEntries(id,minTs,maxTs)` merged head+chunks deduped by `(streamID,tsNs,line)`
- [x] Integration tests: threshold flush → chunk files + manifest exist; append→flush→Close→new Store→entries present (restart); checkpoint drops flushed segments; crash-window (chunk written, WAL not checkpointed) → no duplicates; label filter narrows

**Config + wiring**
- [x] `internal/config` — add `LogsFlushThresholdBytes` (default 8 MiB); reject `<= 0`
- [x] `cmd/server/main.go` — wire `logs.Store` over `data/logs/{wal,chunks,index}`; `Store.Close()` on shutdown
- [x] `ARCHITECTURE_NOTES.md` — "Introduced in 4.3" note for `logchunk`, `streams.index` manifest, flush/checkpoint model
- [x] Verify: `go build ./...`, `go vet ./...`, `golangci-lint run`, `go test ./...` green

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
