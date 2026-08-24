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
Design: `docs/superpowers/specs/2026-07-25-phase-4.4-loki-query-api-design.md` · Plan: `docs/superpowers/plans/2026-07-26-phase-4.4-loki-query-api.md`

**`internal/logs` LogQL parser + query engine**
- [x] `logql.go` — `LogSelector{Matchers []index.Pair, LineFilters []LineFilter}`, `FilterOp` (`|=`/`!=`/`|~`/`!~`), `LineFilter.Keep`; `ParseLogQL` (equality-only selector, ≥1 matcher required, quote-aware line-filter tokenizing, regex compiled once)
- [x] `logql.go` — explicit errors for `{}`/empty selector, non-`=` label matcher ops (`=~`/`!=`/`!~`), pipelines/formatters (`| json`, `| logfmt`, `line_format`, `label_format`), metric wrappers (`rate(`, `count_over_time(`), trailing junk, and invalid regex operands
- [x] `scalar.go` — **constant metric queries** for the Grafana Loki datasource health check: `ParseScalarQuery` evaluates `vector(<number>)` combined with `+ - * /`, parens, and unary sign (design §11), returning `ScalarResult{Value, HasVector}` so the handler can pick the upstream result type (LiteralExpr → scalar, VectorExpr → vector). `vector()` takes a bare number, matching upstream LogQL's `vector OPEN_PARENTHESIS NUMBER CLOSE_PARENTHESIS` — no nested/arithmetic operand. Reads no stored data; every other function name still returns the explicit unsupported error
- [x] `query.go` — `Reader` interface (`MatchingStreamIDs`, `StreamEntries`, `StreamLabelSet`, `LabelNames`, `LabelValues`); `QueryEngine`, `Direction` (backward/forward), `StreamResult`
- [x] `query.go` — `QueryRange` (match → read → line-filter → per-stream cap → global order-by-direction + limit → regroup by stream, drop empty streams), half-open `[start, end)` per Loki; `QueryInstant` shares the path with an **inclusive** end (`ts <= time`); `ctx` threaded for cancellation; `LabelNames`/`LabelValues` delegation
- [x] `diskstore.go` — `StreamLabelSet(id)`, `LabelNames()`, `LabelValues(name)` merging head + persisted index (sorted-unique); `var _ Reader = (*Store)(nil)`

**`internal/api` Loki endpoints + envelope**
- [x] `loki_response.go` — `lokiResponse`/`lokiData`/`lokiStreamResult` (`resultType:"streams"`, values `["<tsNs>","<line>"]`, `stats:{}`); success writer; **plain-text** error writer (Loki-faithful, distinct from the Prometheus JSON envelope)
- [x] `loki_response.go` — `resultType:"vector"` envelope + `writeLokiVector` for the constant metric subset (sample `[<epoch seconds>, "<value>"]`, no labels)
- [x] `loki_response.go` — `resultType:"scalar"` envelope + `writeLokiScalar` for literal-only expressions (bare `[<epoch seconds>, "<value>"]` result, no labels), matching upstream's LiteralExpr-vs-VectorExpr result types
- [x] `loki_query.go` — instant `/query` routes non-`{` expressions to `ParseScalarQuery` and returns the scalar or vector envelope per `ScalarResult.HasVector`; `query_range` stays log-only
- [x] `loki_query.go` — `parseLokiTime` mirroring Loki's `parseTimestamp`: float seconds (fraction rounded to ms), integer seconds (≤10 **raw characters**, sign included), integer nanoseconds (>10), RFC3339/RFC3339Nano; every path range-checked against Go's representable nanosecond window (1677-09-21 → 2262-04-11) since `UnixNano` wraps rather than failing outside it. `limit` (default 100, reject `<=0`), `direction` (default backward, matched case-insensitively as upstream does) parsing
- [x] `loki_query.go` — `handleLokiQueryRange` (end default now; start precedence `start` > `since` duration > 1h default, all relative bounds anchored at `min(end, now)` so a future `end` still means the last hour of data; `since` on the **Prometheus** duration grammar via `metrics.ParsePromDurationNanos` (`1d`/`1w`/bare `0` valid, `150ns`/`1.5h` not), matching upstream's `model.ParseDuration`; `end<start` → 400; `step` parsed and validated as upstream's shared range-query parser does (float seconds or Prometheus duration, **nanosecond** resolution via `parseLokiStep`, non-positive → 400, 11,000-point limit with the span saturated on int64 overflow as `time.Time.Sub` does) though it cannot shape a stream response; **reject `interval`** with 400 since it thins results and ignoring it would over-return) and `handleLokiQuery` (instant; `time` default now)
- [x] `loki_query.go` — `handleLokiLabels`, `handleLokiLabelValues` (accept + ignore `start`/`end`/`query` this phase)
- [x] `server.go` — add `logQuery *logs.QueryEngine` field + `api.New` param; `router.go` — register the 4 `GET` routes
- [x] `cmd/server/main.go` — `logs.NewQueryEngine(logStore)` → `api.New`; update all other `api.New(` call sites (e.g. `server_test.go`)

**Tests + docs + verify**
- [x] Unit `internal/logs/logql_test.go` — bare selector, each filter op, chained filters, `LineFilter.Keep` (substring + regex, positive/negative); rejections (empty/`{}`, non-`=` label op, pipelines, metric wrappers, invalid regex, unclosed brace/missing quotes)
- [x] Unit `internal/logs/query_test.go` — label-only query, time-range narrowing, substring + regex line filters, backward/forward ordering, global `limit` capping across streams, multi-stream merge, empty result (real temp-dir `*Store` + fake `Reader`)
- [x] Unit `internal/logs/diskstore_test.go` — `StreamLabelSet`/`LabelNames`/`LabelValues` across head-only, flushed-only, mixed state
- [x] Integration `internal/api/loki_query_test.go` — push → label-only `query_range`; time-range query; `|=` text filter; `|~` regex filter; `since` (window, `start` precedence, future-`end` anchoring, `1d`/`1w`/`0` accepted, `150ns`/`1.5h`/malformed → 400); `step` validation (bogus/zero/negative/overflow → 400, valid step unsampled, 11,000-point boundary, sub-millisecond steps in both directions, saturated full-range span); case-insensitive `direction`; `/labels` + `/label/{name}/values`; unsupported LogQL → 400 plain text; malformed params → 400; envelope-shape assertions (`resultType:"streams"`, `["<ns>","line"]`, sorted label `data`)
- [x] Unit `internal/api/loki_time_test.go` (in-package) — `parseLokiTime` exact-value table pinning the raw-character digit rule (`"-1234567890"` is 11 characters → nanoseconds), float/RFC3339 forms, and both int64 boundaries; `sinceStart` grammar parity (both directions of the Go-vs-Prometheus difference) + overflow/underflow guards; `parseLokiStep` nanosecond resolution and range checks
- [x] Unit `internal/api/query_param_test.go` (in-package) — `secondsToMillis`/`parseTimeParam`/`parseDurationParam` boundary tests: `float64(math.MaxInt64)` rounds up to 2^63, so the upper bound is exclusive (`time=9223372036854775` wrapped to `MinInt64` before the fix)
- [x] `ARCHITECTURE_NOTES.md` — "Introduced in 4.4" note for the logs query engine + Loki endpoints
- [x] Verify: `go build ./...`, `go vet ./...`, `golangci-lint run`, `go test ./...` green

### Phase 4.5 — Grafana Logs Demo
Design: `docs/superpowers/specs/2026-07-30-phase-4.5-grafana-logs-demo-design.md` · Plan: `docs/superpowers/plans/2026-07-30-phase-4.5-grafana-logs-demo.md`

**Sample app log generator** *(no backend code changes this phase)*
- [x] `examples/sample-app/main.go` — logs-only generator pushing five streams (`service` ∈ {api, worker} × `level` ∈ {info, warn, error}, constant `env=local`) to `POST /loki/api/v1/push`; plain-text lines (never JSON/logfmt, which are unsupported pipelines); `-addr`/`OBS_BACKEND_ADDR`, `-rate` (default 2 batches/s), `-duration`; log-and-continue on push failure. Emits **no metrics** — a second writer on `http_requests_total{service="api"}` would corrupt `rate()`; app metrics move here in Phase 5.1
- [x] `examples/sample-app/main_test.go` — decode the generated payload and run it through the real `logs.NewStreamLabels` / `logs.ValidateEntry`; pin the five-stream set and per-stream grouping

**Packaging**
- [x] `deployments/docker/Dockerfile` — build `/sampleapp`; add the `sampleapp` distroless runtime stage
- [x] `deployments/docker/docker-compose.yml` — add the `sample-app` service; set `OBS_LOGS_FLUSH_THRESHOLD_BYTES: "16384"` on `backend` so the demo actually exercises the 4.3 chunk/index path instead of serving everything from the head buffer
- [x] `.gitignore` — ignore the locally built `sample-app` binary

**Grafana provisioning**
- [x] `observability/grafana/datasources/loki.yml` — `observability-platform-logs`, `type: loki`, `uid: obs-loki`, `url: http://backend:8080`, `maxLines: 1000`
- [x] `observability/grafana/dashboards/logs.json` — `obs-logs-v1`: two logs panels plus a supported-subset text panel, driven by `service`/`level` `label_values` query variables (**single-select — *All* would emit an unsupported regex label matcher**) and a `search` textbox feeding `|= "$search"`

**Smoke test, docs, cross-links**
- [x] `tests/e2e/logs_smoke.sh` — 20 backend checks under an isolated `service="smoke-test"` + per-run `run_id`: push → 204, streams envelope, `level=` narrowing, `|=` / `|~` / empty-`|= ""` filters, `/labels` + `/label/service/values`, **Grafana's exact datasource health-check request** (`vector(1)+vector(1)` at `time=4000000000`, asserting the value since the 10-character time is read as seconds), and metric LogQL → 400
- [x] `tests/e2e/provisioning_test.go` — Docker-free validation of the Grafana provisioning files, in Go so `go test ./...` covers it and no undocumented interpreter gates it: datasource name/type/uid/`access: proxy`/`maxLines`; the datasource **URL cross-referenced against the `backend` service and port in `docker-compose.yml`** (not a magic string); dashboard uid/title; the **panel set pinned by id, type, title, and target count** (a logs panel whose targets went to `[]` still renders — as an empty panel — so a dashboard-wide target count cannot notice it, and every per-target check below would just iterate one fewer time and pass); the variable list pinned by name **and kind** (a `query` variable demoted to `custom` stops reading the backend while still rendering) with `multi`/`includeAll` false — *All* emits an unsupported regex label matcher — plus `query.type: 1` LabelValues, matching label, unscoped stream, and an empty textbox default; every panel/target/variable datasource reference cross-referenced against `loki.yml`'s own **type and uid** (a `prometheus`-typed ref sends LogQL down the wrong query path while the uid still looks right); and every panel expression interpolated with the variable values Grafana would substitute — including the empty-Search default — then run through the backend's own `logs.ParseLogQL`, so an unsupported pipeline (`| json`, `| logfmt`, `line_format`) or metric query fails here rather than in a browser, plus a check that expressions reference only declared variables (Grafana leaves a misspelled one uninterpolated and the panel silently returns nothing)
- [x] `tests/e2e/compose_smoke.sh` — the real-stack regression test the Docker-requiring DoD items below were only ever verified against by hand. Brings the Compose demo up under its own project name (never touching a `make local-up` stack), then asserts **through Grafana's HTTP API, not the backend's**: datasource health (the *Save & test* button), the provisioned dashboard, both variable dropdowns through the datasource resource proxy, and both panel expressions through `/api/ds/query` — the expressions read out of the dashboard Grafana serves, so the test cannot drift from the panels. Plus the storage path the flush override exists for: chunks reaching disk (counted via `docker compose cp`, since the distroless image has no shell) and the data surviving a `docker compose restart backend`. Row assertions read a seeded marker stream rather than the sample app's random output, whose 8% error rate makes "an error line exists" a coin flip on a short run. **Liveness is asserted, not assumed**: the exact running service set is checked after startup and again at the end (nothing else in the run looks at `load-generator`, which writes metrics rather than logs), and a final query over a window that opens *after* the restart proves the sample app is still producing — label values and seeded lines persist, so "it ran once" and "it is running" are otherwise indistinguishable. Every HTTP request and docker call is individually bounded (`--connect-timeout`/`--max-time`, `timeout`), because a polling deadline means nothing if one attempt can hang forever inside a stalled connection
- [x] `Makefile` — add `smoke-logs` (provisioning `go test` then the live-API script) and `smoke-compose`; `smoke` runs metrics then logs
- [x] `.github/workflows/ci.yml` — add the `compose-e2e` job running `compose_smoke.sh`, and add `push: [main]` + `workflow_dispatch` triggers: all work lands directly on `main`, so a `pull_request`-only trigger meant CI almost never ran
- [x] `docs/runbooks/grafana-logs-demo.md` — datasource test, Explore ladder, dashboard walkthrough, and the known-limitations table
- [x] `docs/runbooks/grafana-demo.md` + `README.md` — four services, cross-links to the logs runbook
- [x] `docs/planning/ARCHITECTURE_NOTES.md` — "Grafana demo assets (introduced in 4.5)": both datasource UIDs on one backend port, both dashboard UIDs, the demo-only flush override, and the known Loki gaps

**Verify**
- [x] Verify: `go build ./...`, `go vet ./...`, `golangci-lint run`, `go test ./...` green
- [x] Verify: `make smoke` exits 0 against a locally run backend (metrics checks then logs checks)
- [x] Verify: every LogQL example in the runbook returns 200 (no example teaches a rejected query)
- [x] Verify *(requires Docker)*: `make local-up` builds and starts four services
- [x] Verify *(requires Docker)*: Loki datasource **Save & test** returns success — Grafana 11.1.0 `/api/datasources/uid/obs-loki/health` → `{"message":"Data source successfully connected.","status":"OK"}`, closing the verification gap Phase 4.4 §11 deferred to this phase
- [x] Verify *(requires Docker)*: logs appear in Grafana Explore; service/level/text filters narrow — all four runbook queries run through Grafana's own `/api/ds/query`, each returning log rows; metric LogQL errors as documented
- [x] Verify *(requires Docker)*: logs dashboard variables populate and both logs panels render — dashboard `obs-logs-v1` provisioned; both variable dropdowns resolve through Grafana's resource proxy (`service` → `[api, worker]`, `level` → `[error, info, warn]`); both panel expressions return rows, including the default empty-Search form `{service="api"} |= ""`. Verified through Grafana's HTTP APIs, not a visual render
- [x] Verify *(requires Docker)*: the demo exercises the persisted chunk path — measured 29 flushes at a **32.0 s** mean interval, 28 of them writing exactly **5 chunks** (one per generated stream) and one writing 7 (the flush that caught the smoke test's two extra `service="smoke-test"` streams), confirming the `OBS_LOGS_FLUSH_THRESHOLD_BYTES=16384` override works as intended

### Phase 4.6 — LogQL Metric Queries
Design: `docs/superpowers/specs/2026-08-04-phase-4.6-logql-metric-queries-design.md` · Plan: `docs/superpowers/plans/2026-08-04-phase-4.6-logql-metric-queries.md`

**`internal/logs` shared-parser refactor (`logql.go`)**
- [x] `parseLineFilters` → `parseLineFiltersPrefix(s) ([]LineFilter, int, error)`: consume as many chained filters as possible, stop at the first token that does not begin a filter, return bytes consumed. A malformed operand *after* a matched operator still errors — stopping is only for an unmatched operator
- [x] `ParseLogQL` treats a non-empty remainder as an error, reproducing both existing messages verbatim so its rejection tests are untouched. One implementation of selector + filter syntax now serves both parsers

**`internal/logs` metric parser (`metricql.go`)**
- [x] `RangeOp` (`count_over_time`, `rate`, `bytes_over_time`, `bytes_rate`) + `String()`; `AggKind` (`AggNone`/`AggSum`); `Grouping{Without, Labels}`; `MetricQuery{Op, Selector, RangeNs, Agg, Grouping}`
- [x] `ParseMetricQuery`: `sum [by|without (labels)] ( range_op( log_expr [duration] ) )`, prefix grouping only, reusing the 4.4 selector/filter parser
- [x] `[range]` durations on upstream's own order — `metrics.ParsePromDurationNanos` first, Go's `time.ParseDuration` as fallback (so `1d`/`1w` *and* `1.5h`/`150ns` parse); non-positive rejected
- [x] `ErrNotMetricQuery` sentinel for constant expressions (`vector(1)`, `1+1`, bare numbers) and for a leading `{`, so handlers can fall through to the scalar shim / log path
- [x] Explicit errors naming the offending construct: unsupported `_over_time` functions, non-`sum` aggregations, `unwrap`, `offset`, binary operations, nested aggregations, `sum({...})` with no range aggregation, empty `by ()`, trailing grouping `sum(...) by (l)`, missing `[range]`, non-positive range

**`internal/logs` metric evaluator (`metriceval.go`)**
- [x] `MetricPoint`, `MetricSeries`, `MetricSample`; `EvalMetricRange(ctx, q, startNs, endNs, stepNs)` and `EvalMetricInstant(ctx, q, tsNs)` (the single-tick case of range)
- [x] Time model: ticks `start, start+step, … ≤ end`; window `(t − range, t]` — start-exclusive, end-inclusive at every tick including the last — so entries read `[start − range, end]` and instant `query` needs no special rule, being the single-tick case; `windowStart` clamps at `MinInt64` instead of wrapping; overflow-safe tick advance via the unsigned difference
- [x] Values: count / Σ`len(line)` bytes / both scaled by `1/rangeSeconds` — one accumulation path with a per-entry weight and a scale factor; line filters applied before counting
- [x] Two-pointer sliding window per stream, `O(entries + ticks)`, emitting on "window non-empty" rather than "value non-zero" so `bytes_over_time` over empty lines is a `0` and an empty window is a gap
- [x] Grouping: `AggNone` → stream labels verbatim; `sum` → no labels; `by` → listed labels the stream carries; `without` → labels minus the listed names. Empty value ≡ absent (both render to the same label set, so splitting would emit duplicate series); length-prefixed group key; series sorted by label set, points ascending
- [x] Argument validation (`stepNs > 0`, `endNs >= startNs`, `RangeNs > 0`) and per-stream `ctx` checks

**`internal/api` routing + envelopes**
- [x] `loki_response.go` — `lokiMatrixResponse`/`lokiMatrixData`/`lokiMatrixSeries` + `writeLokiMatrix`; `writeLokiVectorSamples` for labeled vectors, with `writeLokiVector` reimplemented as a thin wrapper so the health-check response stays byte-identical
- [x] `loki_query.go` — `handleLokiQueryRange` dispatches on the leading `{`; `ErrNotMetricQuery` → 400 naming the instant endpoint; metric path evaluates and writes a matrix
- [x] `loki_query.go` — `handleLokiQuery` tries `ParseMetricQuery` first and falls back to `ParseScalarQuery` on the sentinel; a metric query returns a labeled vector at `time`
- [x] `loki_query.go` — `validLokiStep` → `resolveLokiStep` returning nanoseconds, defaulting an absent step to upstream's `max(floor(rangeSeconds/250), 1)` seconds; explicit-step validation and the 11,000-point limit unchanged; log queries still discard the value
- [x] `interval` keeps its 400 with a message that reads correctly for a matrix; `limit`/`direction` stay parsed by the shared parser and are ignored on the metric path

**Grafana dashboard**
- [x] `observability/grafana/dashboards/logs.json` — panel `id: 4`, `timeseries` drawn as stacked bars, `Log volume by level — $service`, expression `sum by (level) (count_over_time({service="$service"} |= "$search" [$__interval]))`; existing panels shift down (panel 1 `y: 0→7`, panels 2/3 `y: 11→18`)
- [x] `logs.json` text panel `id: 3` — metric queries move from the "returns 400" list to the supported list; unsupported list gains `unwrap`, the other `_over_time` functions, and binary operations; the stale "Phase 4.6" sentence goes
- [x] `tests/e2e/provisioning_test.go` — pin the new panel (id/type/title/target count); add `__interval` to `variableValues`; route the expression check on the leading `{` so metric panels reach `ParseMetricQuery` instead of being skipped

**Tests**
- [x] Unit `internal/logs/metricql_test.go` — accept table asserting the full `MetricQuery` (four ops, with/without filters, `sum`/`by`/`without`, whitespace, Prometheus *and* Go durations); reject table covering every rejection above; sentinel cases
- [x] Unit `internal/logs/metriceval_test.go` — boundary rules (an entry at `t` counts, including at the final tick `end`; one at `t − range` does not); non-step-aligned end; gaps not zeros; `bytes_over_time` zero vs gap; filters before counting; `rate`/`bytes_rate` arithmetic; multibyte byte counting; `by` with an absent label, `without`, bare `sum`, `level=""` grouping with absent; deterministic ordering; underflow clamp; instant = last tick; invalid arguments
- [x] Integration `internal/api/loki_metric_query_test.go` — Grafana's exact log-volume query → matrix with expected counts; bare `count_over_time` per-stream series; `rate`/`bytes_over_time`/`bytes_rate`; instant → labeled vector; default step derivation; 11,000-point rejection; unsupported → 400 `text/plain`; `vector(1)+vector(1)` → 400 on `query_range`, unchanged vector on instant
- [x] Update `TestLokiQueryRange_UnsupportedAndBadParams` (swap `rate(...)` for a still-unsupported expression) and `TestLokiInstantQuery_UnsupportedMetricQuery` (keep only unsupported cases, add a positive counterpart)
- [x] `tests/e2e/logs_smoke.sh` — the metric-LogQL check flips from 400 to success, filtered on `|= "run_id=$RUN_ID"` so repeated runs cannot drift the count; assert matrix envelope, both level groups, value `"1"`; add a `rate` check and keep `avg_over_time` → 400
- [x] `tests/e2e/compose_smoke.sh` — assert the volume panel's expression is in the dashboard Grafana serves, then run it through `/api/ds/query` and assert level-labeled numeric frames with no error

**Docs + roadmap**
- [x] `docs/planning/IMPLEMENTATION_PLAN.md` §4.6 — scope and DoD rewritten for the wider subset (they had named `rate` as still-unsupported, from when the phase was scoped to `count_over_time` and `sum by` alone)
- [x] `docs/planning/ARCHITECTURE_NOTES.md` — "LogQL metric queries (introduced in 4.6)" subsection (grammar, time model, step defaulting, output-label semantics vs the metrics aggregator, memory note); "Known gaps" loses metric LogQL
- [x] `docs/runbooks/grafana-logs-demo.md` — drop the log-volume limitation row; add metric queries to the Explore ladder and the dashboard walkthrough; list what is still unsupported
- [x] `README.md` — LogQL supported-syntax table beside the existing PromQL one

**`| drop` pipeline stage** *(added after review — the phase's DoD was not actually met without it)*
- [x] `internal/logs/logql.go` — `LogSelector.DropLabels`; `parseDropStagePrefix` consuming `| drop a, b` after the line filters; accepted in last position only, bare label names only
- [x] `internal/logs/metricql.go` — same stage inside a range aggregation, parsed before the pipeline rejection so every other stage still errors
- [x] `internal/logs/metriceval.go` + `query.go` — dropped names removed from output labels before grouping (metric path) and from `StreamResult.Labels` (log path); matching is unaffected
- [x] Correct the three places that documented Grafana's volume query without the stage: `internal/api/loki_metric_query_test.go`, `tests/e2e/logs_smoke.sh`, `tests/e2e/compose_smoke.sh`
- [x] `README.md`, `docs/runbooks/grafana-logs-demo.md`, `logs.json` text panel, `ARCHITECTURE_NOTES.md` — record the one supported stage
- [x] `tests/e2e/provisioning_test.go` — assert the volume panel's `drawStyle`/`stacking`; a config regression renders lines instead of stacked bars while every expression check stays green
- [x] `tests/e2e/compose_smoke.sh` — parse the volume panel's multi-series response with `jq` (declared as a prerequisite) rather than substring-matching it: assert exactly the `info` and `error` series and that both carry non-empty numeric samples. Substrings could not distinguish "both series returned" from "one did", and `"values":[[` matches an empty array; the filters were validated against good/missing-series/empty-samples fixtures
- [x] `internal/logs/query.go` — group log results by the post-drop label set, so two streams differing only by a dropped label merge (a stream *is* its label set); keys built lazily per contributing stream
- [x] `docs/planning/IMPLEMENTATION_PLAN.md` §4.6 — name final-position, bare-label `| drop` as the sole pipeline exception in both scope and DoD
- [x] `internal/logs/metriceval.go` — count entries landing exactly on a tick, including the final one at `end`. 4.6 originally excluded them, reasoning from upstream's half-open sample reads without accounting for the leap nanosecond it adds *"to include lines exactly at endTs"*; the effect was a final tick narrower than every other tick. `endInclusive` is gone — range and instant now share one bound

**Verify**
- [x] Verify: `go build ./...`, `go vet ./...`, `golangci-lint run`, `go test ./...` green
- [x] Verify: `make smoke-logs` exits 0 against a locally run backend
- [x] Verify *(requires Docker)*: `make smoke-compose` exits 0 — run in CI rather than locally. The `Compose Stack E2E (through Grafana)` job runs on every push to `main` and has been green throughout Phase 4.6; it is what exercises the panel expression, Explore's `| drop __error__` form, and the parsed multi-series assertions against a real Grafana on the real stack. Most recent green run at the time of writing: `fbe01b8` ([run 31155977249](https://github.com/Weilei424/observability-platform/actions/runs/31155977249)) — later commits are covered by their own runs, so this citation dates itself rather than pinning the tip
- [x] Verify *(requires Docker + a browser)*: Explore's log-volume histogram renders instead of erroring, and the dashboard's volume panel draws stacked bars — confirmed by hand at `7686215` against `make local-up` (Grafana 11.1.0). Explore's **Logs volume** panel rendered stacked bars over `{service="api"}` with three level series (`error` 66, `info` 664, `warning` 101 — Grafana canonicalizes our `warn` to `warning` for display only; the dashboard legend shows the raw `{level="warn"}`). The dashboard's *Log volume by level — $service* panel drew stacked bars for all three levels, and the `Service`/`Level` variables narrowed both it and the log panels

---

## Phase 5 Execution Checklist — Packaging, Kubernetes, and Operational Demo

### Phase 5.1 — Docker Compose Demo
Design: `docs/superpowers/specs/2026-08-23-phase-5.1-docker-compose-demo-design.md` · Plan: `docs/superpowers/plans/2026-08-23-phase-5.1-docker-compose-demo.md`

**Already delivered by earlier phases** *(verify, do not rebuild)*
- [x] Backend container runs from local image — Phase 0.3, `deployments/docker/Dockerfile` (four distroless targets, nonroot uid 65532)
- [x] Grafana container starts with provisioned datasources — Phase 0.3 wiring; `prometheus.yml` (2.5) and `loki.yml` (4.5) on one backend port
- [x] Load generator container produces repeatable traffic — Phase 0.3 + 2.5, `examples/load-generator`. **Not modified in 5.1**: it keeps sole ownership of `http_requests_total` / `http_errors_total` / `http_request_duration_seconds` / `active_connections`
- [x] `make local-up` starts complete demo / `make local-down` cleans up — Phase 0.3
- [x] Dashboards populate after startup, logs half — Phase 4.5, `tests/e2e/compose_smoke.sh` in the CI `compose-e2e` job

**Sample app metrics** *(closes the deferral recorded at Phase 4.5 above)*
- [x] `examples/sample-app/logs.go` — move the log generator out of `main.go` verbatim (`entry`, `apiEntry`, `workerEntry`, `buildBatch`, `encodePush`, `postBatch`, `requestID`, and the `methods`/`paths`/`jobs`/`serverErrs` tables); same package, so `main_test.go` is untouched
- [x] `examples/sample-app/metrics.go` — seven `sample_app_*` series pushed to `POST /api/v1/ingest/metrics`: `sample_app_requests_total{method,status}` ×2, `sample_app_errors_total{method,status}` ×2, `sample_app_request_duration_seconds{method}` ×2, `sample_app_active_workers`, all carrying `service="sample-app"`. **Metric names, not label values, separate the producers** — the existing dashboard aggregates `sum by (method)(rate(http_requests_total[1m]))` with no service filter, so a same-named series from a second writer would silently fold into it. Values are an independent random walk, not derived from the log lines. **Every series is emitted on every tick, error counters included at 0** — a counter that first appears on its own increment cannot satisfy `rate()`'s two-sample requirement, so the panel would read empty exactly when the demo is healthiest. Only `204` counts as delivered, matching `postBatch` rather than load-generator's any-2xx check
- [x] `examples/sample-app/main.go` — `-metrics-rate` flag (default 1/s, validated by the existing `tickerInterval`); the **existing `select` loop gains a third case** rather than a second goroutine, keeping the counters and the single `*rand.Rand` race-free; `startupLine` and the stop line carry the metrics rate and push count; header comment replaced (its "deliberately emits no metrics … move here in Phase 5.1" paragraph is what this item closes)
- [x] `examples/sample-app/metrics_test.go` — payload through the real `metrics.NewLabels` + `metrics.ValidateSample`; exactly the seven series per tick; counter monotonicity and error counters present at 0 from tick one; gauge bounds; only-204-is-delivered

**Health-gated startup**
- [x] `cmd/server/healthcheck.go` — `healthcheckRequested` / `probeURL` / `runHealthcheck`. The distroless image has no shell, `curl`, or `wget`, so the only thing a Compose healthcheck can exec is `/server` itself. `probeURL` maps wildcard hosts (`""`, `0.0.0.0`, `[::]`) to `127.0.0.1`, keeps IPv6 literals bracketed, and rejects port `0` as unprobeable
- [x] `cmd/server/main.go` — branch into probe mode right after `config.Load()` and before the logger, data directory, and every store: a probe must not create files or replay a WAL. Loading config first makes the probe follow `OBS_HTTP_ADDR`
- [x] `cmd/server/healthcheck_test.go` — `probeURL` address table including both error cases; `healthcheckRequested` arg forms; `runHealthcheck` exit codes against `httptest` (200→0, 503→1 with status on stderr, connection refused→1)
- [x] `deployments/docker/docker-compose.yml` — `healthcheck: ["CMD","/server","-healthcheck"]` on `backend`; both producers move to `depends_on: {backend: {condition: service_healthy}}`. Grafana stays ungated — its datasources are `access: proxy` and resolved lazily on first query

**Packaging hygiene**
- [x] `deployments/docker/docker-compose.yml` — `name: observability-platform` (it currently defaults to `docker`, the compose file's directory basename); `restart: unless-stopped` on `backend` and `grafana`. `compose_smoke.sh`'s explicit `-p obs-compose-e2e` outranks the file's `name:`, so its isolation is unchanged
- [x] `Makefile` — `local-logs` (`logs -f`) and `local-reset` (`down -v`). `local-down` keeps volumes on purpose: data surviving a stack restart is the durability story the demo tells, so discarding it must be explicit

**Grafana**
- [x] `observability/grafana/dashboards/sample-app.json` — `obs-sample-app-v1`, "Observability Platform Sample App", four panels (request rate by method, error rate, request duration, active workers), no variables, `refresh: 5s`, `now-5m`. Every target on `obs-prometheus`; every expression inside the supported PromQL subset. The directory provider already provisions it, so `dashboards.yml` is untouched

**Tests**
- [x] `tests/e2e/provisioning_test.go` — `loadDatasource`/`loadDashboard` gain a `path` parameter (8 call sites, mechanical) so the metrics-side tests can reuse them; `loadPanels` keeps its zero-arg signature since it asserts the Loki-specific `wantPanels`
- [x] `tests/e2e/provisioning_metrics_test.go` — the metrics-side counterpart to the 4.5 Loki checks, which left the entire metrics half of the stack with no assertion at any level: Prometheus datasource name/type/uid/`access: proxy`/`isDefault`; its URL cross-referenced against the `backend` service and container port in `docker-compose.yml`; both dashboards' identity and panel sets; every target on `obs-prometheus`; every expression run through the backend's own `metrics.ParseExpr`; and the compose gating (backend declares a healthcheck, both producers wait on `service_healthy`)
- [x] `tests/e2e/compose_smoke.sh` — grow a metrics half in the existing style: Prometheus datasource health (*Save & test*), both metric dashboards provisioned, panel expressions read out of the dashboards Grafana serves and then run through `/api/ds/query`. Assertions use **bare selectors** (`sample_app_active_workers`, `http_requests_total`), not `rate()`, so they do not depend on two samples landing inside a window on a short run; one `rate()` expression is checked only for the absence of a datasource error. This finally gives `load-generator` a data-level assertion — the script's own comment at line 60 records that it has none

**Docs**
- [ ] `README.md` — three provisioned dashboards, not two; `make local-logs` / `make local-reset` in the Quickstart
- [ ] `docs/runbooks/grafana-demo.md` — sample-app now emits metrics **and** logs; add the sample-app dashboard walkthrough and a reset note
- [ ] `docs/runbooks/grafana-logs-demo.md` — same services-list correction
- [ ] `docs/planning/ARCHITECTURE_NOTES.md` — demo-stack subsection: the producer split and why it is by metric name, the two namespaces, and probe-mode health gating

**Verify**
- [ ] Verify: `go build ./...`, `go vet ./...`, `golangci-lint run`, `go test ./...` green
- [ ] Verify: `make smoke` and `make smoke-logs` exit 0 against a locally run backend
- [ ] Verify: `docker compose -f deployments/docker/docker-compose.yml config` parses with the new `name:`, healthcheck, and dependency conditions
- [ ] Verify *(requires Docker)*: `make smoke-compose` exits 0 — locally or via the CI `compose-e2e` job
- [ ] Verify *(requires Docker + a browser)*: `make local-up`, then all three dashboards populate without manual setup

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
