# Implementation Plan

## Overview

**observability-platform** is a Grafana-compatible observability backend portfolio project. It demonstrates backend infrastructure, storage-engine design, API compatibility, and cloud-native deployment through a complete end-to-end workflow:

**ingest metrics/logs → persist durably → index by labels/time → query through Prometheus/Loki-compatible APIs → visualize in Grafana**

This is not a dashboard UI project. Grafana is the UI. The project value is the backend observability platform behind Grafana.

Development is **single-node first**. Distributed mode only begins after ingestion, WAL, block/chunk storage, indexing, query APIs, and Grafana integration work locally.

---

## Build Principles

1. **Grafana compatibility is a core milestone.** The platform must expose enough Prometheus/Loki-compatible API behavior for real Grafana data sources to work.
2. **Storage internals matter.** Avoid hiding all data in a generic SQL table. WAL, chunks, blocks, indexes, compaction, and retention are the core learning value.
3. **Start with metrics before logs.** Metrics provide the strongest TSDB signal: series identity, label indexing, range queries, compaction, and rate/aggregation functions.
4. **Avoid full Prometheus/Loki clones.** Implement carefully selected compatible subsets that are useful, demoable, and explainable.
5. **Every phase must leave the repo runnable.** Do not create large broken rewrites. Each phase should produce a small working increment.
6. **Tests are part of the phase DoD.** A phase is not complete until its main unit/integration/e2e checks pass.

---

## Phase 0 — Repository Foundation and Local Runtime

**Goal:** Establish a clean repository, Go service skeleton, local runtime, and planning discipline so AI coding agents can safely implement the project incrementally.

### Phase 0.1 — Repository Layout and Planning Docs

**Goal:** Create the repository structure and source-of-truth planning files.

**Scope:**
- Create the documented repository layout.
- Add `docs/planning/BACKLOG.md`, `docs/planning/IMPLEMENTATION_PLAN.md`, and `docs/planning/ARCHITECTURE_NOTES.md`.
- Add top-level `CLAUDE.md` and `AGENTS.md` for AI agent workflow.
- Add `README.md` with the project goal, local quickstart placeholder, and architecture summary.

**DoD:**
- All source-of-truth docs exist in the expected paths.
- `README.md` clearly states that Grafana is the UI and the backend is the project.
- No implementation phase is marked complete before code exists.

### Phase 0.2 — Go Service Skeleton

**Goal:** Create a minimal Go backend that can run locally and expose basic health endpoints.

**Scope:**
- Initialize Go module.
- Add `cmd/server/main.go`.
- Add config loading from env variables.
- Add structured logging helper.
- Add HTTP router with request ID middleware.
- Add `GET /healthz` and `GET /readyz`.

**DoD:**
- `go build ./...` passes.
- `go test ./...` passes.
- `GET /healthz` returns 200.
- `GET /readyz` returns 200 when the local data directory is writable.

### Phase 0.3 — Local Runtime and Tooling

**Goal:** Make local execution repeatable for humans and AI agents.

**Scope:**
- Add `Makefile` targets: `build`, `test`, `lint`, `run`, `local-up`, `local-down`.
- Add Dockerfile for the backend.
- Add Docker Compose with backend and Grafana placeholders.
- Add local data directory configuration.
- Add GitHub Actions for build/test/lint.

**DoD:**
- `make build` works.
- `make test` works.
- `make run` starts the service locally.
- `make local-up` starts backend + Grafana without requiring Kubernetes.
- CI workflow runs Go build and tests.

---

## Phase 1 — Single-Node Metrics TSDB

**Goal:** Build the first real backend capability: ingest metrics, model time series correctly, persist samples durably, recover after restart, and query recent/historical samples.

### Phase 1.1 — Metrics Data Model

**Goal:** Define the core Prometheus-style metric model.

**Scope:**
- Define metric name, labels, series ID, timestamp, and float64 sample value.
- Implement deterministic label normalization.
- Implement deterministic series fingerprinting from metric name + sorted labels.
- Add validation for metric names, label names, timestamps, and values.

**DoD:**
- Same metric + same labels in different map orders produces the same series ID.
- Invalid metric names and labels are rejected with clear errors.
- Unit tests cover series identity, label sorting, and validation edge cases.

### Phase 1.2 — Metrics Ingestion API

**Goal:** Accept metric samples through a simple internal ingestion API before adding Prometheus compatibility.

**Scope:**
- Add `POST /api/v1/ingest/metrics`.
- Accept JSON payloads with metric name, labels, timestamp, and value.
- Append samples to an in-memory series store.
- Return clear validation errors for malformed payloads.

**DoD:**
- Valid metric samples can be ingested.
- Multiple samples for the same series append in timestamp order.
- Duplicate/out-of-order handling is defined and tested.
- Integration test covers ingest → in-memory store.

### Phase 1.3 — In-Memory Query Engine

**Goal:** Query ingested samples before persistent block storage exists.

**Scope:**
- Add basic selector parser: `metric_name{label="value"}`.
- Support equality label matchers only.
- Add instant query over the latest sample at or before query time.
- Add range query over samples in `[start, end]`.

**DoD:**
- `GET /api/v1/query` returns matching latest samples.
- `GET /api/v1/query_range` returns matching time ranges.
- Unit tests cover selector parsing and matcher behavior.
- Integration test covers ingest → query and ingest → query_range.

### Phase 1.4 — WAL Durability

**Goal:** Persist ingested metric samples before acknowledging writes.

**Scope:**
- Implement append-only WAL segments for metric samples.
- Add WAL record encoding/decoding.
- Add fsync policy configuration.
- Add WAL replay on startup.
- Add corruption handling for partial trailing records.

**DoD:**
- Ingested data survives process restart.
- WAL replay restores series registry and samples.
- Partial trailing records do not crash startup.
- Unit tests cover WAL encode/decode and replay.
- Integration test covers ingest → shutdown → restart → query.

### Phase 1.5 — Phase 1 End-to-End Metrics Path

**Goal:** Make the first metrics TSDB slice demoable.

**Scope:**
- Add sample load generator for metrics.
- Add basic docs for local metrics ingestion and query.
- Add smoke test script for ingest/query/restart.

**DoD:**
- A user can run the backend, ingest metrics, query metrics, restart, and query again.
- README has a working local metrics demo command sequence.
- Phase 1 checklist items in `BACKLOG.md` are updated accurately.

---

## Phase 2 — Grafana-Compatible Metrics API

**Goal:** Make real Grafana connect to the metrics backend through a Prometheus data source.

### Phase 2.1 — Prometheus Response Envelope

**Goal:** Match the response shapes Grafana expects from Prometheus-compatible APIs.

**Scope:**
- Implement Prometheus-style JSON response envelopes: `status`, `data`, `resultType`, `result`.
- Normalize errors into Prometheus-like error responses.
- Add timestamp/value formatting compatible with Grafana.

**DoD:**
- API responses match expected Prometheus-compatible shapes.
- Unit tests validate response serialization.
- Grafana can successfully call the data source test endpoint or equivalent query endpoint.

### Phase 2.2 — Prometheus Instant and Range Query Endpoints

**Goal:** Expose the metrics query engine through standard Prometheus paths.

**Scope:**
- Implement `GET /api/v1/query`.
- Implement `GET /api/v1/query_range`.
- Support selectors like `cpu_usage{service="api"}`.
- Support query parameters: `query`, `time`, `start`, `end`, `step`.

**DoD:**
- Grafana can render a time-series panel from `query_range`.
- Invalid query parameters return clear errors.
- Integration tests cover instant and range query endpoints.

### Phase 2.3 — Prometheus Metadata Endpoints

**Goal:** Implement enough discovery APIs for Grafana query editor usability.

**Scope:**
- Implement `GET /api/v1/labels`.
- Implement `GET /api/v1/label/{name}/values`.
- Implement `GET /api/v1/label/__name__/values`.
- Implement `GET /api/v1/series` for basic match selectors.

**DoD:**
- Grafana can list metric names.
- Grafana can list label names and values.
- Unit/integration tests cover labels, label values, and series discovery.

### Phase 2.4 — Minimal Query Functions

**Goal:** Add a small query subset that makes dashboards meaningful without building full PromQL.

**Scope:**
- Support raw selectors.
- Support `rate(metric[window])` for counters.
- Support `sum(metric)`.
- Support `sum by (label)(metric)`.
- Document unsupported PromQL features clearly.

**DoD:**
- `rate(http_requests_total[5m])` works for a monotonic counter.
- `sum by (service)(http_requests_total)` groups by label.
- Tests cover raw selector, rate, sum, and grouped sum.
- Unsupported functions return explicit errors, not silent wrong results.

### Phase 2.5 — Grafana Metrics Dashboard Demo

**Goal:** Prove interoperability with real Grafana.

**Scope:**
- Add Grafana datasource provisioning for Prometheus-compatible endpoint.
- Add a sample metrics dashboard.
- Add a sample app or load generator that emits useful metrics.
- Add demo docs and screenshots placeholder path.

**DoD:**
- `make local-up` starts backend + Grafana.
- Grafana datasource connects to the backend.
- Dashboard shows live metrics from the sample app/load generator.
- E2E test or scripted smoke test validates the Grafana query path at the API level.

---

## Phase 3 — Metrics Storage Engine Improvements

**Goal:** Move from an in-memory/WAL-backed prototype to a realistic TSDB engine with chunks, immutable blocks, indexes, compaction, retention, and benchmarks.

### Phase 3.1 — Chunked Sample Storage

**Goal:** Store samples in compressed chunks instead of unbounded in-memory arrays.

**Scope:**
- Define chunk format for series samples.
- Add chunk append logic.
- Add chunk encoding/decoding.
- Add compression with Snappy or Zstd.
- Track min/max timestamp per chunk.

**DoD:**
- Samples are grouped into chunks by size/time threshold.
- Chunks can be encoded, compressed, decoded, and queried.
- Tests cover chunk boundaries, empty chunks, and compression round-trip.

### Phase 3.2 — Immutable Time Blocks

**Goal:** Flush chunks into immutable time-bounded blocks.

**Scope:**
- Define block layout: `meta.json`, `index`, `chunks`.
- Implement block writer.
- Implement block reader.
- Implement safe write pattern with temp directory + atomic rename.
- Track block min/max time, series count, and sample count.

**DoD:**
- Blocks are written safely and can be read after restart.
- Query engine reads from both recent memory/WAL state and persisted blocks.
- Integration test covers ingest → flush block → restart → query block data.

### Phase 3.3 — Label Index

**Goal:** Avoid full scans by indexing labels and series IDs.

**Scope:**
- Build index mappings: metric name → series IDs, label name → values, label pair → series IDs, series ID → chunk references.
- Persist index inside block storage.
- Use index during query planning.
- Add label cardinality metrics.

**DoD:**
- Queries use index lookup for equality matchers.
- Label and series endpoints use the persistent index.
- Tests cover index build, load, and query filtering.
- Benchmark shows indexed lookup is faster than full scan on a non-trivial dataset.

### Phase 3.4 — Compaction and Retention

**Goal:** Add background storage maintenance.

**Scope:**
- Implement compactor that merges smaller adjacent blocks.
- Implement retention cleanup by time window.
- Add safe deletion behavior.
- Add metrics for compaction duration, block count, and retained bytes.

**DoD:**
- Small blocks can be compacted into a larger block without losing data.
- Expired blocks are deleted only after retention policy says they are safe to delete.
- Tests cover compaction correctness and retention boundaries.

### Phase 3.5 — Performance Benchmarks

**Goal:** Produce credible performance evidence for resume and README.

**Scope:**
- Add k6 or Go benchmark scripts for ingestion throughput and query latency.
- Track p50/p95/p99 query latency.
- Track samples/sec ingestion throughput.
- Add `PERFORMANCE.md` with reproducible methodology.

**DoD:**
- Benchmarks are reproducible locally.
- README links to `PERFORMANCE.md`.
- Performance report includes dataset size, hardware notes, command used, and metrics observed.

---

## Phase 4 — Mini Loki-Style Logs Backend

**Goal:** Add log ingestion, storage, indexing, and query support using a Loki-inspired model, then expose enough Loki-compatible behavior for Grafana.

### Phase 4.1 — Log Stream Data Model

**Goal:** Define label-based log streams.

**Scope:**
- Define stream labels, stream ID, timestamp, and log line.
- Implement deterministic stream fingerprinting.
- Validate label names, timestamps, and line sizes.
- Define ordering behavior for out-of-order log lines.

**DoD:**
- Same label set produces same stream ID.
- Invalid labels/timestamps are rejected clearly.
- Unit tests cover stream identity and validation.

### Phase 4.2 — Loki-Compatible Push API

**Goal:** Accept logs through a Loki-style ingestion endpoint.

**Scope:**
- Implement `POST /loki/api/v1/push`.
- Accept Loki-style `streams` payload.
- Write log records to WAL before acknowledgment.
- Buffer logs into per-stream chunks.

**DoD:**
- Loki-style push payloads are accepted.
- Logs survive restart through WAL replay.
- Integration test covers push → restart → query-ready storage.

### Phase 4.3 — Log Chunk Storage and Index

**Goal:** Persist logs in compressed chunks with label/time index.

**Scope:**
- Define log chunk format.
- Compress log chunks.
- Persist stream index: label pair → stream IDs, stream ID → chunk references.
- Store min/max timestamp per chunk.

**DoD:**
- Log chunks are persisted and loaded after restart.
- Label filters narrow the search to matching streams.
- Tests cover chunk encode/decode, compression, and index filtering.

### Phase 4.4 — Loki-Compatible Query API

**Goal:** Query logs through a Loki-compatible subset.

**Scope:**
- Implement `GET /loki/api/v1/query`.
- Implement `GET /loki/api/v1/query_range`.
- Implement `GET /loki/api/v1/labels`.
- Implement `GET /loki/api/v1/label/{name}/values`.
- Support selectors like `{service="api", level="error"}`.
- Support text filter `|= "text"`.

**DoD:**
- Grafana Explore can query logs by labels.
- Text filtering works inside candidate chunks.
- Tests cover label-only queries, time-range queries, and text filters.
- Unsupported LogQL features return explicit errors.

### Phase 4.5 — Grafana Logs Demo

**Goal:** Demonstrate logs in real Grafana.

**Scope:**
- Add Grafana datasource provisioning for Loki-compatible endpoint.
- Add sample app log generator.
- Add demo dashboard or Explore workflow docs.

**DoD:**
- Grafana can connect to the backend as a Loki data source.
- Logs appear in Grafana Explore.
- User can filter logs by service/level and search text.

---

## Phase 5 — Packaging, Kubernetes, and Operational Demo

**Goal:** Turn the working single-node observability backend into a clean local/Kubernetes demo with repeatable deployment, sample workloads, dashboards, and runbooks.

### Phase 5.1 — Docker Compose Demo

**Goal:** Make the project easy to run locally.

**Scope:**
- Backend container.
- Grafana container.
- Sample app container.
- Load generator container.
- Provisioned Grafana datasources and dashboards.

**DoD:**
- `make local-up` starts the complete local demo.
- Metrics and logs flow into backend without manual setup.
- Grafana dashboards work after startup.

### Phase 5.2 — Kubernetes Manifests and Helm Chart

**Goal:** Deploy the platform into Kubernetes with persistent storage.

**Scope:**
- Helm chart for backend.
- Kubernetes manifests for Grafana demo.
- PersistentVolumeClaim for local data.
- ConfigMap/Secret support.
- Service definitions for backend and Grafana.

**DoD:**
- Helm install deploys backend successfully.
- Backend persists data across pod restart.
- Grafana can query backend inside Kubernetes.
- Runbook documents deploy, verify, and cleanup commands.

### Phase 5.3 — Platform Self-Observability

**Goal:** Expose metrics and logs about the observability platform itself.

**Scope:**
- Add `/metrics` endpoint for backend internals.
- Emit metrics for ingestion rate, query latency, WAL size, block count, compaction duration, log chunk count, and error counts.
- Add structured logs with correlation IDs.
- Add Grafana dashboard for backend internals.

**DoD:**
- Backend emits self-observability metrics.
- Dashboard shows ingest/query/storage health.
- Logs include request IDs and component names.

### Phase 5.4 — Documentation and Demo Runbook

**Goal:** Make the project understandable and interview-ready.

**Scope:**
- Add architecture diagrams.
- Add local demo runbook.
- Add Kubernetes deployment runbook.
- Add API reference docs.
- Add limitations section.

**DoD:**
- A reviewer can run the demo from README without guessing.
- Architecture docs explain metrics path, logs path, query path, and storage layout.
- Limitations honestly document unsupported PromQL/LogQL features.

---

## Phase 6 — Distributed Mode

**Goal:** Evolve the single-node backend into a simplified distributed observability platform with separate write/read paths, sharding, replication, and query fanout.

### Phase 6.1 — Component Split

**Goal:** Split one binary into deployable logical components while preserving local single-binary mode.

**Scope:**
- Add component modes: `gateway`, `ingester`, `querier`, `store`, `compactor`.
- Keep `all-in-one` mode for local development.
- Move component-specific wiring behind interfaces.

**DoD:**
- `all-in-one` mode still passes all existing tests.
- Each component mode starts independently.
- Documentation explains component responsibilities.

### Phase 6.2 — Ring-Based Sharding

**Goal:** Route writes to ingesters using a deterministic ring.

**Scope:**
- Implement consistent hashing or ring assignment for series/stream IDs.
- Add ingester membership configuration.
- Route metrics/log writes based on series/stream fingerprint.
- Add tests for stable placement and membership changes.

**DoD:**
- Same series/stream routes to the same ingester while ring membership is stable.
- Adding/removing an ingester only remaps part of the keyspace.
- Tests cover ring edge cases.

### Phase 6.3 — Replication and Failure Handling

**Goal:** Add simple replication for write durability in distributed mode.

**Scope:**
- Configurable replication factor.
- Write to N ingesters.
- Define quorum behavior.
- Surface partial write failures clearly.
- Add failure tests with fake/unavailable ingesters.

**DoD:**
- Writes succeed when quorum is met.
- Writes fail clearly when quorum is not met.
- Duplicate replicated samples do not corrupt query results.

### Phase 6.4 — Query Fanout and Merge

**Goal:** Query data across multiple ingesters/stores.

**Scope:**
- Gateway/querier fans out query requests.
- Merge metrics query results by series/time.
- Merge log query results by timestamp.
- Deduplicate replicated data.

**DoD:**
- Querying through gateway returns complete results across shards.
- Duplicate replicated samples/log lines are deduplicated.
- Integration tests cover multi-ingester ingest → query.

### Phase 6.5 — Multi-Tenant Boundaries

**Goal:** Add basic tenant isolation for a realistic Mimir/Loki-inspired architecture.

**Scope:**
- Tenant ID from request header.
- Tenant-aware series/stream identity.
- Tenant-aware query filtering.
- Tenant-aware retention configuration.
- Basic per-tenant limits for ingest payload size and active series/streams.

**DoD:**
- Tenant A cannot query Tenant B data.
- Tenant limits are enforced and tested.
- Metrics expose tenant-scoped active series/stream counts without leaking data.

---

## Minimum Resume-Worthy Version

The smallest version worth putting on a resume should include:

- Go backend.
- Metrics ingestion.
- WAL durability and restart recovery.
- Time-series query API.
- Label index.
- Prometheus-compatible endpoints.
- Real Grafana dashboard.
- Docker Compose demo.
- Basic benchmark report.

Resume bullet:

> Built **observability-platform**, a Grafana-compatible time-series backend in Go with WAL-backed ingestion, label indexing, query-range support, restart recovery, and Dockerized Grafana dashboards.

---

## Full Target Version

The full version should include:

- Metrics backend.
- Logs backend.
- WAL for metrics and logs.
- Compressed chunks.
- Immutable time blocks.
- Label indexes.
- Prometheus-compatible query API.
- Loki-compatible query API.
- Grafana metrics and logs dashboards.
- Compaction and retention.
- Kubernetes + Helm deployment.
- k6 benchmark report.
- Optional distributed ingester/querier/store split.

Resume bullet:

> Built **observability-platform**, a Grafana-compatible observability backend inspired by Prometheus, Loki, and Mimir, supporting WAL-backed metrics/log ingestion, TSDB block storage, label indexing, compressed log chunks, compaction, retention, Kubernetes deployment, and distributed query fanout.
