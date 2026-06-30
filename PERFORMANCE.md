# Performance

Reproducible performance evidence for the single-node metrics TSDB
(observability-platform). Two harnesses by layer:

- **Go micro-benchmarks** (`testing.B`, in-process) measure the storage/query
  engine directly: ingest append, query evaluation, chunk compression. They can
  control storage state (memory vs WAL, in-memory vs persisted, block count) that
  the HTTP API cannot.
- **k6 HTTP load tests** measure the real API under concurrency: end-to-end
  p50/p95/p99 latency and request/sample throughput as Grafana would see them.

## Hardware & environment

- CPU: 4 vCPU · RAM: ~6 GB · OS: WSL2 (Linux) · Go: go1.26
- Storage: WAL/blocks on the Linux filesystem under `$TMPDIR`
- Captured: 2026-06-30

## How to reproduce

```bash
# Go micro-benchmarks (no external tools)
make bench-go

# End-to-end k6 load tests (installs nothing; requires k6 on PATH)
go install go.k6.io/k6@latest      # one-time; binary lands in $(go env GOPATH)/bin
make bench-k6                       # builds + starts a throwaway backend, seeds, runs k6
```

`make bench-k6` is hermetic: it builds the server, starts it on a fresh temp data
dir, seeds a fixed dataset, runs every k6 scenario, and tears down. Raw JSON
summaries are written to `bench/results/` (gitignored).

## Methodology

- **Dataset:** 1 metric (`bench_http_requests_total`) across up to 10,000 series
  (`instance` has 50 distinct values, `series` unique). Go query benchmarks **and the
  k6 range scenario** select `{instance="inst-7"}` (~1/50 of series); the **k6 instant
  scenario** selects all seeded series (`{job="bench"}`). The seed writes all samples at
  a single `Date.now()` timestamp, so most of the 1h range window is empty ticks.
  **The k6 instant and range query latencies are not directly comparable** — they
  target different series counts (~all seeded vs ~1/50).
- **Go benchmarks** report the mean `ns/op` plus `B/op` / `allocs/op` (`-benchmem`)
  and custom `samples/sec` / `bytes/sample` metrics via `b.ReportMetric`.
- **k6** reports true percentiles over all requests in the run window.

## Results — Go: ingestion (samples/sec, higher is better)

| Path | samples/sec | ns/op | allocs/op |
|---|---|---|---|
| MemoryStore (no WAL) | 7,731,392 | 129.3 | 1 |
| WALStore, fsync every record (n=1) | 299.3 | 3,340,693 | 13 |
| WALStore, fsync every 16 | 4,529 | 220,780 | — |
| WALStore, fsync every 128 | 31,917 | 31,332 | — |
| BlockStore, compaction off | 8,385,729 | — | — |
| BlockStore, compaction on (approx.) | 7,742,826 | — | — |

## Results — Go: query latency (ns/op, lower is better)

| Query | ns/op | allocs/op |
|---|---|---|
| Instant, in-memory head | 60,841 | 14 |
| Instant, persisted block (10k series) | 1,495,806 | 2,039 |
| Range, 60 ticks | 8,038,761 | — |
| Range, 360 ticks | 46,908,346 | — |
| Range, 1440 ticks | 190,903,989 | — |
| Instant, 1 block (2k series) | 305,467 | — |
| Instant, 4 blocks (2k series) | 1,168,750 | — |
| Instant, 16 blocks (2k series) | 4,833,724 | — |

Related (existing benchmarks): indexed `SelectSeries` vs full scan —
`BenchmarkSelectSeries_Indexed` 21,908 ns/op vs `BenchmarkSelectSeries_FullScan`
396,181 ns/op (run `go test -bench=SelectSeries -run='^$' ./internal/metrics/`).

## Results — Go: chunk compression (bytes/sample, lower is better)

| Pattern | bytes/sample | chunk bytes (120 samples) |
|---|---|---|
| Monotonic counter | 2.583 | 310 |
| Gauge random-walk | 9.667 | 1,160 |
| Constant | 1.617 | 194 |

## Results — k6: end-to-end HTTP (per-request latency)

| Scenario | req/s | p50 (ms) | p95 (ms) | p99 (ms) |
|---|---|---|---|---|
| Ingest (`POST /api/v1/ingest/metrics`) | 2.59 | 1,527.7 | 1,658.1 | 1,660.9 |
| Instant query (`GET /api/v1/query`) | 878.8 | 3.96 | 6.92 | 10.28 |
| Range query (`GET /api/v1/query_range`) | 5,175.1 | 0.68 | 1.10 | 1.57 |

Ingest throughput: 259.1 samples/s (from the `samples_sent` rate).

> **Selector note:** The instant scenario uses `{job="bench"}` (all seeded series,
> high cardinality); the range scenario uses `{instance="inst-7"}` (~1/50 of series,
> same selector as the Go query benchmarks). This is why range query shows lower
> latency and higher req/s than instant query — they do not measure the same
> workload and are not directly comparable.

## Interpretation

- **Durability cost:** WAL+fsync-every-record vs memory-only is the
  25,832× gap; batching to n=128 closes most of the multiplicative gap — from
  25,832× down to ~242× (7,731,392 vs 31,917 samples/s) — but fsync still
  dominates per-op cost and throughput remains far below the memory baseline.
- **Persisted vs memory reads:** instant queries on a persisted block cost
  24.6× an in-memory head read (block open + chunk decode).
- **Block fan-out:** instant latency grows from 1 → 16 blocks as the store
  consults more readers.
- **Compression:** constant/counter series compress to a fraction of an
  uncompressed 16-byte (8B ts + 8B float) sample; gauge random-walk is the worst
  case.

## Caveats

- WSL2 fsync semantics differ from bare-metal Linux; absolute WAL numbers are
  indicative, the memory-vs-WAL *ratio* is the signal.
- k6 and the backend share the same 4 vCPUs on this box, so k6 latency includes
  client-side contention — treat as a single-box figure, not a server ceiling.
- `BenchmarkIngest_CompactionOnOff` runs a background flush+compact loop and is
  labeled approximate.
- Go `testing.B` reports the **mean** `ns/op`, not a percentile; percentiles come
  from the k6 layer.
