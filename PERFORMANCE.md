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
dir, **seeds a fixed deterministic dataset** (`seed.js`, `shared-iterations`:
exactly `CARDINALITY` series × 1 sample, independent of machine speed), runs the
query scenarios against that dataset, and finally runs the random ingest
throughput scenario (last, so its random series/timestamps don't perturb the
dataset the queries measured). Raw JSON summaries are written to `bench/results/`
(gitignored).

**Gating:** both correctness and latency are **hard** gates. A failed k6 check
(e.g. an empty query result) aborts the run via a `.status` marker; a latency
threshold breach (k6 exit 99) also aborts. The latency thresholds are loose
gross-regression bounds, not SLAs. On a known-slow box, set
`BENCH_ALLOW_THRESHOLD_BREACH=1` to downgrade a latency breach to a recorded
warning (correctness stays hard).

## Methodology

- **Dataset:** 1 metric (`bench_http_requests_total`) across up to 10,000 series
  (`instance` has 50 distinct values, `series` unique). Go query benchmarks **and the
  k6 range scenario** select `{instance="inst-7"}` (~1/50 of series); the **k6 instant
  scenario** selects all seeded series (`{job="bench"}`). The k6 seed is deterministic
  (`shared-iterations` covers every series exactly once at a single timestamp), so each
  run queries the same cardinality and history; most of the 1h range window is empty
  ticks. **The k6 instant and range query latencies are not directly comparable** — they
  target different series counts (~all seeded vs ~1/50).
- **Go instant-query comparison:** `BenchmarkInstant_InMemory` and
  `BenchmarkInstant_Persisted` use the **same** 10k series × 120 samples dataset, so
  their ratio isolates the persistence cost (block open + chunk decode) rather than
  conflating it with dataset size.
- **Go benchmarks** report the mean `ns/op` plus `B/op` / `allocs/op` (`-benchmem`)
  and custom `samples/sec` / `bytes/sample` metrics via `b.ReportMetric`.
- **k6** reports true percentiles over all requests in the run window.

## Results — Go: ingestion (samples/sec, higher is better)

| Path | samples/sec | ns/op | allocs/op |
|---|---|---|---|
| MemoryStore (no WAL) | 7,560,960 | 132.3 | 1 |
| WALStore, fsync every record (n=1) | 292.5 | 3,418,274 | 13 |
| WALStore, fsync every 16 | 4,379 | 228,339 | — |
| WALStore, fsync every 128 | 30,818 | 32,449 | — |
| BlockStore, compaction off | 15,124,611 | — | — |
| BlockStore, compaction on (approx., 2 compactions ran) | 14,809,982 | — | — |

The BlockStore rows advance the timestamp by 1ms/append so flushed blocks stay
within the 2h base compaction range and actually merge — the "compaction on" run
reports a non-zero `compactions` metric to prove it. Because of the small
timestamp deltas, those two rows' absolute samples/sec is only comparable to each
other, not to the memory/WAL rows above (which step by 1000ms).

## Results — Go: query latency (ns/op, lower is better)

| Query | ns/op | allocs/op |
|---|---|---|
| Instant, in-memory head (10k series × 120 samples) | 517,777 | 214 |
| Instant, persisted block (10k series × 120 samples) | 1,496,975 | 2,039 |
| Range, 60 ticks | 8,095,899 | — |
| Range, 360 ticks | 48,423,661 | — |
| Range, 1440 ticks | 200,247,502 | — |
| Instant, 1 block (2k series) | 314,837 | — |
| Instant, 4 blocks (2k series) | 1,176,519 | — |
| Instant, 16 blocks (2k series) | 4,755,945 | — |

Related (existing benchmarks): indexed `SelectSeries` vs full scan —
`BenchmarkSelectSeries_Indexed` 21,867 ns/op vs `BenchmarkSelectSeries_FullScan`
394,221 ns/op (run `go test -bench=SelectSeries -run='^$' ./internal/metrics/`).

## Results — Go: chunk compression (bytes/sample, lower is better)

| Pattern | bytes/sample | chunk bytes (120 samples) |
|---|---|---|
| Monotonic counter | 2.583 | 310 |
| Gauge random-walk | 9.683 | 1,162 |
| Constant | 1.617 | 194 |

The gauge random-walk uses a locally seeded PRNG, so its byte count is
reproducible run-to-run.

## Results — k6: end-to-end HTTP (per-request latency)

| Scenario | req/s | p50 (ms) | p95 (ms) | p99 (ms) |
|---|---|---|---|---|
| Instant query (`GET /api/v1/query`) | 424.9 | 11.92 | 29.16 | 40.42 |
| Range query (`GET /api/v1/query_range`) | 4,149.2 | 1.81 | 5.09 | 8.84 |
| Ingest (`POST /api/v1/ingest/metrics`) | 2.9 | 3,417.0 | 3,445.1 | 3,445.2 |

Ingest throughput: 292.7 samples/s (from the `samples_sent` rate). The ingest
scenario is fsync-bound (~3.4s p95 here) and passes under the loose 8000ms
gross-regression bound; all correctness checks passed.

> **Selector note:** The instant scenario uses `{job="bench"}` (all seeded series,
> high cardinality); the range scenario uses `{instance="inst-7"}` (~1/50 of series,
> same selector as the Go query benchmarks). This is why range query shows lower
> latency and higher req/s than instant query — they do not measure the same
> workload and are not directly comparable.

## Interpretation

- **Durability cost:** WAL+fsync-every-record vs memory-only is a ~25,850× gap
  (7,560,960 vs 292.5 samples/s); batching to n=128 closes most of it — down to
  ~245× (7,560,960 vs 30,818 samples/s) — but fsync still dominates per-op cost and
  throughput remains far below the memory baseline.
- **Persisted vs memory reads:** on the same 10k×120 dataset, an instant query on a
  persisted block costs ~2.9× an in-memory head read (1,496,975 vs 517,777 ns/op) —
  the block open + chunk decode overhead, isolated from dataset size.
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
  labeled approximate (goroutine scheduling noise). It reports a `compactions`
  metric so a run proves compaction actually engaged rather than only flushing.
- k6 latency is highly variable run-to-run on this shared box; treat the numbers as
  indicative single-box figures. The dataset shape (cardinality/history) is fixed by
  the deterministic seed, so correctness and relative behavior are reproducible even
  when absolute latency drifts.
- Go `testing.B` reports the **mean** `ns/op`, not a percentile; percentiles come
  from the k6 layer.
