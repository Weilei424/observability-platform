# k6 HTTP load tests

End-to-end latency/throughput tests for the metrics API. Prefer `bash bench/run.sh`
(or `make bench-k6`), which builds + starts a throwaway backend on a free ephemeral
port, seeds a fixed deterministic dataset (`seed.js`), runs the query scenarios,
then the ingest throughput scenario, and tears down. Run a single script standalone
like this:

## Prerequisites

- A running backend (e.g. `OBS_DATA_DIR="$(mktemp -d)" make run`).
- k6 on PATH: `go install go.k6.io/k6@latest` (binary lands in `$(go env GOPATH)/bin`).

## Scripts

| Script | Measures | Endpoint |
|---|---|---|
| `seed.js` | deterministic seed: `CARDINALITY` series × 1 sample (fixed iterations) | `POST /api/v1/ingest/metrics` |
| `ingest.js` | ingest req/s, samples/s, p50/p95/p99 (random live-load) | `POST /api/v1/ingest/metrics` |
| `instant_query.js` | instant-query p50/p95/p99 | `GET /api/v1/query` |
| `range_query.js` | range-query p50/p95/p99 (1h / 15s) | `GET /api/v1/query_range` |

Gating: correctness checks are a hard gate; a latency-threshold breach also fails
the run (exit 99) unless `BENCH_ALLOW_THRESHOLD_BREACH=1` is set.

## Env knobs

- `BASE_URL` (default `http://localhost:8080`)
- `VUS` (virtual users), `DURATION` (e.g. `30s`)
- `CARDINALITY` (distinct series, default 1000), `BATCH` (samples/request, default 100)

## Example

```bash
mkdir -p bench/results
k6 run -e VUS=10 -e DURATION=30s bench/k6/ingest.js
k6 run -e VUS=10 -e DURATION=30s bench/k6/instant_query.js
```

JSON summaries are written to `bench/results/<scenario>.json` (gitignored).
