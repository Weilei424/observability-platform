# observability-platform

A Grafana-compatible observability backend in Go demonstrating backend infrastructure, storage-engine design, and API compatibility through a complete end-to-end workflow.

**ingest metrics/logs → persist durably → index by labels/time → query through Prometheus/Loki-compatible APIs → visualize in Grafana**

This is not a dashboard UI project. Grafana is the UI. The backend observability system is the project.

## Stack

| Layer | Technology |
|---|---|
| Backend API | Go |
| Metrics model | Prometheus-style labels |
| Metrics storage | Custom WAL, chunks, immutable time blocks, label index |
| Logs model | Loki-style streams |
| Log storage | Custom WAL, compressed chunks, stream index |
| Query APIs | Prometheus-compatible and Loki-compatible subsets |
| Dashboard | Grafana |
| Local runtime | Docker Compose |
| Kubernetes deployment | Helm + Kubernetes manifests |
| Performance testing | k6 and Go benchmarks |
| Optional object storage | MinIO/S3-compatible abstraction |
| Optional GitOps | ArgoCD |
| Secrets | Environment variables locally; Kubernetes Secrets/Vault later |

## Quickstart

```bash
# Run locally
make run

# Start backend + Grafana + load generator in Docker
make local-up   # backend: http://localhost:8080  grafana: http://localhost:3000
make local-down
make smoke      # API-level smoke test (requires backend running)

# Development
make build
make test
make lint
```

## Grafana Demo

```bash
make local-up
```

Opens `http://localhost:3000` (admin / admin). The provisioned **Observability Platform Metrics** dashboard shows live data from the load generator within ~15 seconds of startup. See [`docs/runbooks/grafana-demo.md`](docs/runbooks/grafana-demo.md) for the full walkthrough.

## Local Metrics Demo (without Docker)

**1. Start the backend:**
```bash
make run
```

**2. In a second terminal, start the load generator:**
```bash
go run examples/load-generator/main.go --rate 2 --duration 30
```

**3. Query ingested metrics:**
```bash
# Instant query — request rate by method
curl 'http://localhost:8080/api/v1/query?query=sum+by+(method)(rate(http_requests_total[1m]))'

# Range query — request duration over the last 60 seconds (Linux)
curl "http://localhost:8080/api/v1/query_range?query=http_request_duration_seconds&start=$(date -d '60 seconds ago' +%s)&end=$(date +%s)&step=15"

# Instant query — active connections gauge
curl 'http://localhost:8080/api/v1/query?query=active_connections'
```

**4. Restart the backend (Ctrl+C in terminal 1, then `make run`) and re-query to confirm WAL replay:**
```bash
curl 'http://localhost:8080/api/v1/query?query=http_requests_total'
# Same two series should appear — data recovered from WAL
```

## Supported Query Syntax

The query API accepts a PromQL subset. Unsupported forms return `400 bad_data`.

| Form | Example | Status |
|---|---|---|
| Bare metric name | `http_requests_total` | Supported |
| Label selector | `http_requests_total{job="api"}` | Supported |
| `rate(selector[duration])` | `rate(http_requests_total[5m])` | Supported |
| `sum(expr)` | `sum(http_requests_total)` | Supported |
| `sum by (label,...)(expr)` | `sum by (job)(http_requests_total)` | Supported |
| Any other function | `avg(...)`, `histogram_quantile(...)` | Returns 400 |
| Numeric scalar arithmetic | `1+1`, `10/4` | Supported (returns `scalar`) |
| Metric arithmetic | `a + b`, `a / b` | Returns 400 |
| Subqueries | `rate(...)[5m:1m]` | Returns 400 |

Duration units accepted: `ms`, `s`, `m`, `h`, `d`, `w`, `y`.

## Performance

Benchmark methodology and measured results (ingestion throughput, query latency
percentiles, compression ratios) are in [`PERFORMANCE.md`](PERFORMANCE.md). Run
`make bench-go` for the in-process engine benchmarks and `make bench-k6` for the
end-to-end k6 HTTP load tests.

## Planning Docs

- [`docs/planning/IMPLEMENTATION_PLAN.md`](docs/planning/IMPLEMENTATION_PLAN.md) — phase roadmap with goals and DoD
- [`docs/planning/BACKLOG.md`](docs/planning/BACKLOG.md) — phase-by-phase execution checklists
- [`docs/planning/ARCHITECTURE_NOTES.md`](docs/planning/ARCHITECTURE_NOTES.md) — architecture decisions and constraints
