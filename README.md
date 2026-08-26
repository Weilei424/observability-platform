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

# Start the demo stack in Docker: backend + Grafana + load generator + sample app
make local-up   # backend: http://localhost:8080  grafana: http://localhost:3000
make local-down
make local-logs # follow the stack's logs
make local-reset # stop the demo and delete its data volumes
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

The provisioned **Observability Platform Sample App** dashboard shows the sample app's
own `sample_app_*` series — request rate, error rate, latency, and worker count. Its
metric names are deliberately disjoint from the load generator's `http_*` series, so the
two simulated workloads never mix in one panel.

The provisioned **Observability Platform Logs** dashboard and Grafana Explore show live log streams from the sample app; see [`docs/runbooks/grafana-logs-demo.md`](docs/runbooks/grafana-logs-demo.md).

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
curl -g 'http://localhost:8080/api/v1/query?query=sum+by+(method)(rate(http_requests_total[1m]))'

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

### LogQL subset

| Form | Example | Status |
|---|---|---|
| Stream selector (equality only) | `{service="api", level="error"}` | Supported |
| Line filters | `{service="api"} \|= "timeout" != "healthz"` | Supported |
| Regex line filters | `{service="api"} \|~ "5\\d\\d"` | Supported |
| `count_over_time` | `count_over_time({service="api"}[5m])` | Supported |
| `rate` | `rate({service="api"}[5m])` | Supported |
| `bytes_over_time`, `bytes_rate` | `bytes_over_time({service="api"}[5m])` | Supported |
| `sum`, `sum by`, `sum without` | `sum by (level) (count_over_time({service="api"}[5m]))` | Supported |
| Regex label matchers | `{service=~"api\|web"}` | Returns 400 |
| `\| drop <labels>` | `{service="api"} \| drop __error__` | Supported (last stage only) |
| Other pipelines and formatters | `\| json`, `\| logfmt`, `line_format`, `\| unwrap` | Returns 400 |
| `unwrap` and its aggregations | `avg_over_time({a="b"} \| unwrap d [5m])` | Returns 400 |
| Other vector aggregations | `count(...)`, `topk(...)` | Returns 400 |
| Binary operations | `sum(...) / sum(...)` | Returns 400 |

Metric queries answer `resultType: "matrix"` on `query_range` and `"vector"` on the
instant endpoint. Range durations accept both the Prometheus grammar (`5m`, `1d`,
`1w`) and Go's (`1.5h`, `150ns`), as upstream LogQL does.

## Performance

Benchmark methodology and measured results (ingestion throughput, query latency
percentiles, compression ratios) are in [`PERFORMANCE.md`](PERFORMANCE.md). Run
`make bench-go` for the in-process engine benchmarks and `make bench-k6` for the
end-to-end k6 HTTP load tests.

## Planning Docs

- [`docs/planning/IMPLEMENTATION_PLAN.md`](docs/planning/IMPLEMENTATION_PLAN.md) — phase roadmap with goals and DoD
- [`docs/planning/BACKLOG.md`](docs/planning/BACKLOG.md) — phase-by-phase execution checklists
- [`docs/planning/ARCHITECTURE_NOTES.md`](docs/planning/ARCHITECTURE_NOTES.md) — architecture decisions and constraints
