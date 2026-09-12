# observability-platform

A Grafana-compatible observability backend in Go demonstrating backend infrastructure, storage-engine design, and API compatibility through a complete end-to-end workflow.

**ingest metrics/logs → persist durably → index by labels/time → query through Prometheus/Loki-compatible APIs → visualize in Grafana**

This is not a dashboard UI project. Grafana is the UI. The backend observability system is the project.

## Quickstart

```bash
make local-up    # backend :8080 · Grafana :3000 · Prometheus :9090
make smoke       # exercise the metrics and logs APIs against the running stack
make local-down  # stop (keeps the data volumes)
```

Grafana is at http://localhost:3000 (`admin` / `admin`). Data appears within about
15 seconds. The full ten-minute walkthrough, including a proof that ingested data
survives a restart, is [`docs/runbooks/local-demo.md`](docs/runbooks/local-demo.md).

Developing instead of demoing:

```bash
make build
make test
make lint
make run         # run the backend directly, no Docker
```

## What you'll see

Four dashboards are provisioned into Grafana at startup:

| Dashboard | Shows |
|---|---|
| Observability Platform Metrics | Request rate, error rate, latency, and active connections from the load generator |
| Observability Platform Sample App | The sample app's own `sample_app_*` series, deliberately disjoint from the load generator's |
| Observability Platform Logs | Live log streams and log-volume panels from the sample app |
| Observability Platform Internals | The platform observing itself — ingest rate, query latency, WAL size, block and chunk counts, compaction |

The internals dashboard reads from a **separate Prometheus** that scrapes the
backend's `/metrics`, not from the backend's own TSDB. Telemetry that shares
storage with the workload it observes goes blind exactly when that storage breaks.

## Documentation

| Document | Purpose |
|---|---|
| [Local demo](docs/runbooks/local-demo.md) | Run the whole thing on a laptop, end to end |
| [Kubernetes demo](docs/runbooks/kubernetes-demo.md) | The same stack on a cluster, via four Helm charts |
| [Architecture](docs/architecture/README.md) | How metrics, logs, and queries actually flow |
| [Storage layout](docs/architecture/storage-layout.md) | What the backend writes to disk, and why |
| [API reference](docs/api/README.md) | Every endpoint, its parameters, and its responses |
| [Limitations](docs/api/limitations.md) | What is supported, and what this does not do |
| [Self-observability](docs/runbooks/self-observability.md) | The internals dashboard and the metrics behind it |
| [Performance](PERFORMANCE.md) | Benchmark methodology and measured results |

Per-surface runbooks: [metrics dashboard](docs/runbooks/grafana-demo.md) ·
[logs and Explore](docs/runbooks/grafana-logs-demo.md).

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
| Secrets | Environment variables locally; Kubernetes Secrets/Vault later |

## Query support at a glance

Compatibility is a deliberate subset. Unsupported forms return `400` with an
explicit message rather than being silently ignored.

### Metrics

| Form | Example | Status |
|---|---|---|
| Bare metric name | `http_requests_total` | Supported |
| Label selector | `http_requests_total{job="api"}` | Supported |
| `rate` over a range | `rate(http_requests_total[5m])` | Supported |
| `sum by` | `sum by (job)(http_requests_total)` | Supported |
| Any other function | `avg(http_requests_total)` | Returns 400 |

### Logs

| Form | Example | Status |
|---|---|---|
| Stream selector | `{service="api"}` | Supported |
| Chained line filters | `{service="api"} \|= "timeout" != "healthz"` | Supported |
| `count_over_time` | `count_over_time({service="api"}[5m])` | Supported |
| `sum by` over a metric query | `sum by (level) (count_over_time({service="api"}[5m]))` | Supported |
| Regex label matcher | `{service=~"api\|web"}` | Returns 400 |

Full list, including platform limits such as single-node operation and the
absence of authentication: [`docs/api/limitations.md`](docs/api/limitations.md).
Every row above is executed against the real parser by the test suite.

## Kubernetes

Four Helm charts — `deployments/helm/backend`, `prometheus`, `grafana`, and
`producers` — install the same backend image as a StatefulSet with persistent
storage, the internals-scraping Prometheus, Grafana, and the two producers. See
[`docs/runbooks/kubernetes-demo.md`](docs/runbooks/kubernetes-demo.md) for the
walkthrough and [`deployments/helm/README.md`](deployments/helm/README.md) for the
per-chart values reference.

## Planning Docs

- [`docs/planning/IMPLEMENTATION_PLAN.md`](docs/planning/IMPLEMENTATION_PLAN.md) — phase roadmap with goals and DoD
- [`docs/planning/BACKLOG.md`](docs/planning/BACKLOG.md) — phase-by-phase execution checklists
- [`docs/planning/ARCHITECTURE_NOTES.md`](docs/planning/ARCHITECTURE_NOTES.md) — architecture decisions and constraints
