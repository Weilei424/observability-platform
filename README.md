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

# Start backend + Grafana in Docker
make local-up   # backend: http://localhost:8080  grafana: http://localhost:3000
make local-down

# Development
make build
make test
make lint
```

## Planning Docs

- [`docs/planning/IMPLEMENTATION_PLAN.md`](docs/planning/IMPLEMENTATION_PLAN.md) — phase roadmap with goals and DoD
- [`docs/planning/BACKLOG.md`](docs/planning/BACKLOG.md) — phase-by-phase execution checklists
- [`docs/planning/ARCHITECTURE_NOTES.md`](docs/planning/ARCHITECTURE_NOTES.md) — architecture decisions and constraints
