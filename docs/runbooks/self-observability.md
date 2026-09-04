# Platform Self-Observability

This runbook covers how to view the backend's internal metrics — ingestion rate, query latency, storage state, and compaction progress — through the **Observability Platform Internals** dashboard in Grafana.

## Dashboard and Datasources

The dashboard (`obs-self-v1`, title "Observability Platform Internals") shows 12 panels of metrics **about** the backend itself:

- Ingest rate (samples/sec and lines/sec)
- Ingest rejections by reason
- Query rate and latency
- Block and log storage state
- Compaction and retention progress

It reaches these metrics through the `obs-internals` datasource, which connects to a **separate Prometheus instance** that scrapes the backend's `/metrics` endpoint. This design is intentional: the telemetry system that monitors the backend does not use the same storage the backend uses to record workload metrics. If the metrics TSDB fails, the backend's self-observability signals still exist in the separate Prometheus and can diagnose the failure.

## Two Prometheus Datasources

Grafana has three datasources, and the distinction matters:

| Datasource | UID | Backend URL | Stores | Grafana default |
|---|---|---|---|---|
| **observability-platform** | `obs-prometheus` | `http://backend:8080` | Demo workload metrics (load generator, sample app) | Yes |
| **observability-platform-internals** | `obs-internals` | `http://prometheus:9090` (Compose) or `http://observability-prometheus:9090` (K8s) | Backend's own metrics (ingestion, queries, storage, compaction) | No |
| **observability-platform-logs** | `obs-loki` | `http://backend:8080` | Demo workload logs | No |

The self-observability dashboard always uses `obs-internals`. A panel on the wrong datasource returns "No data" while testing healthy.

## Local Stack (Docker Compose)

### Start the Stack

```bash
make local-up
```

This starts:
- Backend on `:8080`
- Prometheus (internals scraper) on `:9090`
- Grafana on `:3000`
- Load generator and sample app

Wait for all services healthy (usually ~10 seconds).

### View the Dashboard

1. Open Grafana: `http://localhost:3000`
2. Log in: admin / admin
3. Click **Dashboards** (sidebar)
4. Open **Observability Platform Internals**

### First Health Check

Both Prometheus instances expose an `up` metric. In the Grafana Explore tab:

1. Select datasource **observability-platform-internals**
2. Run instant query: `up{job="observability-platform-backend"}`
3. Expected result: `{job="observability-platform-backend", service="observability-platform"}` = `1`

This confirms the backend's `/metrics` endpoint is reachable and its scrape succeeded.

You can also check the Prometheus internals UI directly: `http://localhost:9090` → Targets. The `observability-platform-backend` target should show State **UP** and Last Scrape Success < 15s ago.

### Stop the Stack

```bash
make local-down
```

### Reset Data

```bash
make local-reset
```

Removes all volumes (Prometheus internals data, Grafana provisioning state, and backend data).

## Kubernetes Deployment

### Prerequisites

- A Kubernetes cluster (local `kind` or cloud)
- `kubectl` context pointing to it
- Helm 3

### Install Charts in Order

The four charts below have a cross-chart contract: the grafana and prometheus charts both expect a backend Service the backend chart defines.

**1. Backend:**
```bash
helm install observability-backend deployments/helm/backend
```

**2. Prometheus (internals scraper):**
```bash
helm install observability-prometheus deployments/helm/prometheus
```

**3. Grafana dashboards ConfigMap** (required, done once per cluster):
```bash
kubectl create configmap grafana-dashboards \
  --from-file=observability/grafana/dashboards/
```

The Grafana chart mounts this ConfigMap non-optionally. Without this step, the Grafana pod stays in `ContainerCreating` state. The ConfigMap name defaults to `grafana-dashboards` and must match the chart's `dashboards.configMapName` value.

**4. Grafana:**
```bash
helm install observability-grafana deployments/helm/grafana \
  --set admin.password=<your-password>
```

The chart does not ship a default admin password; supply one with `--set admin.password=...` or use an existing Secret with `--set admin.existingSecret=<secret-name>` (the Secret must have an `admin-password` key).

**5. Producers (load generator and sample app):**
```bash
helm install observability-producers deployments/helm/producers
```

Wait for all pods to reach Running/Ready state:
```bash
kubectl get pods -w
```

### Access Grafana and Prometheus

Port-forward Grafana:
```bash
kubectl port-forward svc/observability-grafana 3000:3000
```

Then open `http://localhost:3000` (admin / admin).

Port-forward the internals Prometheus:
```bash
kubectl port-forward svc/observability-prometheus 9090:9090
```

Then open `http://localhost:9090` to check targets.

### First Health Check

In Grafana Explore (internals datasource), run:
```
up{job="observability-platform-backend"}
```

Expected: `1`.

### Retention and Data Loss

**Important:** The internals Prometheus uses an `emptyDir` volume and retains metrics for 24 hours only. When the pod is rescheduled (e.g., on a node failure), this telemetry is lost. For persistent platform telemetry in production, mount a persistent volume and extend retention in the Prometheus chart values.

### Uninstall

```bash
helm uninstall observability-backend observability-prometheus observability-grafana observability-producers
```

## Troubleshooting

### Panel Shows "No Data"

1. **Check the scrape target is reachable:**
   - Open Grafana Explore
   - Select datasource `obs-internals`
   - Run instant query: `up{job="observability-platform-backend"}`
   - If result is empty or 0, the backend's `/metrics` endpoint is unreachable
   - Check backend logs: `make local-logs` (Compose) or `kubectl logs pod/observability-backend-0` (Kubernetes)

2. **Check the metric exists at the source:**
   - Backend Compose: `curl http://localhost:8080/metrics | grep <metric_name>`
   - Backend Kubernetes: Port-forward the backend (`kubectl port-forward pod/observability-backend-0 8080:8080`), then `curl http://localhost:8080/metrics | grep <metric_name>`
   - If the metric is not listed, the corresponding collector has not been invoked yet or has failed

3. **Check the datasource UID:**
   - Open the panel's edit mode
   - Verify the query targets datasource UID `obs-internals` (not `obs-prometheus`)
   - A typo or mismatch silently returns no data

### All Panels Empty

The scrape target is unreachable or has never scraped successfully. Check the Prometheus (internals) UI:
- Compose: `http://localhost:9090` → **Targets** tab
- Kubernetes: Port-forward Prometheus, then `http://localhost:9090` → **Targets**

Look for the `observability-platform-backend` job. If State is RED or Last Scrape Error is recent, the backend is down or the endpoint path/port is wrong.

### Storage Panels Show Gaps Instead of Zero

If `obs_blocks_bytes`, `obs_log_chunk_bytes`, or WAL size panels show gaps (no data points), the collector read failed. This is by design — **a gap means the read failed, a zero means the size is truly zero.** Check `obs_collector_errors_total{collector="logs"}` or `collector="wal"` to confirm:

```
rate(obs_collector_errors_total[1m])
```

If it's climbing, the collector cannot read its directory:
- Compose: check backend file permissions in `data/logs/` and `data/metrics/wal/`
- Kubernetes: check PVC mount and pod logs

### Collector Error Climbing

If `obs_collector_errors_total` is climbing:
- **WAL collector (`collector="wal"`)**: Cannot read WAL segment files. Check filesystem permissions on `data/metrics/wal/`.
- **Logs collector (`collector="logs"`)**: Cannot read logs directory. Check permissions on `data/logs/` (Compose) or PVC mount (Kubernetes).

Both collectors emit a gap for that scrape and increment the error counter, never emitting a zero. This prevents a full-storage outage from appearing as "the metrics backend has zero storage" on the dashboard — the gap is the tell.

## Metrics Reference

All `obs_*` metrics exposed by the backend (scraped by the internals Prometheus):

**Cardinality:**
- `obs_active_series` — number of unique metric series in the backend's TSDB
- `obs_label_names_total` — number of unique label names across all series
- `obs_label_pairs_total` — number of unique label name=value pairs

**Storage:**
- `obs_blocks_total` — number of persisted metric blocks on disk
- `obs_blocks_bytes` — total size of metric blocks in bytes
- `obs_wal_bytes{wal="metrics"}` — size of metrics WAL segment files
- `obs_wal_segments{wal="metrics"}` — number of metrics WAL segment files
- `obs_log_streams_total` — number of distinct log streams
- `obs_log_chunks_total` — number of persisted log chunk files
- `obs_log_chunk_bytes` — total size of log chunks in bytes

**Ingestion:**
- `obs_samples_ingested_total` — total metric samples accepted
- `obs_samples_rejected_total{reason}` — samples rejected (reasons: `name`, `timestamp`, `value`, `labels`, `other`, `append`). The first five are validation errors (client sent invalid data); `append` means the data passed validation but the write to storage failed — a durability signal, not a client error.
- `obs_log_lines_ingested_total` — total log lines accepted
- `obs_log_lines_rejected_total{reason}` — log lines rejected (reasons: `values`, `timestamp`, `line`, `labels`, `other`, `append`). The first five are validation errors; `append` means the write to storage failed.

**Queries:**
- `obs_http_requests_total{route,method,status}` — HTTP requests by route pattern, method, and status
- `obs_http_request_duration_seconds{route,method}` — request duration histogram (quantiles computed in Grafana)

**Maintenance:**
- `obs_compactions_total` — block merges completed
- `obs_compaction_failures_total` — block merges that failed
- `obs_compaction_duration_seconds` — time spent in compaction (histogram)
- `obs_retention_deleted_blocks_total` — blocks deleted by retention policy
- `obs_flushes_total` — successful log/metrics head flushes
- `obs_flush_failures_total` — failed flushes

**Errors:**
- `obs_collector_errors_total{collector}` — scrape-time collector failures (values: "wal", "logs")

## See Also

- Backend design: `docs/planning/ARCHITECTURE_NOTES.md`
- Grafana demo: `docs/runbooks/grafana-demo.md`
- Kubernetes deployment: `docs/runbooks/kubernetes-demo.md`
