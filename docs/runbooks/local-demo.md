# Local Demo

One pass through the whole platform on a laptop: ingest, storage, query, and all
four Grafana dashboards, plus a proof that data survives a restart.

Budget about ten minutes. Every other runbook in this directory assumes you have
done the **Start the stack** section below.

## What runs

`make local-up` starts five containers from
[`deployments/docker/docker-compose.yml`](../../deployments/docker/docker-compose.yml):

| Service | Role |
|---|---|
| `backend` | The observability platform itself |
| `prometheus` | Scrapes the backend's `/metrics`; stores telemetry *about* the backend |
| `grafana` | The UI, with datasources and dashboards provisioned at startup |
| `load-generator` | Emits `http_*` metrics |
| `sample-app` | Emits `sample_app_*` metrics and log streams |

## Prerequisites

- Docker with Compose v2 (`docker compose version` should print v2.x)
- Ports 3000, 8080, and 9090 free
- No Go toolchain needed for this path

## Start the stack

```bash
make local-up
```

| URL | What | Credentials |
|---|---|---|
| http://localhost:8080 | Backend API | none |
| http://localhost:3000 | Grafana | `admin` / `admin` (demo-only, set in the Compose file) |
| http://localhost:9090 | Prometheus, scraping the backend | none |

The producers are gated on the backend's health check, so they do not start
writing until it is ready. Expect data within about 15 seconds. **Empty panels in
the first few seconds are normal, not broken** — give it one refresh before
troubleshooting.

Check the backend is up:

```bash
curl -s http://localhost:8080/readyz
```

A `{"status":"ok"}` here means the data directory is writable, not just that the
port is open.

## Metrics dashboard

Grafana → Dashboards → **Observability Platform Metrics**. Request rate by
method, error rate, total RPS, request duration, and active connections should
all have data.

The same numbers through the API:

```bash
curl -sG 'http://localhost:8080/api/v1/query' \
  --data-urlencode 'query=sum(rate(http_requests_total[1m]))'
```

There is a second metrics dashboard, **Observability Platform Sample App**, whose
`sample_app_*` series are deliberately disjoint from the load generator's
`http_*` series so the two workloads never mix in one panel.

Panel-by-panel walkthrough: [grafana-demo.md](grafana-demo.md).

## Logs dashboard

Grafana → Dashboards → **Observability Platform Logs**, or use Explore with the
`observability-platform-logs` datasource.

```bash
curl -sG 'http://localhost:8080/loki/api/v1/query_range' \
  --data-urlencode 'query={service="sample-app"}' \
  --data-urlencode 'limit=5'
```

Explore workflow and the LogQL ladder: [grafana-logs-demo.md](grafana-logs-demo.md).

## Internals dashboard

Grafana → Dashboards → **Observability Platform Internals**. This is the platform
observing itself: ingestion rate, query latency, WAL size, block and chunk
counts, compaction progress, error counts.

It reads from the `observability-platform-internals` datasource, which points at
Prometheus — **not** at the backend. The backend's own TSDB holds what the
producers push; Prometheus holds telemetry about the backend. Telemetry that
shares storage with the workload it observes goes blind exactly when that storage
breaks.

```bash
curl -s 'http://localhost:8080/metrics' | grep -c '^obs_'
```

Full runbook and troubleshooting: [self-observability.md](self-observability.md).

## Prove the data is durable

This is the part worth doing slowly. Ingest a marker whose value is unique to this
run, restart the backend, and read the marker back **by value**:

```bash
# 1. Ingest a marker with a run-unique value.
MARKER=$RANDOM$RANDOM
NOW_MS=$(( $(date +%s) * 1000 ))
curl -sf -X POST localhost:8080/api/v1/ingest/metrics \
  -H 'Content-Type: application/json' \
  -d "{\"metrics\":[{\"name\":\"demo_restart_marker\",\"labels\":{\"run\":\"local\"},\"timestamp_ms\":$NOW_MS,\"value\":$MARKER}]}"

# 2. Restart only the backend.
docker compose -f deployments/docker/docker-compose.yml restart backend

# 3. Read it back BY VALUE. The same number means the WAL replayed it.
curl -sG localhost:8080/api/v1/query --data-urlencode 'query=demo_restart_marker' \
  | grep -q "$MARKER" \
  && echo "durable: marker $MARKER survived the restart" \
  || echo "LOST: marker $MARKER is gone"
```

**Why it is written this way.** Phase 5.1 shipped a restart check that queried a
live series the producers keep writing. Fresh post-restart samples satisfied it
whether or not a single pre-restart sample survived — it asserted persistence and
proved nothing. Comparing a value written *before* the restart is what makes this
a proof rather than a gesture.

## Run the smoke test

```bash
make smoke
```

Exercises the metrics and logs APIs against the running backend and prints
`Results: N passed, 0 failed`. A non-zero failure count names the endpoint that
failed and the response it returned.

## Stop, and reset

```bash
make local-down    # stop the containers, KEEP the data volumes
make local-reset   # stop and DELETE the data and Grafana volumes
```

`local-down` keeps volumes on purpose: data surviving a stack restart is the
durability story this project tells, so discarding it has to be a deliberate act.
Use `local-reset` when you want a clean first-run experience.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Panels empty for the first few seconds | Producers wait for the backend health check | Wait ~15s and refresh once |
| Panels still empty after a minute | Producers exited | `make local-logs` and look for `load-generator` / `sample-app` restarts |
| `port is already allocated` | 3000, 8080, or 9090 in use | Stop the other process, or change the host port in the Compose file |
| `docker compose: unknown command` | Compose v1 | Install Compose v2; the Makefile targets use `docker compose`, not `docker-compose` |
| Grafana shows "datasource not found" | Provisioning did not mount | `make local-reset && make local-up`; check the `grafana` volume mounts |
| Internals dashboard empty, others fine | Prometheus is not scraping | Open http://localhost:9090/targets and check the `backend` target is `UP` |

## See also

- [grafana-demo.md](grafana-demo.md) — metrics dashboard walkthrough
- [grafana-logs-demo.md](grafana-logs-demo.md) — logs and Explore workflow
- [self-observability.md](self-observability.md) — platform internals
- [kubernetes-demo.md](kubernetes-demo.md) — the same stack on Kubernetes
- [../api/README.md](../api/README.md) — API reference
