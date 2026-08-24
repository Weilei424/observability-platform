# Grafana Metrics Dashboard Demo

## Prerequisites

- Docker and Docker Compose installed
- Ports 8080 and 3000 free

## Start the stack

```bash
make local-up
```

This starts four services:
- **backend** on port 8080 — the observability backend
- **grafana** on port 3000 — Grafana with provisioned datasources and dashboards
- **load-generator** — continuously posts metrics to the backend
- **sample-app** — continuously pushes log streams and its own `sample_app_*` metrics

## Wait for data

Allow ~15 seconds for the load generator to emit enough samples for rate() calculations to produce non-zero results.

## Open Grafana

1. Navigate to `http://localhost:3000`
2. Login: **admin / admin** (you may be prompted to change password — skip it)

## Verify the datasource

1. In the left sidebar, go to **Connections → Data sources**
2. Click **observability-platform**
3. Scroll down and click **Save & test**
4. Expected: green banner — "Successfully queried the Prometheus API."

## View the dashboard

1. In the left sidebar, go to **Dashboards**
2. Click **Observability Platform Metrics**
3. All five panels should show live data:
   - **Request Rate by Method** — lines for GET and POST
   - **Error Rate** — occasional spikes (~5% error rate)
   - **Total RPS** — current aggregate request rate
   - **Request Duration** — GET (faster) and POST (slower) latency lines
   - **Active Connections** — random walk between 1 and 50

## View the sample app dashboard

1. In the left sidebar, go to **Dashboards**
2. Click **Observability Platform Sample App**
3. Four panels should show live data:
   - **Request Rate by Method** — `sum by (method)(rate(sample_app_requests_total[1m]))`
   - **Error Rate** — `rate(sample_app_errors_total[1m])`, non-zero within a minute or two
   - **Request Duration** — the GET and POST latency gauges
   - **Active Workers** — a bounded random walk between 1 and 8

These series come from `examples/sample-app`, which emits them independently of the log
lines it pushes. The two signals describe the same fictional service without agreeing on
any particular event, so do not read a metric spike as the cause of a log line.

## Reset the demo

```bash
make local-reset   # stops the stack and deletes the data and Grafana volumes
make local-up      # start again from empty storage
```

`make local-down` stops the stack but keeps the volumes, so data survives a restart —
that persistence is the WAL and block storage doing their job.

## Run the API smoke test

With the stack still running:

```bash
make smoke
# or
BACKEND_ADDR=http://localhost:8080 bash tests/e2e/smoke.sh
```

Expected: all checks PASS, exit 0.

## Stop the stack

```bash
make local-down
```
