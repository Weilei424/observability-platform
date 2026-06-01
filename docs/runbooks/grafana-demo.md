# Grafana Metrics Dashboard Demo

## Prerequisites

- Docker and Docker Compose installed
- Ports 8080 and 3000 free

## Start the stack

```bash
make local-up
```

This starts three services:
- **backend** on port 8080 — the observability backend
- **grafana** on port 3000 — Grafana with provisioned datasource and dashboard
- **load-generator** — continuously posts metrics to the backend

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
