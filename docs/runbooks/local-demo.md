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
- `make` — every lifecycle command here is a Make target
- Ports 3000, 8080, and 9090 free
- `curl`
- Go, **only** for `make smoke` — that target also runs `go test ./tests/e2e/` to
  check the Grafana provisioning files. Without Go, run the two scripts directly
  (`bash tests/e2e/smoke.sh` and `bash tests/e2e/logs_smoke.sh`); they need only
  `curl` and hit the running backend. Everything else here is Docker-only.

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
  --data-urlencode 'query={service="api"}' \
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
demo_durability() {
  local COMPOSE="docker compose -f deployments/docker/docker-compose.yml"
  local MARKER=$RANDOM$RANDOM
  local NOW_MS=$(( $(date +%s) * 1000 ))
  local CID BEFORE AFTER RESPONSE

  # 1. Ingest a marker with a run-unique value. If it was never stored, there is
  #    nothing to prove either way.
  curl -sf -X POST localhost:8080/api/v1/ingest/metrics \
    -H 'Content-Type: application/json' \
    -d "{\"metrics\":[{\"name\":\"demo_restart_marker\",\"labels\":{\"run\":\"local\"},\"timestamp_ms\":$NOW_MS,\"value\":$MARKER}]}" \
    >/dev/null || { echo "INCONCLUSIVE: the backend did not accept the marker."; return 1; }
  echo "ingested marker $MARKER"

  # 2. Note which container is serving, and when it last started.
  CID=$($COMPOSE ps -q backend) || { echo "INCONCLUSIVE: could not list the backend container."; return 1; }
  [ -n "$CID" ] || { echo "INCONCLUSIVE: no backend container is running."; return 1; }
  BEFORE=$(docker inspect -f '{{.State.StartedAt}}' "$CID") \
    || { echo "INCONCLUSIVE: could not read the container's start time."; return 1; }
  [ -n "$BEFORE" ] || { echo "INCONCLUSIVE: the container's start time came back empty."; return 1; }

  # 3. Restart it — and prove it restarted. An unchecked restart that fails
  #    leaves the original process running with the marker still in memory, and
  #    every step below would then pass while proving nothing at all.
  $COMPOSE restart backend || { echo "INCONCLUSIVE: the restart command failed."; return 1; }
  AFTER=$(docker inspect -f '{{.State.StartedAt}}' "$CID") \
    || { echo "INCONCLUSIVE: could not re-read the container's start time."; return 1; }
  [ -n "$AFTER" ] || { echo "INCONCLUSIVE: the container's start time came back empty after the restart."; return 1; }
  # Both values are known non-empty here, so this compares two real timestamps.
  # An unguarded assignment that failed would leave one empty, and "" != "<time>"
  # would read as a successful restart.
  [ "$BEFORE" != "$AFTER" ] || { echo "INCONCLUSIVE: the container's start time did not change, so it never restarted."; return 1; }

  # 4. Wait for it to come back. The container is up well before the process is:
  #    WAL replay runs first, and /readyz answers only once the data directory is
  #    writable again.
  for i in $(seq 1 60); do
    curl -sf localhost:8080/readyz >/dev/null 2>&1 && break
    sleep 1
  done

  # 5. Read the marker back BY VALUE.
  RESPONSE=$(curl -sf -G localhost:8080/api/v1/query \
    --data-urlencode 'query=demo_restart_marker' 2>/dev/null) \
    || { echo "INCONCLUSIVE: the backend did not answer. This is not a data-loss result."; return 1; }

  #    Compare the sample VALUE, not the whole body. A bare `grep -q "$MARKER"`
  #    matches anywhere in the JSON, and the response carries each sample's own
  #    timestamp as a bare number — so a short $RANDOM$RANDOM marker such as
  #    17897 matches the timestamp 1789700100 and reports "durable" for a marker
  #    that was never read back at all. A Prometheus vector renders every sample
  #    as "value":[<ts>,"<value>"], so anchoring the marker as the quoted second
  #    element is what makes this a comparison of the value.
  if echo "$RESPONSE" | grep -Eq "\"value\":\[[0-9.]+,\"$MARKER\"\]"; then
    echo "durable: marker $MARKER survived a real restart"
    return 0
  fi
  echo "LOST: the backend answered, but $MARKER is not the value of any"
  echo "      demo_restart_marker sample. Response: $RESPONSE"
  return 1
}

demo_durability
```

`LOST` is printed in exactly one situation: the marker was accepted, the
container demonstrably restarted, the backend answered, and the value was not
there. Every other outcome is **inconclusive**, including the one that matters
most — a restart that did not happen.

**Every path also sets the exit status**, so `demo_durability && echo ok` is safe
to put in a script: `0` only for a proven durable read, `1` for `LOST` and for
each `INCONCLUSIVE` branch. An earlier version of this function printed `LOST`
and then returned the `echo`'s own status, which is `0` — a data-loss result
that any caller read as a pass. Phase 5.2 found this exact hole in
`kind_smoke.sh`: the `kubectl delete pod` exit status was unchecked, and because
a StatefulSet pod keeps its name across a reschedule, a delete that never
happened would have left every later check passing. Comparing the container's
start time before and after is the same fix, which is why step 3 is not just
`restart`.

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
| `make smoke-compose` exits with `port 3000 is already serving` | something else holds the port — `make local-up`, or a stack a previous `OBS_COMPOSE_KEEP_UP=1` run left behind (each run uses its own `obs-compose-e2e-<id>` project, so kept stacks accumulate under distinct names) | `make local-down`, or find the kept one with `docker compose ls \| grep obs-compose-e2e` and `docker compose -p <that-project> down -v` |
| `make smoke-compose` exits with `already has containers or volumes` | a project of this run's exact name somehow already exists — the name carries a per-run id, so this should be unreachable; the test will not run `down -v` on a stack it did not create, because that deletes named volumes | remove **that** project with the `docker compose -p <project> ... down -v` command the message prints, or pick another prefix with `OBS_COMPOSE_PROJECT`. Re-running does not help: each run derives a new name, so a rerun would act on a different, empty project |
| `make smoke-compose` exits with `could not list containers` or `could not list volumes` | Docker could not answer, so the test cannot tell whether a stack is already there and refuses to guess | start Docker, then run `docker compose ls` yourself to confirm it answers |

## See also

- [grafana-demo.md](grafana-demo.md) — metrics dashboard walkthrough
- [grafana-logs-demo.md](grafana-logs-demo.md) — logs and Explore workflow
- [self-observability.md](self-observability.md) — platform internals
- [kubernetes-demo.md](kubernetes-demo.md) — the same stack on Kubernetes
- [../api/README.md](../api/README.md) — API reference
