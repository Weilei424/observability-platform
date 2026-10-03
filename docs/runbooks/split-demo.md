# Split Demo

The same demo as [local-demo.md](local-demo.md), with the backend split into
its five components: gateway, ingester, querier, store, and compactor, with
the ingester run three times as a ring. Grafana,
Prometheus, the producers, every dashboard, and every URL are the same — the
gateway answers as `backend` — so this page covers only what differs, and a
failure drill that only the split topology can show. What each component does
is in [../architecture/components.md](../architecture/components.md).

## Start it

```bash
make local-up-split
```

`docker compose -f deployments/docker/docker-compose.split.yml ps` lists eleven
services: the gateway, `ingester-1` to `ingester-3`, the querier, the store, the
compactor, Prometheus, Grafana, and the two producers. The components have no
dependencies on one another: they start in any order, and each is healthy on
its own.

| Port | Service |
|---|---|
| `localhost:8080` | the gateway — the only component published |
| `localhost:3000` | Grafana (`admin` / `admin`) |
| `localhost:9090` | Prometheus, scraping every component, each ingester separately |

Check the whole path through the gateway:

```bash
curl -s http://localhost:8080/readyz
curl -sG 'http://localhost:8080/api/v1/query' --data-urlencode 'query=http_requests_total'
```

The gateway and the querier each log one `ring ready` line at startup:

```bash
docker compose -f deployments/docker/docker-compose.split.yml logs gateway querier | grep 'ring ready'
```

It carries `members=3` and `ring=<8 hex chars>`, a hash of the sorted member
list. The two hashes must be equal: if they differ, the gateway writes to an
ingester the querier does not read.

## What to look at

Open **Observability Platform Internals** in Grafana. **Component Health** plots
`up` for each component. The HTTP panels count only the gateway, so a request it
proxies to the querier is counted once. **Blocks and Log Chunks** is the store's;
**WAL Size** and **Active Series and Log Streams** (series) are the ingesters',
one line per ingester. The ring spreads series and streams over the three, so
each ingester's `obs_samples_ingested_total` is above zero and roughly equal:
the gateway's per-ingester write counter shows the same split.

```bash
curl -s http://localhost:8080/metrics | grep -E 'obs_ring_members|obs_gateway_ingester_requests_total'
```

Every log line names the process that wrote it:

```bash
make local-logs-split
```

and look for `"target":"ingester"` and the other targets beside `component`.

## Add or remove an ingester

Membership is static: the gateway and the querier read the ingester list once,
at startup. Both lists must be identical, and both must be restarted for a
change to take effect. The ring moves only the keys the changed member gains
or loses: adding a fourth ingester routes about a quarter of series and
streams to it. Their older data stays on the previous owner until it flushes,
and every read covers both.

### Compose

To add `ingester-4`:

1. In `deployments/docker/docker-compose.split.yml`, add an `ingester-4`
   service beside `ingester-3` (same `<<: *ingester` and `*ingester-env`, with
   its own `obs-ingester-4-data` volume, declared under `volumes:`), and add it
   to the producers' `depends_on`.
2. Append `,http://ingester-4:8080` to `OBS_INGESTER_URL` on **both** the
   gateway and the querier.
3. Add an `ingester-4:8080` target with `component: ingester` to
   `observability/prometheus/prometheus.split.yml`.
4. Start the new ingester first, then recreate the gateway and querier, which
   picks up their changed environment:

```bash
docker compose -f deployments/docker/docker-compose.split.yml up -d ingester-4
docker compose -f deployments/docker/docker-compose.split.yml up -d gateway querier
docker compose -f deployments/docker/docker-compose.split.yml restart prometheus
```

Check that both `ring ready` lines now say `members=4` with equal hashes.

To remove an ingester, **stop it first**: its shutdown runs a final flush into
the store, so nothing it holds is left behind. Only then take it out of the
lists.

```bash
docker compose -f deployments/docker/docker-compose.split.yml stop ingester-3
```

Then delete it from `OBS_INGESTER_URL` on both the gateway and the querier (and
from the Prometheus targets), and recreate them:

```bash
docker compose -f deployments/docker/docker-compose.split.yml up -d gateway querier
```

Until you do, reads answer `503`, because the querier still expects the
stopped ingester. If the final flush failed (the stop log says so, and
`obs_log_flush_failures_total` or `obs_flush_failures_total` counted it), what
it could not flush remains in that ingester's WAL on its volume and is not
read until the ingester rejoins the ring; start it again and let it flush
before removing it.

### Kubernetes (Helm)

```bash
helm upgrade backend deployments/helm/backend -n obs --set topology=split --set split.ingester.replicas=4 --wait
```

`split.ingester.replicas` (default 3, at least 1) sizes the ingester
StatefulSet and re-renders the list in both the gateway's and the querier's
ConfigMap from the pods' DNS names under the headless Service
(`<ingester>-<i>.<ingester>-headless`). Both Deployments carry a
`checksum/config` annotation of that ConfigMap file, so the changed list rolls
the gateway and the querier by itself. The Prometheus chart's
`split.targets.ingester` is a list that must match the replica count; update it
with the same number of entries:

```bash
helm upgrade prometheus deployments/helm/prometheus -n obs --set topology=split \
  --set 'split.targets.ingester={http://observability-ingester-0.observability-ingester-headless:8080,http://observability-ingester-1.observability-ingester-headless:8080,http://observability-ingester-2.observability-ingester-headless:8080,http://observability-ingester-3.observability-ingester-headless:8080}' --wait
```

To scale down, reduce the replica count the same way. Kubernetes stops the
highest-numbered pod, whose graceful shutdown runs the final flush, but the
gateway and querier roll to the shorter list only after the upgrade applies the
change: until they do, reads answer `503`. A pod's PVC is kept after it is
removed; scaling back up reattaches it.

## Failure drill: stop one ingester

```bash
docker compose -f deployments/docker/docker-compose.split.yml stop ingester-2
curl -s -w '\n%{http_code}\n' -G 'http://localhost:8080/api/v1/query' --data-urlencode 'query=http_requests_total'
```

The query answers `503`: a read covers every ingester, and the ring cannot
tell which one holds a series. Writes fail only where they must: a batch with a
series or stream `ingester-2` owns answers `503` `{"error":"ingester
unavailable"}`, and a batch whose keys all belong to the other two succeeds.
The gateway's counter shows where the failures went:

```bash
curl -s http://localhost:8080/metrics | grep obs_gateway_ingester_requests_total
```

`outcome="unavailable"` climbs for `ingester-2`'s `host:port`, while the other
two keep climbing under `outcome="ok"`. A client may retry the whole
batch: it rewrites the same values at the same timestamps, and reads collapse
duplicate log entries.

```bash
docker compose -f deployments/docker/docker-compose.split.yml start ingester-2
```

Reads and writes recover as soon as it answers healthy: the gateway and querier
call it per request.

## Failure drill: stop the store

```bash
docker compose -f deployments/docker/docker-compose.split.yml stop store
curl -s -w '\n%{http_code}\n' -G 'http://localhost:8080/api/v1/query' --data-urlencode 'query=http_requests_total'
```

The query answers `503` with `"errorType":"unavailable"` — it refuses rather
than answer from the ingester alone, which would silently omit everything
already flushed. The same applies to a Loki read:

```bash
curl -s -w '\n%{http_code}\n' -G 'http://localhost:8080/loki/api/v1/query_range' \
  --data-urlencode 'query={service="api"}'
```

Writes still land — the ingester does not need the store to accept a push:

```bash
curl -s -o /dev/null -w '%{http_code}\n' -X POST localhost:8080/api/v1/ingest/metrics \
  -H 'Content-Type: application/json' \
  -d "{\"metrics\":[{\"name\":\"http_requests_total\",\"labels\":{\"run\":\"split-drill\"},\"timestamp_ms\":$(date +%s%3N),\"value\":1}]}"
```

answers `204`. A current timestamp matters here: a `0` (1970) sample would still
prove the write landed, but it would also sit in `http_requests_total` looking
like a broken data point on every panel that reads that series afterward. The
producers keep going, and the ingester's WAL grows on **WAL Size** while
**Maintenance Failures** counts the flushes it cannot deliver. **Component
Health** shows `store` at 0.

```bash
docker compose -f deployments/docker/docker-compose.split.yml start store
curl -s -w '\n%{http_code}\n' -G 'http://localhost:8080/api/v1/query' --data-urlencode 'query=http_requests_total'
```

Reads recover as soon as the store answers healthy again — the querier calls
it directly per request, not on a scrape interval — and the ingester's next
flush drains what it held.

## Test it

```bash
make smoke-compose-split
```

The same Grafana-level checks as `make smoke-compose`, plus the split-only
ones: every ingester's ingested counter above zero after seeding, a flushed
marker read back by value after an ingester restarts, a block on the store,
`target` on every component's logs, the one-ingester outage drill (a write it
owns answers `503`, and so does a read, until it returns), and the store-outage
drill above.

## Kubernetes

The backend chart's `topology: split` deploys the same five components, with
three ingesters by default; the
gateway takes the Service name `observability-backend`, so the grafana and
producers charts install unchanged. Follow
[kubernetes-demo.md](kubernetes-demo.md), replacing its "1. Backend" and
"2. Prometheus" install commands with these two (same release names, `backend`
and `prometheus`, plus `--set topology=split`):

```bash
helm install backend deployments/helm/backend -n obs --set topology=split --wait
helm install prometheus deployments/helm/prometheus -n obs --set topology=split --wait
```

and wait for five workloads instead of one (the ingester's StatefulSet has three pods) (`helm install` prints the rollout
commands). `make smoke-kind-split` runs the cluster test against this topology;
like `make smoke-kind` it needs a cgroup v2 host.

## Stop, and reset

```bash
make local-down-split    # stop, KEEP the ingester and store volumes
make local-reset-split   # stop and DELETE every ingester volume and the store, Grafana, and Prometheus volumes
```

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Queries answer `503 unavailable` | an ingester or the store is down | `docker compose -f deployments/docker/docker-compose.split.yml ps`, then start the one that is not running |
| Queries answer `503 unavailable` right after an ingester was added or removed | the querier and gateway still hold the old list | recreate both with the same list and compare their `ring ready` hashes |
| Writes answer `503` at the gateway | an ingester is down | `obs_gateway_ingester_requests_total{outcome="unavailable"}` names it by `host:port`; start it. Batches that do not touch its keys still succeed |
| A sample was accepted but never shows up in a query | the gateway's and querier's ingester lists differ | compare the `ring=` hashes in their `ring ready` lines; fix the lists and recreate both |
| A component exits at startup naming a peer URL | its environment names a peer it does not use, or lacks one it needs | compare it with the Responsibilities table in [../architecture/components.md](../architecture/components.md) |
| `make local-up-split` fails, or Grafana/Prometheus/the gateway won't bind their ports | the all-in-one demo (`make local-up`) is already running and holds 3000/8080/9090 | run one demo at a time: `make local-down` first |
