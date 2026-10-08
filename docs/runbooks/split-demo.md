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

It carries `members=3`, `ring=<8 hex chars>` (a hash of the sorted member
list), `replication_factor=3`, and `quorum=2`. The two hashes must be equal: if
they differ, the gateway writes to an ingester the querier does not read. The
two `replication_factor` values must be equal too, except while an RF change is
staged ([below](#add-or-remove-an-ingester)). Compose sets
`OBS_REPLICATION_FACTOR: "3"` once, in the `x-ring-env` anchor the gateway and
querier share, so every series and stream is written to all three ingesters and
a write is acknowledged once two of them hold it. Each ingester's
`obs_samples_ingested_total` is therefore about equal, not a third of the
total.

## What to look at

Open **Observability Platform Internals** in Grafana. **Component Health** plots
`up` for each component. The HTTP panels count only the gateway, so a request it
proxies to the querier is counted once. **Blocks and Log Chunks** is the store's;
**WAL Size** and **Active Series and Log Streams** (series) are the ingesters',
one line per ingester. The ring spreads series and streams over the three, so
each ingester's `obs_samples_ingested_total` is above zero and, at replication
factor 3, roughly equal, since every ingester holds a copy: the gateway's
per-ingester write counter shows the same.

```bash
curl -s http://localhost:8080/metrics | grep -E 'obs_ring_members|obs_gateway_ingester_requests_total'
```

Every log line names the process that wrote it:

```bash
make local-logs-split
```

and look for `"target":"ingester"` and the other targets beside `component`.

## Add or remove an ingester

With replication factor 3 on three ingesters, removing one first needs the
factor lowered (the gateway and querier refuse a factor above their list) or a
fourth ingester added. An RF change is staged like a membership change, so a
read never skips more ingesters than a write's acknowledgements cover:
**raise** it on the gateway first, and on the querier only once the heads have
flushed (the maintenance flush interval, or a drain); **lower** it on the
querier first, then on the gateway. See
[Replication](../architecture/components.md#replication-63).

Membership is static: the gateway and the querier read the ingester list once,
at startup, so a change takes a restart of each. The ring moves only the keys
the changed member gains or loses: adding a fourth ingester routes about a
quarter of series and streams to it. Their older data stays on the previous
owner until it flushes, and every read covers both.

Order the two restarts so the gateway never writes to an ingester the querier
does not read — otherwise a read can answer `200` with acknowledged writes
missing:

- **Adding:** start the ingester, move the **querier** to the longer list,
  then the **gateway**.
- **Removing:** move the **gateway** to the shorter list, stop the ingester,
  then move the **querier**.

Before a removed ingester leaves the querier's list, drain it and wait for the
acknowledgment: `POST /internal/v1/drain` stops the ingester taking writes,
seals and flushes its whole head — every metrics chunk and buffered log line —
into the store, and answers `200` only once all of it is there
([../api/internal.md](../api/internal.md)). From the first drain call until it
restarts, the ingester refuses every write with `503`, so drain it only after
the gateway has stopped writing to it. A graceful stop runs the same drain, but
a stop's outcome is only a log line; the route's `200` is what you wait for. While a stopped ingester is still on
the querier's list, a read at replication factor 2 or more skips it (its
drained head is in the store), so reads stay complete unless a second ingester
is also down; at factor 1 reads answer `503` — they fail closed, never
incomplete.

### Compose

The gateway and the querier take their ingester list and replication factor
from one `x-ring-env` anchor in `deployments/docker/docker-compose.split.yml`,
so the two cannot drift at rest. To stage a change, edit the anchor, then
recreate **one service at a time** with `--no-deps`, in the order below. A
plain `docker compose up -d` after editing the anchor recreates the gateway and
the querier together, in no order, which is exactly the overlap the staging
avoids.

To add `ingester-4`:

1. Add an `ingester-4` service beside `ingester-3` (same `<<: *ingester` and
   `*ingester-env`, with its own `obs-ingester-4-data` volume, declared under
   `volumes:`), and add it to the producers' `depends_on`. Add an
   `ingester-4:8080` target with `component: ingester` to
   `observability/prometheus/prometheus.split.yml`.
2. Start it, then append `,http://ingester-4:8080` to the anchor's
   `OBS_INGESTER_URL` and recreate the **querier** only:

   ```bash
   docker compose -f deployments/docker/docker-compose.split.yml up -d ingester-4
   docker compose -f deployments/docker/docker-compose.split.yml up -d --no-deps querier
   ```

3. Recreate the **gateway**, which picks up the same list:

   ```bash
   docker compose -f deployments/docker/docker-compose.split.yml up -d --no-deps gateway
   docker compose -f deployments/docker/docker-compose.split.yml restart prometheus
   ```

Between steps 2 and 3 the running gateway still holds the old list, on
purpose. Once both are recreated, check that the gateway's and the querier's
`ring ready` lines say `members=4` with equal hashes.

To remove `ingester-3` from the three-ingester ring at replication factor 3:

1. Lower the factor first, querier then gateway: set the anchor's
   `OBS_REPLICATION_FACTOR` to `"2"` and

   ```bash
   docker compose -f deployments/docker/docker-compose.split.yml up -d --no-deps querier
   docker compose -f deployments/docker/docker-compose.split.yml up -d --no-deps gateway
   ```

   Both `ring ready` lines now say `replication_factor=2` and `quorum=2`.
2. Delete `http://ingester-3:8080` from the anchor's `OBS_INGESTER_URL` and
   recreate the **gateway** only (`up -d --no-deps gateway`). It stops
   writing to `ingester-3`; the querier still reads it.
3. Drain it. The internal API is not published, so call it from a container
   on the Compose network, and repeat until it answers `200`:

   ```bash
   docker run --rm --network observability-platform-split_default curlimages/curl \
     -s -X POST -w '\n%{http_code}\n' http://ingester-3:8080/internal/v1/drain
   ```

   A `503` says why (most often the store is unreachable); nothing is lost —
   what it could not flush is still in its WAL.
4. Stop it, then recreate the **querier** (and remove `ingester-3` from the
   Prometheus targets). Between the two, the querier at factor 2 skips the
   stopped ingester and reads stay complete.

   ```bash
   docker compose -f deployments/docker/docker-compose.split.yml stop ingester-3
   docker compose -f deployments/docker/docker-compose.split.yml up -d --no-deps querier
   ```

5. Delete the `ingester-3` service and its `depends_on` entries from the file,
   or the next `up -d` starts it again.

### Kubernetes (Helm)

`split.ingester.replicas` (default 3, at least 1) sizes the ingester
StatefulSet; the querier reads every replica, by the pods' DNS names under the
headless Service (`<ingester>-<i>.<ingester>-headless`).
`split.ingester.writeReplicas` (default: all replicas; between 1 and
`replicas`) is how many of them, from ordinal 0, the gateway writes to, so the
gateway's list is always a prefix of the querier's. Each workload carries a
`checksum/config` annotation over its own ConfigMap only, so a stage rolls just
the Deployment whose list it changes — the querier when `replicas` changes, the
gateway when `writeReplicas` does — with its old and new pods running side by
side for a while. Existing ingester pods, the store, and the compactor do not
restart. On an upgrade the chart
reads the ingester counts the running gateway and querier pods loaded (an
annotation on each Deployment's pod template) and refuses a change that is not
staged — a new gateway writing to an ingester the old querier does not read,
or a new querier dropping one the old gateway writes to — and the failure
names the next safe step. It also refuses any ring change while either
Deployment is still rolling out, since its old pods may hold an older list:
after a failed or stuck upgrade, wait for `kubectl rollout status` or
`helm rollback` first. (`helm template` cannot read the live release; set both
`split.ingester.previous.replicas` and `.writeReplicas` to preview the check.
A real upgrade refuses them.)

`split.replicationFactor` (default 3, at least 1) is how many ingesters hold each
series and stream; the chart renders it as `OBS_REPLICATION_FACTOR` on the
gateway and querier only, so changing it rolls just those two
(`config.OBS_REPLICATION_FACTOR` is chart-owned and refused). The chart refuses
a factor above `writeReplicas`, the ingesters the gateway writes to. Because
of that refusal, removing an ingester from a three-ingester ring at factor 3
needs the factor lowered first or a fourth ingester added; `helm upgrade` fails
naming `split.replicationFactor` otherwise.

An RF change is staged with `split.querier.replicationFactor` (unset, the
querier runs `split.replicationFactor`). Each gateway and querier pod template
records the factor it loaded (`observability-platform.dev/replication-factor`),
and the chart refuses a querier quorum above the gateway's, a new gateway
quorum below the running querier's, a new querier quorum above the running
gateway's, and any RF change while either Deployment is still rolling out.
Lower it querier first, then the gateway:

```bash
helm upgrade backend deployments/helm/backend -n obs --reuse-values \
  --set split.querier.replicationFactor=2 --wait
helm upgrade backend deployments/helm/backend -n obs --reuse-values \
  --set split.replicationFactor=2 --set split.querier.replicationFactor=null --wait
```

Raise it gateway first, keeping the querier at the old factor, and let the
heads flush (the maintenance flush interval, or a drain) before the querier
follows — the chart cannot see the flush, so that wait is yours:

```bash
helm upgrade backend deployments/helm/backend -n obs --reuse-values \
  --set split.replicationFactor=3 --set split.querier.replicationFactor=2 --wait
# wait for the heads to flush
helm upgrade backend deployments/helm/backend -n obs --reuse-values \
  --set split.querier.replicationFactor=null --wait
```

To add a fourth ingester, first add the pod and the querier's read, then the
gateway's writes:

```bash
helm upgrade backend deployments/helm/backend -n obs --reuse-values \
  --set split.ingester.replicas=4 --set split.ingester.writeReplicas=3 --wait
helm upgrade backend deployments/helm/backend -n obs --reuse-values \
  --set split.ingester.writeReplicas=4 --wait
```

To remove one (`n` is the current replica count), stop the gateway's writes,
drain the pod and wait for its `200`, then scale it down and shrink the
querier's list:

```bash
helm upgrade backend deployments/helm/backend -n obs --reuse-values \
  --set split.ingester.writeReplicas=<n-1> --wait
kubectl port-forward -n obs pod/observability-ingester-<n-1> 18080:8080 &
curl -s -X POST -w '\n%{http_code}\n' http://localhost:18080/internal/v1/drain   # repeat until 200
kill %1
kubectl scale statefulset/observability-ingester -n obs --replicas=<n-1>
kubectl wait --for=delete pod/observability-ingester-<n-1> -n obs --timeout=120s
helm upgrade backend deployments/helm/backend -n obs --reuse-values \
  --set split.ingester.replicas=<n-1> --wait
```

Do not scale down before the drain answers `200`: a `503` means part of its
head is only in its WAL, and once the querier stops reading it that data is
hidden. Between the scale and the last upgrade, a read at factor 2 or more
skips the removed pod and stays complete; at factor 1 it answers `503`. A pod's PVC
is kept after it is removed; scaling back up reattaches it.

The Prometheus chart's `split.targets.ingester` is a list that must match the
replica count; update it after each change with the same number of entries:

```bash
helm upgrade prometheus deployments/helm/prometheus -n obs --set topology=split \
  --set 'split.targets.ingester={http://observability-ingester-0.observability-ingester-headless:8080,http://observability-ingester-1.observability-ingester-headless:8080,http://observability-ingester-2.observability-ingester-headless:8080,http://observability-ingester-3.observability-ingester-headless:8080}' --wait
```

## Failure drill: stop one ingester

```bash
docker compose -f deployments/docker/docker-compose.split.yml stop ingester-2
curl -s -w '\n%{http_code}\n' -G 'http://localhost:8080/api/v1/query' --data-urlencode 'query=http_requests_total'
```

With replication factor 3 the query answers `200` and is complete: the querier
skips up to W-1 = 1 ingester that fails with an outage, and every acknowledged
write is on at least two ingesters. Writes still answer `204`, because every
series has two other replicas. The gateway's counters show the outage was
absorbed:

```bash
curl -s http://localhost:8080/metrics | grep -E 'obs_gateway_ingester_requests_total|obs_gateway_write_quorum_total'
```

`outcome="unavailable"` climbs for `ingester-2`'s `host:port`, the other two
keep climbing under `outcome="ok"`, and `obs_gateway_write_quorum_total` counts
batches under `outcome="degraded"` (quorum met, one replica failed) rather than
`failed`. The querier logs `read answered by replication` at `warn` with the
skipped ingester, and `obs_querier_ingester_reads_total` counts it as
`unavailable`; the internals dashboard's "Write quorum" and "Ingester reads"
panels plot both. Stop a second ingester and both writes and reads answer
`503`; a failed write's body is `write quorum not met: <k> of <n> series could
not reach 2 of 3 ingesters`. A client may retry the whole batch: it rewrites
the same values at the same timestamps, and reads collapse duplicates.

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
`target` on every component's logs, the replication check (every ingester's
ingested counter within 80% of the largest, since at factor 3 each holds every
sample), the ingester-outage drill (one ingester down: writes `204` and reads
complete `200`; two down: both `503`), and the store-outage drill above.

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
| Queries answer `503 unavailable` | the store is down, or more ingesters are down than the factor tolerates (two of three at factor 3) | `docker compose -f deployments/docker/docker-compose.split.yml ps`, then start the ones that are not running |
| Queries answer `503 unavailable` right after an ingester was added or removed | the querier lists an ingester that is gone, beyond what the factor tolerates | recreate the querier (`--no-deps`) with the gateway's list and compare their `ring ready` hashes |
| Writes answer `503` at the gateway with `write quorum not met` | more ingesters are down than the factor tolerates | `obs_gateway_ingester_requests_total{outcome="unavailable"}` names them by `host:port`, as does the gateway's `write quorum not met` warn line; start them. At factor 3 on three ingesters every key is on every ingester, so two down fail every batch |
| A sample was accepted but never shows up in a query | the gateway's and querier's ingester lists differ, or the querier's quorum exceeds the gateway's | compare the `ring=` hashes and `quorum` values in their `ring ready` lines; fix the anchor and recreate them one at a time, in the staged order |
| A component exits at startup naming a peer URL | its environment names a peer it does not use, or lacks one it needs | compare it with the Responsibilities table in [../architecture/components.md](../architecture/components.md) |
| `make local-up-split` fails, or Grafana/Prometheus/the gateway won't bind their ports | the all-in-one demo (`make local-up`) is already running and holds 3000/8080/9090 | run one demo at a time: `make local-down` first |
