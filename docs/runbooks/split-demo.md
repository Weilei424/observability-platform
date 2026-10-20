# Split Demo

The same demo as [local-demo.md](local-demo.md), with the backend split into
its five components: gateway, ingester, querier, store, and compactor, with
the ingester run three times as a ring and the store four times as a second
ring. Grafana,
Prometheus, the producers, every dashboard, and every URL are the same — the
gateway answers as `backend` — so this page covers only what differs, and a
failure drill that only the split topology can show. What each component does
is in [../architecture/components.md](../architecture/components.md).

## Start it

```bash
make local-up-split
```

`docker compose -f deployments/docker/docker-compose.split.yml ps` lists
fourteen services: the gateway, `ingester-1` to `ingester-3`, the querier,
`store-1` to `store-4`, the compactor, Prometheus, Grafana, and the two
producers. The components have no
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

The stores form a second ring. Compose sets `OBS_STORE_URL` (all four) and
`OBS_STORE_REPLICATION_FACTOR: "3"` once, in the `x-store-ring-env` anchor the
ingesters and querier share, so every flushed series and stream goes to three
of the four stores and a flush counts once two of them hold it; the
compactor lists the same four stores on its own. Each ingester logs it:

```bash
docker compose -f deployments/docker/docker-compose.split.yml logs ingester-1 | grep 'store ring ready'
```

with `stores=4` and `store_replication_factor=3`.

## What to look at

Open **Observability Platform Internals** in Grafana. **Component Health** plots
`up` for each component. The HTTP panels count only the gateway, so a request it
proxies to the querier is counted once. **Blocks and Log Chunks** is the stores',
one line per store;
**WAL Size** and **Active Series and Log Streams** (series) are the ingesters',
one line per ingester. The ring spreads series and streams over the three, so
each ingester's `obs_samples_ingested_total` is above zero and, at replication
factor 3, roughly equal, since every ingester holds a copy: the gateway's
per-ingester write counter shows the same. **Store flush quorum**, **Store
reads**, and **Store retry queue** show the store ring: flush batches `full`,
`degraded`, or `failed`, each store's reads by outcome, and the copies waiting
for a store that missed them.

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
**raise** it on the gateway first, then drain every ingester, one at a time,
restarting each after its `200`, and only then on the querier (a flush
interval is not enough: it leaves open chunks and small log lines on their one
ingester); **lower** it on the
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
to the stores, and answers `200` only once all of it is on a quorum of its
stores
([../api/internal.md](../api/internal.md)). From the first drain call until it
restarts, the ingester refuses every write with `503`, so drain it only after
the gateway has stopped writing to it. A graceful stop runs the same drain, but
a stop's outcome is only a log line; the route's `200` is what you wait for. While a stopped ingester is still on
the querier's list, a read at replication factor 2 or more skips it (its
drained head is in the stores), so reads stay complete unless a second ingester
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
   `*ingester-env`, with `OBS_INGESTER_SELF_URL: http://ingester-4:8080` and
   its own `obs-ingester-4-data` volume, declared under `volumes:`), and add it
   to the producers' `depends_on`. Add an
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

   A `503` says why (most often two of a key's stores are unreachable); nothing is lost —
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
the Deployment whose list it changes (and the ingesters, one pod at a time, when it is the gateway's: they carry its list and RF) — the querier when `replicas` changes, the
gateway when `writeReplicas` does — with its old and new pods running side by
side for a while. The stores and the compactor do not restart; the ingesters roll (one pod at a
time) only when the gateway's list changes. On an upgrade the chart
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
gateway, querier and ingesters (the ingesters divide a tenant's limit by it), so changing the gateway's rolls the gateway and the ingesters
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

Raise it gateway first, keeping the querier at the old factor, then drain and
restart every ingester, one at a time, before the querier follows — the chart
cannot see the drains, so they are yours:

```bash
helm upgrade backend deployments/helm/backend -n obs --reuse-values \
  --set split.replicationFactor=3 --set split.querier.replicationFactor=2 --wait
for i in 0 1 2; do   # every ingester, one at a time
  kubectl port-forward -n obs pod/observability-ingester-$i 18080:8080 &
  sleep 2
  curl -s -X POST -w '\n%{http_code}\n' http://localhost:18080/internal/v1/drain   # repeat until 200
  kill %1
  kubectl delete pod -n obs observability-ingester-$i --wait=true   # a drained ingester takes writes again after a restart
  kubectl rollout status statefulset/observability-ingester -n obs
done
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

## Add or remove a store

The ingesters flush each series and stream to its R_s stores on the store
ring, and the querier reads every store, skipping up to W_s − 1 that are down
(one, at R_s 3). A store change is staged so the ingesters never flush to a
store the querier does not read:

- **Adding:** start the store, add it to the **querier**'s and the
  **compactor**'s lists, then to the **ingesters'**, restarting them one at a
  time. Keys whose replica set changes keep their older blocks on their
  previous stores; reads cover every store, and those copies stay until
  retention (compaction never merges across stores).
- **Removing:** drop it from the **ingesters'** lists, restarting them one at a
  time; hand it off (`POST /internal/v1/handoff`, repeated until `200`); then
  drop it from the **querier**'s and **compactor**'s lists and stop it.

A restarted ingester loses its retry queue: copies it was still holding for a
store that missed a flush are not delivered, and those keys stay on W_s of
their R_s stores. The handoff closes the leaving store's flush barrier for
good (every flush to it answers `503` until it restarts) and copies every
series and stream it holds to every owner on the remaining ring
([../api/internal.md](../api/internal.md#handoff-store-only)). List the
remaining stores spelled exactly as the ring lists them: each one checks the
name it is sent under (`421` otherwise, and the handoff answers `503`). A
target list that names the leaving store itself answers `400` when the store
has `OBS_STORE_SELF_URL`, as Compose and the chart set it; without one it
answers `503`, because the store's own barrier refuses the copy. Remove one
store per handoff, and keep `OBS_STORE_REPLICATION_FACTOR` at most the number
of stores that remain. Send that same value as the handoff's
`replication_factor`: the route accepts any value from 1 to the target count,
but a lower one gives keys fewer new owners and can leave a key with fewer than
W_s copies, so reads may be silently incomplete if that store is down. The
handoff has a fixed 10-minute deadline, every retry resends everything, and
the store holds all its log entries in memory while exporting, so a store with
very large data may never finish or may run out of memory
([../api/limitations.md](../api/limitations.md)). A failed handoff's `503` body
names the failing store.

### Compose

The ingesters and the querier take the store list and replication factor
from one `x-store-ring-env` anchor in
`deployments/docker/docker-compose.split.yml`; the compactor has its own
literal `OBS_STORE_URL`. As with an ingester change, recreate **one service at
a time** with `--no-deps`: a plain `up -d` after editing the anchor recreates
the ingesters and the querier together, in no order.

To add `store-5`:

1. Add a `store-5` service beside `store-4` (`<<: *store`, environment
   `<<: *store-env` with `OBS_STORE_SELF_URL: http://store-5:8080`, and its own
   `obs-store-5-data` volume, declared under `volumes:`), and a `store-5:8080`
   target with `component: store` in
   `observability/prometheus/prometheus.split.yml`. Start it:

   ```bash
   docker compose -f deployments/docker/docker-compose.split.yml up -d store-5
   ```

2. Append `,http://store-5:8080` to the anchor's `OBS_STORE_URL` and to the
   compactor's, then recreate the **querier** and the **compactor** only:

   ```bash
   docker compose -f deployments/docker/docker-compose.split.yml up -d --no-deps querier
   docker compose -f deployments/docker/docker-compose.split.yml up -d --no-deps compactor
   ```

3. Recreate the ingesters, one at a time, each healthy before the next:

   ```bash
   docker compose -f deployments/docker/docker-compose.split.yml up -d --no-deps ingester-1
   docker compose -f deployments/docker/docker-compose.split.yml up -d --no-deps ingester-2
   docker compose -f deployments/docker/docker-compose.split.yml up -d --no-deps ingester-3
   docker compose -f deployments/docker/docker-compose.split.yml restart prometheus
   ```

   Each ingester's `store ring ready` line now says `stores=5`.

To remove `store-4` from the four-store ring at R_s 3:

1. Delete `http://store-4:8080` from the anchor's `OBS_STORE_URL` and recreate
   the **ingesters** only, one at a time, as in step 3 above. They stop
   flushing to `store-4`; the querier and compactor still read it.
2. Hand it off, from a container on the Compose network, and repeat until it
   answers `200`:

   ```bash
   docker run --rm --network observability-platform-split_default curlimages/curl \
     -s -X POST -w '\n%{http_code}\n' -H 'Content-Type: application/json' \
     -d '{"stores":["http://store-1:8080","http://store-2:8080","http://store-3:8080"],"replication_factor":3}' \
     http://store-4:8080/internal/v1/handoff
   ```

   The answer counts the series and streams copied. A `503` names the error
   (most often a target store down); a retry resends everything, and the
   duplicates collapse on read and at compaction.
3. Delete `store-4` from the compactor's `OBS_STORE_URL`, recreate the
   **querier** and the **compactor** (`up -d --no-deps` each), then stop it:

   ```bash
   docker compose -f deployments/docker/docker-compose.split.yml up -d --no-deps querier
   docker compose -f deployments/docker/docker-compose.split.yml up -d --no-deps compactor
   docker compose -f deployments/docker/docker-compose.split.yml stop store-4
   ```

4. Delete the `store-4` service and its Prometheus target from the files, or
   the next `up -d` starts it again. Its volume stays until removed by hand
   (`docker volume rm observability-platform-split_obs-store-4-data`).

### Kubernetes (Helm)

`split.store.replicas` (default 4) sizes the store StatefulSet; the querier
and compactor read every pod, by its DNS name under the headless Service
(`<store>-<i>.<store>-headless`). `split.store.writeReplicas` (default: all
replicas) is how many of them, from ordinal 0, the ingesters flush to, so the
ingesters' list is always a prefix of the querier's. Each store pod gets
`OBS_STORE_SELF_URL` from its pod name. On an upgrade the chart refuses
`split.store.replicas` below the stores the running ingesters flush to (the
`observability-platform.dev/store-count` annotation on their pod template),
and any store-count change while the ingester StatefulSet is still rolling
out. It does not refuse growing the stores and `writeReplicas` together:
adding is staged by convention, so set `writeReplicas` explicitly.

To add a fifth store, first the pod and the querier's and compactor's reads,
then the ingesters' flushes:

```bash
helm upgrade backend deployments/helm/backend -n obs --reuse-values \
  --set split.store.replicas=5 --set split.store.writeReplicas=4 --wait
kubectl rollout status statefulset/observability-store -n obs
helm upgrade backend deployments/helm/backend -n obs --reuse-values \
  --set split.store.writeReplicas=5 --wait
kubectl rollout status statefulset/observability-ingester -n obs
```

To remove one (`n` is the current store count), stop the ingesters' flushes
to it, hand it off and wait for its `200`, then shrink the StatefulSet and the
querier's and compactor's lists:

```bash
helm upgrade backend deployments/helm/backend -n obs --reuse-values \
  --set split.store.writeReplicas=<n-1> --wait
kubectl rollout status statefulset/observability-ingester -n obs
kubectl port-forward -n obs pod/observability-store-<n-1> 18080:8080 &
curl -s -X POST -w '\n%{http_code}\n' -H 'Content-Type: application/json' \
  -d '{"stores":["http://observability-store-0.observability-store-headless:8080","http://observability-store-1.observability-store-headless:8080","http://observability-store-2.observability-store-headless:8080"],"replication_factor":3}' \
  http://localhost:18080/internal/v1/handoff   # every store but the leaving one; repeat until 200
kill %1
helm upgrade backend deployments/helm/backend -n obs --reuse-values \
  --set split.store.replicas=<n-1> --wait
```

The chart cannot see the handoff: shrink only after its `200`, or the keys it
held lose a copy. `split.storeReplicationFactor` must stay at most the stores
the ingesters flush to; the chart refuses it otherwise. A removed pod's PVC is
kept; scaling back up reattaches it. Update the Prometheus chart's
`split.targets.store` list to the same pods after each change, as for the
ingesters below.

### Upgrading from 6.3

A 6.3 split ran one store, so everything it flushed is on that store alone,
and nothing copies it to the new ones. Until retention deletes those blocks
(log chunks it never deletes), an outage of that store gives reads that
answer `200` without the older data
([../api/limitations.md](../api/limitations.md)). In Helm that store is
`store-0`, whose PVC the new StatefulSet keeps. Stage the upgrade, so the
ingesters keep flushing to that one store while the other three start:

```bash
helm upgrade backend deployments/helm/backend -n obs --reuse-values \
  --set split.store.replicas=4,split.store.writeReplicas=1,split.storeReplicationFactor=1 --wait
kubectl rollout status statefulset/observability-store -n obs
helm upgrade backend deployments/helm/backend -n obs --reuse-values \
  --set split.store.writeReplicas=4,split.storeReplicationFactor=3 --wait
```

Until every store pod is ready, expect flush and read errors, and `421`s
from ingester and querier pods still running the old single store URL. The
second step does not re-replicate the old blocks either. Compose has no
in-place path: the 6.3 `store` service and its `obs-store-data` volume are
gone from the file, so the four stores start empty and the data flushed
before the upgrade is no longer read (the old volume stays until
`docker volume rm observability-platform-split_obs-store-data`; the old
container is reported as an orphan until `docker compose ... up -d --remove-orphans`).

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

## Failure drill: stop one store, then two

```bash
docker compose -f deployments/docker/docker-compose.split.yml stop store-1
curl -s -w '\n%{http_code}\n' -G 'http://localhost:8080/api/v1/query' --data-urlencode 'query=http_requests_total'
```

With four stores at R_s 3 the query answers `200` and is complete: the
querier skips up to W_s-1 = 1 store that fails with an outage, and every
flushed series is on at least two of its three stores. A Loki read is the
same:

```bash
curl -s -w '\n%{http_code}\n' -G 'http://localhost:8080/loki/api/v1/query_range' \
  --data-urlencode 'query={service="api"}'
```

Flushes keep landing too: each batch reaches two of its three stores, counts
as `degraded` on **Store flush quorum**, and the parts meant for `store-1`
wait in the ingesters' retry queue (**Store retry queue**,
`obs_ingester_store_retry_pending_bytes`). **Store reads** counts `store-1`'s
reads as `unavailable`, and the querier logs `read answered by replication`
naming `store store-1:8080`.

```bash
docker compose -f deployments/docker/docker-compose.split.yml stop store-2
curl -s -w '\n%{http_code}\n' -G 'http://localhost:8080/api/v1/query' --data-urlencode 'query=http_requests_total'
```

With two stores down the query answers `503` with `"errorType":"unavailable"`
— it refuses rather than answer without keys whose only readable copy may be
on a stopped store, which would silently omit data already flushed. Loki
reads answer `503` too. Writes still land — the ingester does not need a store
to accept a push:

```bash
curl -s -o /dev/null -w '%{http_code}\n' -X POST localhost:8080/api/v1/ingest/metrics \
  -H 'Content-Type: application/json' \
  -d "{\"metrics\":[{\"name\":\"http_requests_total\",\"labels\":{\"run\":\"split-drill\"},\"timestamp_ms\":$(date +%s%3N),\"value\":1}]}"
```

answers `204`. A current timestamp matters here: a `0` (1970) sample would still
prove the write landed, but it would also sit in `http_requests_total` looking
like a broken data point on every panel that reads that series afterward. The
producers keep going; a flush batch with a key on both stopped stores misses
its quorum (`failed` on **Store flush quorum**) and stays in the head, so the
ingesters' WALs grow on **WAL Size** while **Maintenance Failures** counts the
flushes they cannot deliver. **Component Health** shows both stores at 0.

```bash
docker compose -f deployments/docker/docker-compose.split.yml start store-1 store-2
curl -s -w '\n%{http_code}\n' -G 'http://localhost:8080/api/v1/query' --data-urlencode 'query=http_requests_total'
```

Reads recover as soon as enough stores answer healthy again — the querier
calls them directly per request, not on a scrape interval — the ingesters'
next flushes deliver what the heads held, and the retry queue sends the
stores the copies they missed, as long as the ingesters were not restarted
meanwhile (the queue is in memory) and no copy was older than 15 minutes.

## Test it

```bash
make smoke-compose-split
```

The same Grafana-level checks as `make smoke-compose`, plus the split-only
ones: every ingester's ingested counter above zero after seeding, a flushed
marker read back by value after an ingester restarts, a block on at least one store,
`target` on every component's logs, the replication check (a controlled batch
of marker series, read back from each ingester's own head over the internal
select route: every ingester must hold all of them, since at factor 3 each holds
every write), the ingester-outage drill (one ingester down: writes `204` and reads
complete `200`; two down: both `503`), and the store-outage drill above (one
store down: reads complete for metrics and logs and writes `204`; two down:
reads `503`, writes still `204`, and every value reads back once both return).

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

and wait for five workloads instead of one (the ingester's StatefulSet has three pods, the store's four) (`helm install` prints the rollout
commands). `make smoke-kind-split` runs the cluster test against this topology;
like `make smoke-kind` it needs a cgroup v2 host.

## Stop, and reset

```bash
make local-down-split    # stop, KEEP the ingester and store volumes
make local-reset-split   # stop and DELETE every ingester and store volume and the Grafana and Prometheus volumes
```

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Queries answer `503 unavailable` | more stores or ingesters are down than their factor tolerates (two of four stores at R_s 3, two of three ingesters at factor 3) | `docker compose -f deployments/docker/docker-compose.split.yml ps`, then start the ones that are not running |
| Queries answer `503 unavailable` right after an ingester was added or removed | the querier lists an ingester that is gone, beyond what the factor tolerates | recreate the querier (`--no-deps`) with the gateway's list and compare their `ring ready` hashes |
| Writes answer `503` at the gateway with `write quorum not met` | more ingesters are down than the factor tolerates | `obs_gateway_ingester_requests_total{outcome="unavailable"}` names them by `host:port`, as does the gateway's `write quorum not met` warn line; start them. At factor 3 on three ingesters every key is on every ingester, so two down fail every batch |
| A sample was accepted but never shows up in a query | the gateway's and querier's ingester lists differ, or the querier's quorum exceeds the gateway's | compare the `ring=` hashes and `quorum` values in their `ring ready` lines; fix the anchor and recreate them one at a time, in the staged order |
| Queries answer `503 unavailable` right after a store was added or removed | the querier lists a store that is gone, beyond what R_s tolerates | recreate the querier (`--no-deps`) from the anchor; for a removal, hand the store off before it leaves the querier's list ([above](#add-or-remove-a-store)) |
| `obs_ingester_store_retries_total{outcome="dropped"}` climbs | a store has been down longer than the retry queue keeps a copy (15 minutes, or 64 MiB per ingester) | start it; the dropped copies' keys stay on two of their three stores until retention |
| A component exits at startup naming a peer URL | its environment names a peer it does not use, or lacks one it needs | compare it with the Responsibilities table in [../architecture/components.md](../architecture/components.md) |
| `make local-up-split` fails, or Grafana/Prometheus/the gateway won't bind their ports | the all-in-one demo (`make local-up`) is already running and holds 3000/8080/9090 | run one demo at a time: `make local-down` first |
