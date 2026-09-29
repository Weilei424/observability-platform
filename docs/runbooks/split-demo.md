# Split Demo

The same demo as [local-demo.md](local-demo.md), with the backend split into
its five components: gateway, ingester, querier, store, and compactor. Grafana,
Prometheus, the producers, every dashboard, and every URL are the same — the
gateway answers as `backend` — so this page covers only what differs, and a
failure drill that only the split topology can show. What each component does
is in [../architecture/components.md](../architecture/components.md).

## Start it

```bash
make local-up-split
```

`docker compose -f deployments/docker/docker-compose.split.yml ps` lists nine
services. The five components have no dependencies on one another: they start
in any order, and each is healthy on its own.

| Port | Service |
|---|---|
| `localhost:8080` | the gateway — the only component published |
| `localhost:3000` | Grafana (`admin` / `admin`) |
| `localhost:9090` | Prometheus, scraping all five components |

Check the whole path through the gateway:

```bash
curl -s http://localhost:8080/readyz
curl -sG 'http://localhost:8080/api/v1/query' --data-urlencode 'query=http_requests_total'
```

## What to look at

Open **Observability Platform Internals** in Grafana. **Component Health** plots
`up` for each component. The HTTP panels count only the gateway, so a request it
proxies to the querier is counted once. **Blocks and Log Chunks** is the store's;
**WAL Size** and **Active Series and Log Streams** (series) are the ingester's.

Every log line names the process that wrote it:

```bash
make local-logs-split
```

and look for `"target":"ingester"` and the other targets beside `component`.

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
ones: a flushed marker read back by value after the ingester restarts, a block
on the store, `target` on every component's logs, and the store-outage drill
above.

## Kubernetes

The backend chart's `topology: split` deploys the same five components; the
gateway takes the Service name `observability-backend`, so the grafana and
producers charts install unchanged. Follow
[kubernetes-demo.md](kubernetes-demo.md), replacing its "1. Backend" and
"2. Prometheus" install commands with these two (same release names, `backend`
and `prometheus`, plus `--set topology=split`):

```bash
helm install backend deployments/helm/backend -n obs --set topology=split --wait
helm install prometheus deployments/helm/prometheus -n obs --set topology=split --wait
```

and wait for five workloads instead of one (`helm install` prints the rollout
commands). `make smoke-kind-split` runs the cluster test against this topology;
like `make smoke-kind` it needs a cgroup v2 host.

## Stop, and reset

```bash
make local-down-split    # stop, KEEP the ingester and store volumes
make local-reset-split   # stop and DELETE the ingester, store, Grafana, and Prometheus volumes
```

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Queries answer `503 unavailable` | the ingester or store is down | `docker compose -f deployments/docker/docker-compose.split.yml ps`, then start the one that is not running |
| Writes answer `503` at the gateway | the ingester is down | start it; writes need only the ingester |
| A component exits at startup naming a peer URL | its environment names a peer it does not use, or lacks one it needs | compare it with the Responsibilities table in [../architecture/components.md](../architecture/components.md) |
| `make local-up-split` fails, or Grafana/Prometheus/the gateway won't bind their ports | the all-in-one demo (`make local-up`) is already running and holds 3000/8080/9090 | run one demo at a time: `make local-down` first |
