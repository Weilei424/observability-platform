# Helm Charts

Four charts, installed separately rather than as one umbrella chart, because they scale
and fail independently: the backend is stateful and singular, Grafana is stateless and
singular, the producers are optional demo traffic you might want to disable or scale
without touching either of the other two, and Prometheus scrapes the backend's own
`/metrics` so the self-observability dashboard has something to read in the Kubernetes
demo. See [`docs/runbooks/kubernetes-demo.md`](../../docs/runbooks/kubernetes-demo.md)
for the install walkthrough; this file is the values reference.

## Cross-chart contract

The grafana and producers charts each default `backend.url` to
`http://observability-backend:8080` — the literal Service name the backend chart creates
via its pinned `fullnameOverride: observability-backend`, not a name derived from
whatever release name you install it under. All three values files must agree: change
`fullnameOverride` in the backend chart, or `backend.url` in either of the other two, and
you must change all three or the deployment breaks silently — Grafana shows "no data"
and the producers log connection failures, with every chart still linting clean, because
Helm never checks a claim one chart makes about another.

`tests/e2e/helm_test.go`'s `TestCrossChartBackendURLResolves` is what actually enforces
this: it renders all three charts and fails if any `backend.url` doesn't resolve to a
Service name and port the backend chart's own rendered output defines.

The same pattern applies one level over: the grafana chart's `internals.url` defaults to
`http://observability-prometheus:9090`, a claim about the Service the **prometheus**
chart creates via its own pinned `fullnameOverride`. `TestGrafanaInternalsURLResolvesToThePrometheusService`
enforces that direction, and `TestPrometheusChartScrapesTheBackendService` enforces the
prometheus chart's own `backend.url` the same way `TestCrossChartBackendURLResolves` does
for grafana and producers — including checking that the rendered scrape ConfigMap
actually contains the resolved target, not just that the value parses.

## `backend` chart

`deployments/helm/backend/` — a StatefulSet, single replica, one PVC per pod via
`volumeClaimTemplates`. StatefulSet rather than Deployment because the backend owns a WAL
and on-disk chunks/blocks: a Deployment on a `ReadWriteOnce` volume deadlocks on rolling
update (the new pod waits forever for a volume the old pod still holds). One replica is a
deliberate ceiling, not an oversight — two replicas would each get a private PVC and a
private WAL, so a query would see whichever shard it landed on.

| Key | Default | Meaning |
|---|---|---|
| `fullnameOverride` | `observability-backend` | Pinned Service/StatefulSet name. The other two charts' `backend.url` depends on this exact value. At most 52 characters (a StatefulSet name limit); it is never truncated, so a longer value fails the render. In split mode the other components are named `<prefix>-<component>`, the prefix being this value without a trailing `-backend`, truncated to 43 characters; a value that would give a component the gateway's own name (43 characters, then `-store`) fails the split render. |
| `image.repository` | `observability-platform/backend` | Image name; built by the `backend` target in `deployments/docker/Dockerfile`. |
| `image.tag` | `dev` | Image tag. |
| `image.pullPolicy` | `IfNotPresent` | So a `kind load docker-image`ed image is used as-is with no registry. |
| `service.port` | `8080` | Backend HTTP port, exposed on both the ClusterIP and the headless Service, and the sole owner of the listen address: the ConfigMap derives `OBS_HTTP_ADDR` from it. Setting `config.OBS_HTTP_ADDR` is rejected at render time — it would emit the key twice and leave the server on a port the Services and probes do not use. |
| `persistence.size` | `2Gi` | Size of the per-pod PVC created from `volumeClaimTemplates`. |
| `persistence.storageClassName` | `""` (cluster default) | Set to pin a specific StorageClass; empty lets the cluster choose (`local-path` on kind). |
| `resources.requests.cpu` | `100m` | |
| `resources.requests.memory` | `128Mi` | |
| `resources.limits.memory` | `512Mi` | |
| `config.OBS_DATA_DIR` | `/data` | Must match the volume mount path; also read by `internal/config/config.go`. |
| `config.OBS_LOG_LEVEL` | `info` | |
| `config.OBS_RETENTION` | `0s` | Disabled by default, as in Compose. |
| `startupProbe.periodSeconds` | `5` | |
| `startupProbe.failureThreshold` | `30` | Startup budget = `periodSeconds * failureThreshold` = 150s, the time WAL replay is allowed to take before the pod is killed. |
| `topology` | `all-in-one` | `all-in-one` runs one StatefulSet. `split` runs the five components: Deployments for the gateway (which takes the Service name `observability-backend`), querier, and compactor; StatefulSets with their own PVCs for the ingester and store |
| `split.<component>.replicas` | `1` | Gateway and querier may scale. The store and compactor must stay at 1 (the store until Phase 6.4); the chart refuses more |
| `split.ingester.replicas` | `3` | Ingester pods in the ring; at least 1, and the chart refuses less. The querier's `OBS_INGESTER_URL` lists every pod, one `http://<ingester>-<i>.<ingester>-headless:<port>` each. Each workload's `checksum/config` annotation hashes only its own ConfigMap, so a ring stage rolls only the gateway or querier whose list it changes; the ingester StatefulSet scales without restarting its pods, and the store and compactor are untouched. The prometheus chart's `split.targets.ingester` must list the same pods |
| `split.replicationFactor` | `3` | How many distinct ingesters every write goes to, and how many copies the querier dedups on a read. A write succeeds once every series or stream has W = RF/2 + 1 acknowledgements (2 of 3 at the default), and a read skips up to W−1 ingesters that fail with an outage, so reads and writes survive one ingester down at the default. The chart refuses a value above the ingesters the gateway writes to (`split.ingester.writeReplicas`, else `replicas`) and sets it on the gateway and querier only, so changing it rolls just those two; `config.OBS_REPLICATION_FACTOR` is refused. A change is staged with `split.querier.replicationFactor` (next row) |
| `split.querier.replicationFactor` | `split.replicationFactor` | The querier's RF, set only while staging an RF change; at least 1 and at most `split.ingester.replicas`. A read skips up to its quorum − 1 ingesters, so it stays complete only while the querier's quorum is at most the gateway's, and the chart refuses a querier quorum above the gateway's. Raise an RF gateway first (set the new `split.replicationFactor` with this at the old value), then remove this (`--set split.querier.replicationFactor=null` on a `--reuse-values` upgrade) once every ingester has been drained (`POST /internal/v1/drain` answers `200`) and restarted, one at a time — a flush interval is not enough, since the maintenance flush leaves open chunks and the logs head; lower it querier first (set this to the new value), then lower `split.replicationFactor` and remove this. Each gateway and querier pod template records its RF (`observability-platform.dev/replication-factor`); on an upgrade the chart refuses a gateway quorum below the running querier's and a querier quorum above the running gateway's, and counts an RF change as a ring change, refused while either Deployment is still rolling out. A Deployment without the annotation counts as RF 1, never as its ConfigMap says — an upgrade applies the ConfigMap before the pods roll, so after an interrupted upgrade the ConfigMap can claim an RF its pods never loaded. A Phase 6.2 release therefore counts as RF 1: upgrading one to the RF 3 default is refused until it is staged (first `--set split.querier.replicationFactor=1`, then remove it once every ingester has been drained and restarted). The release counts as live when any of it survives — either Deployment, either ConfigMap, the ingester StatefulSet, or kept ingester PVCs (`data-<ingester>-<i>`) — and a side whose RF cannot be read counts as RF 1, so a recovery after a deleted Deployment, or a reinstall over kept ingester PVCs, stages its RF the same way |
| `split.ingester.writeReplicas` | all replicas | How many pods, from ordinal 0, the gateway writes to; between 1 and `replicas`, and the chart refuses anything else. The gateway's list is always a prefix of the querier's, so it never writes to an ingester the querier does not read. Set it below `replicas` to stage a membership change: raise `replicas` first when adding, lower `writeReplicas` first when removing ([../../docs/runbooks/split-demo.md](../../docs/runbooks/split-demo.md)). On an upgrade the chart reads the counts the running gateway and querier pods loaded (their `observability-platform.dev/ingester-count` pod-template annotation; the ConfigMaps for a release from before it) and refuses a change that skips a stage, and any ring change while either Deployment is still rolling out |
| `split.ingester.previous.replicas`, `split.ingester.previous.writeReplicas` | unset | For previews only: `helm template` cannot read the live release, so these stand in for the counts it currently runs when checking that a change is staged. Give both or neither; a real upgrade refuses them. They carry no replication factor: a preview checks the ingester counts only, never an RF change's order |
| `split.<component>.resources` | see `values.yaml` | Per-component requests and limits |
| `split.ingester.persistence.size`, `split.store.persistence.size` | `1Gi`, `2Gi` | PVC sizes; `persistence.storageClassName` applies to both |

The ingester and store StatefulSets set `terminationGracePeriodSeconds: 60`,
fixed rather than a value the chart exposes: a graceful shutdown against a slow
peer can take up to its 50s shutdown budget (a 10s HTTP drain, then the ingester's drain of its head), and
Kubernetes would otherwise SIGKILL the pod mid-flush before it finishes. The
Compose split gives the same two services a 60s `stop_grace_period` for the
same reason; the chart's 60s adds headroom on top.

The gateway exports `obs_ring_members` and
`obs_gateway_ingester_requests_total{ingester,outcome}`, and the querier
exports `obs_ring_members`; the gateway and querier also log `ring ready` with
`members=N ring=<hash>` at startup, and equal hashes mean the same member set.
Membership changes are in
[../../docs/runbooks/split-demo.md](../../docs/runbooks/split-demo.md).

`OBS_TARGET`, `OBS_INGESTER_URL`, `OBS_STORE_URL`, and `OBS_QUERIER_URL` are
rendered by the chart from `topology` and are refused under `config` — setting
one there would emit the key twice and could contradict what the chart itself
derives.

Every `config.*` key must start with `OBS_` and correspond to a `v.SetDefault` in
`internal/config/config.go` — Viper silently ignores env vars it has no default for, so a
typo'd key would be invisible at runtime rather than an error.

That is enforced in two places, because they catch different mistakes. `values.schema.json`
lists the allowed keys and sets `additionalProperties: false`, so Helm rejects an unknown
key from **any** source — including an `--set config.OBS_LOG_LEVLE=debug` typed at install
time — before it renders anything:

```console
$ helm install backend deployments/helm/backend --set-string config.OBS_LOG_LEVLE=debug
Error: values don't meet the specifications of the schema(s) in the following chart(s):
observability-platform-backend:
- config: Additional property OBS_LOG_LEVLE is not allowed
```

And in `tests/e2e/helm_test.go`, `TestBackendConfigKeysAreReal` checks every key shipped in
`values.yaml` against `config.go`, while `TestBackendConfigOverridesAreValidated` checks the
schema's key list against `config.go` in both directions — so the schema cannot fall behind
a newly added backend option, and cannot allow one that no longer exists.

Probes use `httpGet` against `/readyz` (startup and readiness) and `/healthz`
(liveness) — not the `/server -healthcheck` exec mode the Compose healthcheck uses. The
kubelet performs an `httpGet` probe from outside the container itself, so the
distroless image's no-shell constraint that forces an exec probe in Compose does not
apply here. `/readyz` also proves the data directory is writable (it creates and removes
a temp file there); `/healthz` is a bare 200 so a full volume can't turn into a restart
loop through liveness.

## `grafana` chart

`deployments/helm/grafana/` — a stateless Deployment. Provisions three datasources:
`obs-prometheus` and `obs-loki` pointed at the backend, and `obs-internals` pointed at
the `prometheus` chart's Service (`internals.url`), plus a dashboard provider; the
dashboards themselves come from an operator-created ConfigMap, not from this chart.

| Key | Default | Meaning |
|---|---|---|
| `fullnameOverride` | `observability-grafana` | Pinned Service/Deployment name. |
| `image.repository` | `grafana/grafana` | Upstream Grafana image. |
| `image.tag` | `11.1.0` | Pinned to match the Compose demo's Grafana version. |
| `service.port` | `3000` | Grafana's HTTP port, and the sole owner of it: the Service, the `containerPort`, the readiness probe and the container's own `GF_SERVER_HTTP_PORT` are all derived from this one value. Overriding it used to move the first three and leave Grafana listening on 3000, so the chart installed and then failed readiness forever; `tests/e2e/helm_ports_test.go` now renders an off-default value and fails if the four disagree. |
| `backend.url` | `http://observability-backend:8080` | The backend Service the two backend datasources, `obs-prometheus` and `obs-loki`, point at. Must match the backend chart's `fullnameOverride` and `service.port` — see Cross-chart contract above. |
| `internals.url` | `http://observability-prometheus:9090` | The `prometheus` chart's Service that the `obs-internals` datasource points at — the self-observability dashboard's source. Like `backend.url`, a claim about a Service another chart owns: it must match the prometheus chart's `fullnameOverride` and `service.port`, and `tests/e2e/helm_test.go` fails if it does not. |
| `dashboards.configMapName` | `grafana-dashboards` | Name of the ConfigMap the operator creates with `kubectl create configmap grafana-dashboards --from-file=observability/grafana/dashboards/` before installing this chart. The volume mount is **not** `optional`, so a missing ConfigMap of this name leaves the pod in `ContainerCreating`, naming exactly what's absent, rather than starting Grafana with no dashboards. |
| `admin.user` | `admin` | |
| `admin.password` | `""` (none) | No default on purpose — `CLAUDE.md` forbids secrets in git. Rendering fails with a clear message unless this or `admin.existingSecret` is set. |
| `admin.existingSecret` | `""` (none) | Name of an operator-created Secret with an `admin-password` key, as an alternative to `admin.password`. |
| `resources.requests.cpu` | `100m` | |
| `resources.requests.memory` | `128Mi` | |
| `resources.limits.memory` | `512Mi` | |

Grafana reads its provisioned datasources, its dashboard provider, and
`GF_SECURITY_ADMIN_PASSWORD` **once, at startup**, so the pod template carries a
`checksum/` annotation for each chart-managed input. Without them a `helm upgrade` that
changes only the Secret or a ConfigMap leaves the pod template byte-identical, no new
ReplicaSet is created, and the running container keeps the old values — an upgrade that
reports success and changes nothing. Rotating `admin.password` therefore restarts Grafana;
rotating a Secret named by `admin.existingSecret` does not, because the chart does not
manage that object and cannot see its contents.

## `producers` chart

`deployments/helm/producers/` — two independent Deployments, the sample app and the load
generator, both reusing the `sampleapp`/`loadgen` Dockerfile stages unchanged and both
addressing the backend through `backend.url`. Without this chart, a fresh install renders
three empty dashboards.

| Key | Default | Meaning |
|---|---|---|
| `fullnameOverride` | `observability-producers` | Base name for both Deployments (`-sample-app` / `-load-generator` suffixes). |
| `backend.url` | `http://observability-backend:8080` | Same cross-chart claim as the grafana chart's `backend.url`; see Cross-chart contract above. |
| `sampleApp.enabled` | `true` | Set `false` to skip the sample app Deployment. |
| `sampleApp.replicas` | `1` | Pod count for the sample app Deployment. Safe above 1: each pod labels its series with `instance=<pod name>` (`OBS_INSTANCE`, from the downward API), so replicas do not share a series identity. Log streams are not split that way — they are appends, not counters. |
| `sampleApp.image.repository` | `observability-platform/sample-app` | Built by the `sampleapp` target. |
| `sampleApp.image.tag` | `dev` | |
| `sampleApp.rate` | `2` | Log batches per second. |
| `sampleApp.metricsRate` | `1` | Metric pushes per second — an independent ticker from `rate`. |
| `loadGenerator.enabled` | `true` | Set `false` to skip the load generator Deployment. |
| `loadGenerator.replicas` | `1` | Pod count for the load generator Deployment. Carries the same per-pod `instance` label as `sampleApp.replicas`. |
| `loadGenerator.image.repository` | `observability-platform/load-generator` | Built by the `loadgen` target. |
| `loadGenerator.image.tag` | `dev` | |
| `loadGenerator.rate` | `5` | Requests simulated per second. |
| `resources.requests.cpu` | `50m` | |
| `resources.requests.memory` | `64Mi` | |
| `resources.limits.memory` | `128Mi` | |

## `prometheus` chart

`deployments/helm/prometheus/` — a single-replica Deployment running upstream
`prom/prometheus`, scraping only the backend's own `/metrics`. It exists so the
"Observability Platform Internals" dashboard has a datasource to read in the Kubernetes
demo the same way the Compose `prometheus` service already gives it one; it is not a
general-purpose monitoring stack.

| Key | Default | Meaning |
|---|---|---|
| `fullnameOverride` | `observability-prometheus` | Pinned Service/Deployment name. The grafana chart's `internals.url` depends on this exact value — see Cross-chart contract above. |
| `image.repository` | `prom/prometheus` | Upstream Prometheus image. |
| `image.tag` | `v2.53.0` | Pinned, matching the Compose demo's Prometheus version. |
| `image.pullPolicy` | `IfNotPresent` | |
| `service.port` | `9090` | Prometheus HTTP port. Must match the port half of the grafana chart's `internals.url`. |
| `backend.url` | `http://observability-backend:8080` | The scrape target. Same cross-chart claim as the grafana and producers charts' `backend.url` — must resolve to the backend chart's Service; see Cross-chart contract above. The ConfigMap builds the scrape target with `trimPrefix "http://" .Values.backend.url`, so the rendered target is a bare `host:port` — Prometheus rejects a `static_configs` target that still carries a URL scheme. |
| `topology` | `all-in-one` | Match the backend chart. `split` scrapes every component in `split.targets`, each labelled with its `component` |
| `split.targets` | the five split Services | Each URL is a claim about a Service the backend chart creates in split; `tests/e2e/helm_split_prometheus_test.go` checks them. `split.targets.ingester` is a **list**, one URL per ingester pod (`http://observability-ingester-<i>.observability-ingester-headless:8080`), and must match the backend chart's `split.ingester.replicas`; every URL of a component that lists several becomes its own scrape target |
| `scrapeInterval` | `15s` | Matches the Compose Prometheus's `global.scrape_interval`. |
| `retention` | `24h` | Passed straight through to `--storage.tsdb.retention.time`. Only matters relative to the emptyDir below — data this Prometheus holds does not survive a pod reschedule regardless of what this says. |
| `resources.requests.cpu` | `100m` | |
| `resources.requests.memory` | `256Mi` | |
| `resources.limits.memory` | `1Gi` | |

Storage is an `emptyDir`, deliberately not a PVC: this chart exists to make the
self-observability dashboard render in the Kubernetes demo, not to ship a production
Prometheus. Every sample is lost when the pod is rescheduled. If you want internals
metrics to survive that, run a real Prometheus (e.g. `kube-prometheus-stack`) and point
it at the backend's `/metrics` — the endpoint this chart scrapes is the same one a
production Prometheus would use.

## Static validation

`tests/e2e/helm_test.go` runs in plain `go test ./...` — no cluster required — and skips
itself with a clear message if `helm` isn't on `PATH`. It covers `helm lint` for all
four charts, the cross-chart URL checks described above (backend, and the internals/
prometheus pair), that every rendered probe path is a real route in
`internal/api/router.go`, that every backend ConfigMap key has a matching config
default, and that rendering Grafana without a password fails as designed.

`tests/e2e/kind_smoke.sh` is the cluster-dependent counterpart: it builds and loads the
three custom images (backend, load-generator, sample-app; Prometheus and Grafana use
public images) into a real `kind` cluster, installs all four charts in the documented
order, and additionally verifies that data survives a pod restart and that Grafana can
query the backend from inside the cluster. It only runs in CI (the `helm-k8s-e2e` job).
