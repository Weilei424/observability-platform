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
| `fullnameOverride` | `observability-backend` | Pinned Service/StatefulSet name. The other two charts' `backend.url` depends on this exact value. |
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

`deployments/helm/grafana/` — a stateless Deployment. Provisions two datasources
(`obs-prometheus`, `obs-loki`) pointed at the backend, plus a dashboard provider; the
dashboards themselves come from an operator-created ConfigMap, not from this chart.

| Key | Default | Meaning |
|---|---|---|
| `fullnameOverride` | `observability-grafana` | Pinned Service/Deployment name. |
| `image.repository` | `grafana/grafana` | Upstream Grafana image. |
| `image.tag` | `11.1.0` | Pinned to match the Compose demo's Grafana version. |
| `service.port` | `3000` | |
| `backend.url` | `http://observability-backend:8080` | The backend Service both provisioned datasources point at. Must match the backend chart's `fullnameOverride` and `service.port` — see Cross-chart contract above. |
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
three images into a real `kind` cluster, installs all three charts in the documented
order, and additionally verifies that data survives a pod restart and that Grafana can
query the backend from inside the cluster. It only runs in CI (the `helm-k8s-e2e` job).
