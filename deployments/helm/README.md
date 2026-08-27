# Helm Charts

Three charts, installed separately rather than as one umbrella chart, because they scale
and fail independently: the backend is stateful and singular, Grafana is stateless and
singular, and the producers are optional demo traffic you might want to disable or scale
without touching either of the other two. See
[`docs/runbooks/kubernetes-demo.md`](../../docs/runbooks/kubernetes-demo.md) for the
install walkthrough; this file is the values reference.

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
| `service.port` | `8080` | Backend HTTP port, exposed on both the ClusterIP and the headless Service. |
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
`TestBackendConfigKeysAreReal` in `tests/e2e/helm_test.go` checks this for every key
actually shipped in `values.yaml`.

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
| `sampleApp.replicas` | `1` | Pod count for the sample app Deployment. |
| `sampleApp.image.repository` | `observability-platform/sample-app` | Built by the `sampleapp` target. |
| `sampleApp.image.tag` | `dev` | |
| `sampleApp.rate` | `2` | Log batches per second. |
| `sampleApp.metricsRate` | `1` | Metric pushes per second — an independent ticker from `rate`. |
| `loadGenerator.enabled` | `true` | Set `false` to skip the load generator Deployment. |
| `loadGenerator.replicas` | `1` | Pod count for the load generator Deployment. |
| `loadGenerator.image.repository` | `observability-platform/load-generator` | Built by the `loadgen` target. |
| `loadGenerator.image.tag` | `dev` | |
| `loadGenerator.rate` | `5` | Requests simulated per second. |
| `resources.requests.cpu` | `50m` | |
| `resources.requests.memory` | `64Mi` | |
| `resources.limits.memory` | `128Mi` | |

## Static validation

`tests/e2e/helm_test.go` runs in plain `go test ./...` — no cluster required — and skips
itself with a clear message if `helm` isn't on `PATH`. It covers `helm lint` for all
three charts, the cross-chart URL check described above, that every rendered probe path
is a real route in `internal/api/router.go`, that every backend ConfigMap key has a
matching config default, and that rendering Grafana without a password fails as
designed.

`tests/e2e/kind_smoke.sh` is the cluster-dependent counterpart: it builds and loads the
three images into a real `kind` cluster, installs all three charts in the documented
order, and additionally verifies that data survives a pod restart and that Grafana can
query the backend from inside the cluster. It only runs in CI (the `helm-k8s-e2e` job).
