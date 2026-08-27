# Kubernetes Demo

The deployment counterpart to [`grafana-demo.md`](grafana-demo.md) and
[`grafana-logs-demo.md`](grafana-logs-demo.md). Those two run the backend as a single
Compose container; this one runs the same backend image as a Kubernetes StatefulSet,
fronted by Grafana and the same two producers, all installed from three separate Helm
charts under `deployments/helm/`.

The Compose demo remains the fastest way to see the project work. This runbook exists to
demonstrate the deployment story — Helm charts, a StatefulSet with a real PVC, liveness
and readiness probes evaluated by a real kubelet, and data surviving a pod being killed
and rescheduled — not to replace Compose as the everyday path.

## Prerequisites

- A Kubernetes cluster. [`kind`](https://kind.sigs.k8s.io/) is what CI uses and is the
  easiest local option; any cluster you can point `kubectl` at works.
- `kubectl`
- `helm` (v3)
- `docker`, to build the images `kind load` will push into the cluster

## Create a cluster

```bash
kind create cluster --name obs-demo
kubectl create namespace obs
```

Everything below targets the `obs` namespace. Drop `-n obs` throughout (and the
`kubectl create namespace` step) if you'd rather install into `default`.

## Build and load the images

The three charts use `imagePullPolicy: IfNotPresent` and reference images by name only —
there is no registry involved. Build them from the existing multi-stage
[`Dockerfile`](../../deployments/docker/Dockerfile) and load each one directly into the
cluster's node:

```bash
docker build -t observability-platform/backend:dev --target backend \
  -f deployments/docker/Dockerfile .
docker build -t observability-platform/sample-app:dev --target sampleapp \
  -f deployments/docker/Dockerfile .
docker build -t observability-platform/load-generator:dev --target loadgen \
  -f deployments/docker/Dockerfile .

kind load docker-image observability-platform/backend:dev --name obs-demo
kind load docker-image observability-platform/sample-app:dev --name obs-demo
kind load docker-image observability-platform/load-generator:dev --name obs-demo
```

If your cluster isn't `kind`, push these three images to whatever registry it can pull
from instead, and override `image.repository`/`image.tag` on install.

## Install, in this order

Install order is not arbitrary. The backend must exist before anything queries it, the
dashboards ConfigMap must exist before Grafana starts (its mount is deliberately not
`optional`, so a missing ConfigMap leaves the pod stuck rather than starting with no
dashboards), and the producers assume both are already up.

### 1. Backend

```bash
helm install backend deployments/helm/backend -n obs --wait
kubectl rollout status statefulset/observability-backend -n obs
kubectl get pvc -n obs
```

The backend is a **StatefulSet with a single replica**, deliberately. Each replica owns
a private PVC and a private WAL; two replicas would each hold a different slice of the
data, and a query would see whichever one it happened to land on. That ceiling is real —
this phase does not attempt to hide it — and Phase 6 is where sharding is meant to be
solved properly.

### 2. Dashboards ConfigMap (mandatory before Grafana)

The Grafana chart does not ship the dashboards. Helm can only read files inside its own
chart directory, so copying them in would fork them from
[`observability/grafana/dashboards/`](../../observability/grafana/dashboards/) — the
same files the Compose demo provisions from. Instead, the operator creates the ConfigMap
directly from that directory, keeping one source of truth:

```bash
kubectl create configmap grafana-dashboards \
  --from-file=observability/grafana/dashboards/ -n obs
```

`tests/e2e/kind_smoke.sh` runs this exact command in CI, so it is a tested path, not
just documentation.

### 3. Grafana

```bash
helm install grafana deployments/helm/grafana -n obs \
  --set admin.password=<choose-a-password> --wait
```

The chart ships **no default admin password**. This is deliberate — `CLAUDE.md` forbids
secrets in git, and a chart with a working default password is exactly how a demo
credential ends up in production. This differs from the Compose demo, which keeps the
`admin` / `admin` default because that stack only ever runs locally. Supply one of:

- `--set admin.password=<password>`, or
- `--set admin.existingSecret=<name>` naming a Secret you created yourself, with an
  `admin-password` key

Omitting both fails the install with a message telling you which to set.

### 4. Producers

```bash
helm install producers deployments/helm/producers -n obs --wait
```

This installs the sample app and the load generator, both pointed at the backend
Service. Without this chart the dashboards render with nothing in them.

## Verify

```bash
kubectl rollout status statefulset/observability-backend -n obs
kubectl rollout status deploy/observability-grafana -n obs
kubectl rollout status deploy/observability-producers-sample-app -n obs
kubectl rollout status deploy/observability-producers-load-generator -n obs
kubectl get pvc -n obs
```

Expected: all four rollouts complete, and the PVC created for the backend's
`volumeClaimTemplates` shows `Bound`.

Port-forward the backend and confirm it answers:

```bash
kubectl port-forward -n obs svc/observability-backend 8080:8080
curl -g 'http://localhost:8080/api/v1/query?query=up'
```

In a second terminal, port-forward Grafana and open it in a browser:

```bash
kubectl port-forward -n obs svc/observability-grafana 3000:3000
# then http://localhost:3000, admin / <the password you set above>
```

The same three dashboards from the Compose demo should be present and, after ~15
seconds, showing live data from the producers: **Observability Platform Metrics**,
**Observability Platform Sample App**, and **Observability Platform Logs**.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Grafana pod stuck in `ContainerCreating`, event names a missing ConfigMap | the dashboards ConfigMap step was skipped | run the `kubectl create configmap` command, then `kubectl rollout restart deploy/observability-grafana` |
| `helm install grafana` fails with a message about `admin.password` | no password supplied; the chart ships none by design | pass `--set admin.password=...` or `--set admin.existingSecret=...` |
| Backend pod `ImagePullBackOff` | the image was never loaded into the cluster | `kind load docker-image observability-platform/backend:dev --name <cluster>` |

## Cleanup

```bash
helm uninstall producers -n obs
helm uninstall grafana -n obs
helm uninstall backend -n obs
kubectl delete pvc -n obs --all
kind delete cluster --name obs-demo
```

The PVC is not removed by `helm uninstall` — StatefulSet volumes are left behind on
purpose, so an accidental uninstall cannot silently delete a WAL. Delete it explicitly,
or leave it and reinstall the backend chart to pick the data back up.
