#!/usr/bin/env bash
#
# Cluster test: deploys all three charts into a kind cluster and exercises the
# Phase 5.2 DoD — helm install works, data survives a pod restart, and Grafana
# can query the backend from inside the cluster.
#
# tests/e2e/helm_test.go validates the charts without a cluster. It cannot see
# whether the images actually start, whether the PVC binds, whether the probes
# pass against a real server, or whether data survives rescheduling. That is
# what this script is for.
#
# Not `set -e`: like compose_smoke.sh, this counts failures and must always
# reach its teardown and summary.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
CLUSTER="${KIND_CLUSTER:-obs-e2e}"
NS="${K8S_NAMESPACE:-obs}"
KEEP_UP="${OBS_KIND_KEEP_UP:-0}"
ROLLOUT_TIMEOUT="${OBS_ROLLOUT_TIMEOUT:-300s}"

PASS=0
FAIL=0
log_pass() { echo "  PASS: $1"; PASS=$((PASS + 1)); }
log_fail() { echo "  FAIL: $1"; FAIL=$((FAIL + 1)); }

# wait_for_port <label> <timeout-seconds> <url> — polls a freshly started
# port-forward until it actually answers, instead of guessing with a fixed
# sleep. Following compose_smoke.sh's own rule: a polling loop's deadline only
# means something if each attempt is guaranteed to return, so every curl here
# carries its own --max-time — a tunnel that accepts the connection and then
# stalls would otherwise hang the whole loop instead of letting it retry.
wait_for_port() {
    local label="$1" timeout="$2" url="$3"
    local start=$SECONDS
    while [ $((SECONDS - start)) -lt "$timeout" ]; do
        if curl -sf --max-time 5 "$url" >/dev/null 2>&1; then
            log_pass "$label ($((SECONDS - start))s)"
            return 0
        fi
        sleep 1
    done
    log_fail "$label — not ready after ${timeout}s"
    return 1
}

# numeric_columns <body> — how many non-empty all-numeric arrays a Grafana
# dataframe response carries (one series contributes two: timestamps and
# values). Same helper as compose_smoke.sh's; used to require positive
# evidence a real frame came back, not merely the absence of an error key —
# an empty or non-JSON body has no numeric columns either.
numeric_columns() {
    printf '%s' "$1" | jq '[.. | arrays | select(length > 0) | select(all(.[]; type == "number"))] | length' 2>/dev/null || echo 0
}

for tool in kind kubectl helm docker jq; do
    command -v "$tool" >/dev/null 2>&1 || { echo "FATAL: $tool is required" >&2; exit 2; }
done

teardown() {
    local rc=$?
    if [ "$FAIL" -ne 0 ] || [ "$rc" -ne 0 ]; then
        echo ""
        echo "-- Cluster state (run failed) --"
        kubectl get pods -n "$NS" -o wide 2>&1 | tail -20
        kubectl get pvc -n "$NS" 2>&1 | tail -10
        echo ""
        echo "-- Events --"
        kubectl get events -n "$NS" --sort-by=.lastTimestamp 2>&1 | tail -25
        for app in observability-backend observability-grafana; do
            echo ""
            echo "-- Logs: $app --"
            kubectl logs -n "$NS" -l "app.kubernetes.io/name=$app" --tail=40 2>&1 | tail -40
        done
    fi
    # Always stop the port-forward; it outlives the script otherwise.
    [ -n "${PF_PID:-}" ] && kill "$PF_PID" 2>/dev/null
    if [ "$KEEP_UP" = "1" ]; then
        echo ""
        echo "OBS_KIND_KEEP_UP=1 — leaving the cluster; delete it with: kind delete cluster --name $CLUSTER"
        return
    fi
    echo ""
    echo "-- Deleting cluster --"
    kind delete cluster --name "$CLUSTER" >/dev/null 2>&1
}
trap teardown EXIT

echo "=== Phase 5.2 kind cluster test: cluster=$CLUSTER namespace=$NS ==="

# ---- Cluster and images ---------------------------------------------
echo ""
echo "-- Creating cluster --"
kind delete cluster --name "$CLUSTER" >/dev/null 2>&1
if ! kind create cluster --name "$CLUSTER" --wait 120s; then
    echo "FATAL: kind create cluster failed" >&2
    exit 2
fi
kubectl create namespace "$NS"

echo ""
echo "-- Building and loading images --"
# The charts use imagePullPolicy: IfNotPresent, so a kind-loaded image is used
# without any registry.
for target in backend:backend sampleapp:sample-app loadgen:load-generator; do
    stage="${target%%:*}"; name="${target##*:}"
    if ! docker build -q -t "observability-platform/$name:dev" \
            -f "$REPO_ROOT/deployments/docker/Dockerfile" --target "$stage" "$REPO_ROOT" >/dev/null; then
        echo "FATAL: docker build --target $stage failed" >&2
        exit 2
    fi
    if ! kind load docker-image "observability-platform/$name:dev" --name "$CLUSTER" >/dev/null; then
        echo "FATAL: kind load docker-image $name failed" >&2
        exit 2
    fi
done
log_pass "built and loaded three images"

# ---- Install ---------------------------------------------------------
echo ""
echo "-- helm install backend --"
if helm install backend "$REPO_ROOT/deployments/helm/backend" -n "$NS" --wait --timeout "$ROLLOUT_TIMEOUT"; then
    log_pass "helm install deploys the backend"
else
    log_fail "helm install backend failed"
fi

if kubectl rollout status statefulset/observability-backend -n "$NS" --timeout="$ROLLOUT_TIMEOUT"; then
    log_pass "backend StatefulSet rolled out"
else
    log_fail "backend StatefulSet did not roll out"
fi

# The PVC is the whole point of using a StatefulSet; assert it bound.
if [ "$(kubectl get pvc -n "$NS" -o jsonpath='{.items[0].status.phase}' 2>/dev/null)" = "Bound" ]; then
    log_pass "PersistentVolumeClaim is Bound"
else
    log_fail "PVC did not bind: $(kubectl get pvc -n "$NS" 2>&1 | tail -3)"
fi

echo ""
echo "-- Dashboards ConfigMap (the command the runbook prints) --"
# Run it exactly as docs/runbooks/kubernetes-demo.md and the chart's NOTES.txt
# instruct. If the documented command is wrong, this job fails rather than a
# reader discovering it.
if kubectl create configmap grafana-dashboards \
        --from-file="$REPO_ROOT/observability/grafana/dashboards/" -n "$NS"; then
    log_pass "documented kubectl create configmap command works"
else
    log_fail "documented kubectl create configmap command failed"
fi

echo ""
echo "-- helm install grafana and producers --"
helm install grafana "$REPO_ROOT/deployments/helm/grafana" -n "$NS" \
    --set admin.password=e2e-only --wait --timeout "$ROLLOUT_TIMEOUT" \
    && log_pass "helm install deploys Grafana" || log_fail "helm install grafana failed"

helm install producers "$REPO_ROOT/deployments/helm/producers" -n "$NS" \
    --wait --timeout "$ROLLOUT_TIMEOUT" \
    && log_pass "helm install deploys the producers" || log_fail "helm install producers failed"

# ---- Seed a marker ---------------------------------------------------
echo ""
echo "-- Seeding a restart marker --"
kubectl port-forward -n "$NS" svc/observability-backend 18080:8080 >/dev/null 2>&1 &
PF_PID=$!
wait_for_port "backend port-forward is ready" 30 "http://localhost:18080/healthz"

# A run-unique value, on a series nothing else writes. Querying a live producer
# series after the restart would prove nothing: the producers keep writing
# throughout, so fresh samples would satisfy the assertion even if every
# pre-restart sample had been lost. Phase 5.1 shipped exactly that mistake in
# compose_smoke.sh.
MARKER_VALUE=$(( (RANDOM % 90000) + 10000 ))
MARKER_MS=$(( $(date +%s) * 1000 ))
STATUS=$(curl -s -o /dev/null -w "%{http_code}" --max-time 10 \
    -X POST "http://localhost:18080/api/v1/ingest/metrics" \
    -H "Content-Type: application/json" \
    -d "{\"metrics\":[{\"name\":\"k8s_e2e_marker\",\"labels\":{\"run\":\"kind\"},\"timestamp_ms\":$MARKER_MS,\"value\":$MARKER_VALUE}]}")
if [ "$STATUS" = "204" ]; then
    log_pass "seeded marker (HTTP 204, value $MARKER_VALUE)"
else
    log_fail "seeding the marker returned HTTP $STATUS, want 204"
fi
kill "$PF_PID" 2>/dev/null; PF_PID=""

# ---- Restart ---------------------------------------------------------
echo ""
echo "-- Deleting the backend pod --"
kubectl delete pod observability-backend-0 -n "$NS" --wait=true
if kubectl rollout status statefulset/observability-backend -n "$NS" --timeout="$ROLLOUT_TIMEOUT"; then
    log_pass "StatefulSet rescheduled the pod"
else
    log_fail "pod did not come back"
fi

kubectl port-forward -n "$NS" svc/observability-backend 18080:8080 >/dev/null 2>&1 &
PF_PID=$!
wait_for_port "backend port-forward is ready (post-restart)" 30 "http://localhost:18080/healthz"

BODY=$(curl -sg --max-time 15 "http://localhost:18080/api/v1/query?query=k8s_e2e_marker")
# `jq -e` treats zero output values the same as a successful non-null/false
# result on some jq builds — an empty $BODY (a dropped connection or a
# port-forward that closed) would otherwise print PASS on this phase's
# headline persistence claim. Guard the empty case explicitly, and read the
# match back into a variable instead of trusting -e's exit status alone.
if [ -z "$BODY" ]; then
    log_fail "data persists across pod restart — empty response body from the query (connection or port-forward failure)"
else
    MATCH=$(printf '%s' "$BODY" | jq -r --argjson want "$MARKER_VALUE" \
        '([.. | strings | tonumber? // empty] | index($want)) // "null"' 2>/dev/null)
    if [ -n "$MATCH" ] && [ "$MATCH" != "null" ]; then
        log_pass "data persists across pod restart — marker value $MARKER_VALUE read back"
    else
        log_fail "marker value $MARKER_VALUE not found after restart; body: $BODY"
    fi
fi
kill "$PF_PID" 2>/dev/null; PF_PID=""

# ---- Grafana queries the backend -------------------------------------
echo ""
echo "-- Querying through Grafana --"
kubectl port-forward -n "$NS" svc/observability-grafana 13000:3000 >/dev/null 2>&1 &
PF_PID=$!
wait_for_port "grafana port-forward is ready" 30 "http://localhost:13000/api/health"

# Through Grafana's own API, not the backend's: this is what proves the
# in-cluster datasource URL resolves and Grafana can reach the backend Service.
HEALTH=$(curl -s --max-time 15 -u "admin:e2e-only" \
    "http://localhost:13000/api/datasources/uid/obs-prometheus/health")
if printf '%s' "$HEALTH" | grep -q '"status":"OK"'; then
    log_pass "Grafana datasource health passes inside the cluster"
else
    log_fail "Grafana datasource health failed: $HEALTH"
fi

DSQ=$(curl -s --max-time 20 -u "admin:e2e-only" -H 'Content-Type: application/json' \
    -X POST "http://localhost:13000/api/ds/query" \
    -d '{"queries":[{"refId":"A","datasource":{"type":"prometheus","uid":"obs-prometheus"},"expr":"k8s_e2e_marker","range":true,"intervalMs":5000,"maxDataPoints":100}],"from":"now-15m","to":"now"}')
# An absence-of-error test alone passes on an empty body (a dropped
# connection) or an HTML error page from a broken proxy — neither contains
# `"error":"`. Mirror compose_smoke.sh's check_absent: guard the empty body
# first, then look for the error *key* (not the bare word — a field unrelated
# to failure could legitimately contain the substring "error"), and finally
# require positive evidence — a real numeric dataframe for k8s_e2e_marker —
# so a non-JSON body cannot pass just by lacking an error key.
if [ -z "$DSQ" ]; then
    log_fail "Grafana query returned an empty response body (connection or port-forward failure)"
elif printf '%s' "$DSQ" | grep -q '"error":"'; then
    log_fail "Grafana query returned an error: $DSQ"
else
    COLS=$(numeric_columns "$DSQ")
    if [ "${COLS:-0}" -ge 2 ]; then
        log_pass "Grafana queries the backend inside Kubernetes — marker series returned with samples"
    else
        log_fail "Grafana query returned no numeric samples for k8s_e2e_marker (cols=${COLS:-0}); body: $DSQ"
    fi
fi
kill "$PF_PID" 2>/dev/null; PF_PID=""

echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
