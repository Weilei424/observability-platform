#!/usr/bin/env bash
#
# Cluster test: deploys all four charts into a kind cluster and exercises the
# Phase 5.2 DoD — helm install works, data survives a pod restart, and Grafana
# can query the backend from inside the cluster — plus the Phase 5.3 DoD, that
# Prometheus scrapes the backend and Grafana serves a real internals panel
# query from it.
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
# OBS_KIND_TOPOLOGY picks the backend chart's topology: all-in-one (default) or
# split (the five Phase 6.1 components). Grafana, the producers, and every
# check made through the gateway run unchanged against both.
TOPOLOGY="${OBS_KIND_TOPOLOGY:-all-in-one}"
case "$TOPOLOGY" in
    all-in-one) DEFAULT_CLUSTER=obs-e2e ;;
    split)      DEFAULT_CLUSTER=obs-e2e-split ;;
    *)
        echo "FATAL: OBS_KIND_TOPOLOGY must be all-in-one or split, not '$TOPOLOGY'." >&2
        exit 2
        ;;
esac
CLUSTER="${KIND_CLUSTER:-$DEFAULT_CLUSTER}"
TOPOLOGY_SET=(--set "topology=$TOPOLOGY")
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

# numeric_columns <body> — how many non-empty all-numeric arrays a GRAFANA
# DATAFRAME response carries (one series contributes two: timestamps and
# values). Same helper as compose_smoke.sh's; used to require positive
# evidence a real frame came back, not merely the absence of an error key —
# an empty or non-JSON body has no numeric columns either.
#
# Only valid against /api/ds/query. It must never be pointed at the backend's
# own Prometheus API: a Prometheus sample is [<number>, "<string>"], so
# `all(.[]; type == "number")` is false for every valid vector and the count is
# always 0. Use prom_sample_count below for those.
numeric_columns() {
    printf '%s' "$1" | jq '[.. | arrays | select(length > 0) | select(all(.[]; type == "number"))] | length' 2>/dev/null || echo 0
}

# prom_sample_count <body> — how many real samples a Prometheus instant-query
# response carries. Prometheus renders sample values as strings ([ts, "1.5"]),
# which is exactly what internal/api/query.go emits, so this reads
# .data.result[].value[1] and counts the entries that parse as a number.
#
# Everything that is not a successful envelope with numeric samples counts 0:
# an error envelope (filtered by the status check), an empty result array, a
# body that is not JSON at all (jq exits non-zero), an empty body (jq prints
# nothing), and a sample whose value is not a finite number. The last case
# needs the explicit isinfinite/isnan filter: jq's tonumber ACCEPTS "NaN" and
# "+Inf", and neither is evidence that a producer wrote a real sample.
prom_sample_count() {
    local n
    n=$(printf '%s' "$1" | jq '
        [ select(.status == "success")
        | .data.result[]? | objects | .value[1]? | strings | tonumber?
        | select((isinfinite or isnan) | not) ] | length
    ' 2>/dev/null)
    [ -n "$n" ] || n=0
    printf '%s' "$n"
}

# loki_entry_count <body> — how many log entries a Loki query_range response
# carries, across every returned stream. Same contract as prom_sample_count:
# anything that is not a successful envelope holding real entries counts 0.
loki_entry_count() {
    local n
    n=$(printf '%s' "$1" | jq '
        [ select(.status == "success")
        | .data.result[]? | objects | .values[]? | select(length == 2) ] | length
    ' 2>/dev/null)
    [ -n "$n" ] || n=0
    printf '%s' "$n"
}

# Sourcing hook for tests/e2e/kind_smoke_helpers_test.go, which exercises the
# assertion helpers above against fixtures. Everything below this line needs a
# cluster; the helpers above do not, and a helper that silently counts zero for
# every valid response is exactly the failure this hook exists to catch.
if [ "${OBS_KIND_SMOKE_LIB_ONLY:-0}" = "1" ]; then
    # Library mode is for tests that SOURCE this file. Executed directly, the
    # `return` fails and the old fallback ran `exit 0` — so the script reported
    # success having asserted nothing at all. `make smoke-kind` inherits the
    # environment, which makes a stray export of this variable a silent green
    # across the whole suite. Refuse rather than honour it.
    if [ "${BASH_SOURCE[0]}" = "$0" ]; then
        echo "FATAL: OBS_KIND_SMOKE_LIB_ONLY=1 is set, but this script was executed," >&2
        echo "       not sourced. Library mode would exit 0 here having run no assertions." >&2
        echo "       Unset the variable to run the suite." >&2
        exit 2
    fi
    return 0
fi

for tool in kind kubectl helm docker jq curl; do
    command -v "$tool" >/dev/null 2>&1 || { echo "FATAL: $tool is required" >&2; exit 2; }
done

# Cluster ownership. The invariant is that this script never deletes a cluster
# that existed before it started, and three states are what it takes to hold it:
#
#   unknown — nothing has been established. Teardown deletes nothing.
#   free    — `kind get clusters` SUCCEEDED and did not list $CLUSTER, so the
#             name was provably not in use and anything bearing it from here on
#             belongs to this run.
#   ours    — a create has been attempted against a name proven free. Teardown
#             may delete it, which is what removes the containers a create that
#             fails halfway leaves behind.
#
# Two earlier versions of this got it wrong in the same direction, so both are
# recorded. The first ran an unconditional `kind delete cluster` before the
# create, which destroyed a cluster KIND_CLUSTER happened to name and silently
# destroyed the one `OBS_KIND_KEEP_UP=1` had left behind to debug in. The second
# added an existence check but read a FAILED enumeration as "absent" and claimed
# the name with a plain flag: with a broken `kind get clusters` and a real
# cluster of that name it went on to attempt a create, which kind refuses
# ("node(s) already exist for a cluster with the name"), then reached teardown
# with the name claimed and deleted the pre-existing cluster. A failed
# enumeration is absence of evidence, not evidence of absence.
#
# Checked here, before `trap teardown EXIT` is installed, so no refusal path can
# reach the teardown at all: it neither deletes a cluster nor dumps the state of
# whatever cluster kubectl's current context happens to name.
#
# The enumeration below is a snapshot, so something could create $CLUSTER
# between it and the create. That window is closed at the create itself rather
# than with a lock: kind refuses a name already in use, and a create that is
# refused for that reason is proof the cluster is not ours — see the create
# below. A lock file would only exclude other runs of this same script; keying
# off kind's own refusal also covers a cluster that appeared by any other means,
# and carries no stale-lock state to clean up.
CLUSTER_OWNERSHIP=unknown

# Exit status read on its own, not through a pipeline: `kind get clusters | grep`
# makes grep the decider, and grep reports "no match" for an enumeration that
# never ran. stdout is captured for the match; kind's own error text is dropped
# because the reader gets an actionable message instead.
if ! EXISTING_CLUSTERS="$(kind get clusters 2>/dev/null)"; then
    echo "FATAL: \`kind get clusters\` failed, so this script cannot tell whether a" >&2
    echo "       cluster named '$CLUSTER' already exists — and it will not create or" >&2
    echo "       delete one on a guess." >&2
    echo "       Run \`kind get clusters\` to see why; a stopped Docker daemon is the" >&2
    echo "       usual cause." >&2
    exit 2
fi

if grep -qxF "$CLUSTER" <<<"$EXISTING_CLUSTERS"; then
    if [ "${OBS_KIND_REPLACE_CLUSTER:-0}" = "1" ]; then
        echo "-- OBS_KIND_REPLACE_CLUSTER=1: deleting the existing '$CLUSTER' cluster --"
        # Checked, unlike before: a delete that fails leaves the cluster in
        # place, and creating into it is how the "already exist" path above is
        # reached with the name claimed.
        if ! kind delete cluster --name "$CLUSTER"; then
            echo "FATAL: could not delete the existing '$CLUSTER' cluster; nothing was changed" >&2
            exit 2
        fi
        CLUSTER_OWNERSHIP=free
    else
        echo "FATAL: a kind cluster named '$CLUSTER' already exists." >&2
        echo "       This script will not delete a cluster it did not create. Either:" >&2
        echo "         - delete it yourself:     kind delete cluster --name $CLUSTER" >&2
        echo "         - use a different name:   KIND_CLUSTER=<other-name> $0" >&2
        echo "         - opt in to replacing it: OBS_KIND_REPLACE_CLUSTER=1 $0" >&2
        exit 2
    fi
else
    CLUSTER_OWNERSHIP=free
fi

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
        if [ "$TOPOLOGY" = split ]; then
            # All five split components share app.kubernetes.io/name=
            # observability-backend, distinguished only by
            # app.kubernetes.io/component. A single name-only selector here
            # would match all five pods at once; `kubectl logs --tail=40` then
            # concatenates their output with no indication of which pod wrote
            # which line, and the old `| tail -40` on top of that kept only the
            # last 40 lines total — a failed split run's dump was effectively
            # one unattributed pod. Select one component at a time and prefix
            # each line with its pod name instead of piping through tail.
            for component in gateway ingester querier store compactor; do
                echo ""
                echo "-- Logs: $component --"
                kubectl logs -n "$NS" \
                    -l "app.kubernetes.io/name=observability-backend,app.kubernetes.io/component=$component" \
                    --tail=40 --prefix 2>&1
                # The selector matches all three ingester pods; --prefix names
                # each. The ingester and store are the components this run restarts;
                # if the pre-restart container crashed rather than terminating
                # cleanly, its logs live only under --previous. That flag
                # errors when there is no previous container to read, which is
                # the common case here, not a failure worth reporting.
                if [ "$component" = ingester ] || [ "$component" = store ]; then
                    kubectl logs -n "$NS" \
                        -l "app.kubernetes.io/name=observability-backend,app.kubernetes.io/component=$component" \
                        --tail=40 --prefix --previous 2>/dev/null
                fi
            done
        else
            echo ""
            echo "-- Logs: observability-backend --"
            kubectl logs -n "$NS" -l "app.kubernetes.io/name=observability-backend" --tail=40 2>&1 | tail -40
        fi
        for app in observability-grafana observability-prometheus observability-producers-sample-app observability-producers-load-generator; do
            echo ""
            echo "-- Logs: $app --"
            kubectl logs -n "$NS" -l "app.kubernetes.io/name=$app" --tail=40 2>&1 | tail -40
        done
    fi
    # Always stop the port-forward; it outlives the script otherwise.
    [ -n "${PF_PID:-}" ] && kill "$PF_PID" 2>/dev/null
    # Delete only what this run created. Only the `ours` state authorises it,
    # and that state is reachable only after an enumeration that succeeded and
    # showed the name free — so a create that failed halfway has its wreckage
    # cleaned up, while a cluster that predates this run, or one another run
    # created in the gap (`raced`), is never touched.
    if [ "$CLUSTER_OWNERSHIP" != "ours" ]; then
        echo ""
        echo "-- Leaving the cluster alone: this run does not own it (ownership: $CLUSTER_OWNERSHIP) --"
        return
    fi
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
# Unreachable as the script stands, and deliberately here anyway: it states the
# precondition for claiming the name, so a later rearrangement that reaches the
# create without proving the name free fails closed instead of authorising a
# delete.
if [ "$CLUSTER_OWNERSHIP" != "free" ]; then
    echo "FATAL: cluster ownership is '$CLUSTER_OWNERSHIP', not 'free'; refusing to create '$CLUSTER'" >&2
    exit 2
fi
# Claimed before the create, not after: a create that fails partway leaves
# containers behind, and teardown must be allowed to remove them.
CLUSTER_OWNERSHIP=ours

# The create's output is teed rather than swallowed, because how it failed
# decides whether the cluster is ours to delete. Two runs that both saw the name
# free will both reach this line; one wins, and kind refuses the other with
# "node(s) already exist for a cluster with the name". That refusal is proof the
# cluster belongs to the winner — it is untouched by the refusal — so the loser
# disowns the name and its teardown leaves the winner's cluster alone. Without
# this, the loser tore down the winner's cluster mid-run.
#
# Any other failure is a create of ours that got partway, so the name stays
# claimed and teardown removes the containers it left.
#
# PIPESTATUS[0] rather than the pipeline's status: tee succeeds regardless, and
# the file keeps the output available for matching while the reader still sees
# the create's progress live over its two-minute run.
CREATE_LOG="$(mktemp -t kind-create.XXXXXX)"
kind create cluster --name "$CLUSTER" --wait 120s 2>&1 | tee "$CREATE_LOG"
CREATE_RC=${PIPESTATUS[0]}
if [ "$CREATE_RC" -ne 0 ]; then
    if grep -qi "already exist" "$CREATE_LOG"; then
        CLUSTER_OWNERSHIP=raced
        echo "FATAL: a cluster named '$CLUSTER' appeared between this run's check and its" >&2
        echo "       create — most likely a second run of this script. It is not ours, so" >&2
        echo "       it will be left alone. Re-run with KIND_CLUSTER=<other-name> to work" >&2
        echo "       alongside it." >&2
    else
        echo "FATAL: kind create cluster failed" >&2
    fi
    rm -f "$CREATE_LOG"
    exit 2
fi
rm -f "$CREATE_LOG"
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
if helm install backend "$REPO_ROOT/deployments/helm/backend" -n "$NS" --wait --timeout "$ROLLOUT_TIMEOUT" \
        "${TOPOLOGY_SET[@]}"; then
    log_pass "helm install deploys the backend"
else
    log_fail "helm install backend failed"
fi

if [ "$TOPOLOGY" = split ]; then
    ROLLOUTS="statefulset/observability-ingester statefulset/observability-store deployment/observability-backend deployment/observability-querier deployment/observability-compactor"
    # One PVC per ingester replica (chart default is 3) plus the store's PVC (1) = 4 total.
    # If the chart's split.ingester.replicas default changes, update this value.
    WANT_PVCS=4
else
    ROLLOUTS="statefulset/observability-backend"
    WANT_PVCS=1
fi
for workload in $ROLLOUTS; do
    if kubectl rollout status "$workload" -n "$NS" --timeout="$ROLLOUT_TIMEOUT"; then
        log_pass "$workload rolled out"
    else
        log_fail "$workload did not roll out"
    fi
done

# The PVCs are the whole point of the StatefulSets; assert every one bound.
BOUND=$(kubectl get pvc -n "$NS" -o jsonpath='{range .items[*]}{.status.phase}{"\n"}{end}' 2>/dev/null | grep -c '^Bound$')
if [ "$BOUND" = "$WANT_PVCS" ]; then
    log_pass "$WANT_PVCS PersistentVolumeClaim(s) Bound"
else
    log_fail "$BOUND of $WANT_PVCS PVCs bound: $(kubectl get pvc -n "$NS" 2>&1 | tail -4)"
fi

echo ""
echo "-- helm install prometheus --"
# The self-observability Prometheus (Phase 5.3): scrapes the backend's own
# /metrics. Installed before Grafana because Grafana's provisioned obs-internals
# datasource references this chart's Service by name, which keeps provisioning
# valid on Grafana's first start.
if helm upgrade --install obs-prometheus "$REPO_ROOT/deployments/helm/prometheus" \
        --namespace "$NS" --create-namespace --wait --timeout "$ROLLOUT_TIMEOUT" \
        "${TOPOLOGY_SET[@]}"; then
    log_pass "helm install deploys prometheus"
else
    log_fail "helm install prometheus failed"
fi

if kubectl rollout status deployment/observability-prometheus -n "$NS" --timeout="$ROLLOUT_TIMEOUT"; then
    log_pass "prometheus Deployment rolled out"
else
    log_fail "prometheus Deployment did not roll out"
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

# ---- Producers are actually writing -----------------------------------
echo ""
echo "-- Verifying producers write to the backend --"
# Both generators log-and-continue on a push failure and never exit, so a
# producer pointed at an unreachable backend stays Running and Ready forever:
# `helm install --wait` succeeds and every check above this passes while all
# three dashboards stay empty. This is the same class of gap an external
# reviewer caught in this project's previous phase (compose_smoke.sh's
# sample_app_metrics_up / load-generator checks) — query a series only the
# producers write and require real samples, not merely the absence of an
# error.
#
# One series per producer, and one per signal. http_requests_total is written
# only by the load generator; a check on it alone leaves a disconnected
# sample-app invisible, with both the sample-app and logs dashboards empty
# while the suite passes. So this also queries sample_app_active_workers (the
# sample app's metrics half) and a sample-app log stream (its Loki half) —
# three assertions covering all three provisioned dashboards.
kubectl port-forward -n "$NS" svc/observability-backend 18080:8080 >/dev/null 2>&1 &
PF_PID=$!
wait_for_port "backend port-forward is ready (producers check)" 30 "http://localhost:18080/healthz"

# await_samples <label> <writer> <url> <counter-fn> — polls one producer query
# until the response carries real samples, then records a single PASS or FAIL.
# The empty-body case is called out separately: an empty $BODY (a dropped
# connection or a stalled port-forward) must not read as "no samples yet",
# which points the reader at the wrong component.
await_samples() {
    local label="$1" writer="$2" url="$3" counter="$4"
    local body="" n=0 start=$SECONDS
    while [ $((SECONDS - start)) -lt 60 ]; do
        body=$(curl -sg --max-time 10 "$url")
        n=$("$counter" "$body")
        [ "${n:-0}" -ge 1 ] && break
        sleep 2
    done
    if [ -z "$body" ]; then
        log_fail "$writer is writing to the backend — empty response body for $label (connection or port-forward failure)"
    elif [ "${n:-0}" -ge 1 ]; then
        log_pass "$writer is writing $label with real samples ($n)"
    else
        log_fail "$writer is writing to the backend — no samples for $label after 60s; body: $body"
    fi
}

await_samples "http_requests_total" "load generator" \
    "http://localhost:18080/api/v1/query?query=http_requests_total" prom_sample_count
await_samples "sample_app_active_workers" "sample app" \
    "http://localhost:18080/api/v1/query?query=sample_app_active_workers" prom_sample_count
# The logs half: the sample app is the only writer of {service="worker"}, and
# it is the stream the provisioned logs dashboard reads. start/end are explicit
# because query_range's default window is not guaranteed to cover the few
# minutes this run has been up.
LOKI_START_NS=$(( ($(date +%s) - 900) * 1000000000 ))
LOKI_END_NS=$(( ($(date +%s) + 60) * 1000000000 ))
await_samples 'log stream {service="worker"}' "sample app" \
    "http://localhost:18080/loki/api/v1/query_range?query=%7Bservice%3D%22worker%22%7D&limit=100&start=${LOKI_START_NS}&end=${LOKI_END_NS}" \
    loki_entry_count
kill "$PF_PID" 2>/dev/null; PF_PID=""

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

# Split only: 120 samples seal one chunk, so the owning ingester's graceful stop
# below (every ingester is restarted) flushes this series to the store before
# it exits. Reading it back after the
# ingester's own restart proves only that the series survived — the ingester
# could equally have replayed it from its own WAL. Reading it back after the
# store's restart proves only that the marker survived a store restart: the
# read merges the ingester's head, which may hold the marker or have replayed
# it from its WAL, so it does not prove the store persisted the chunk.
if [ "$TOPOLOGY" = split ]; then
    FLUSH_METRICS=""
    for i in $(seq 0 119); do
        FLUSH_METRICS="$FLUSH_METRICS{\"name\":\"k8s_e2e_flush_marker\",\"labels\":{\"run\":\"kind\"},\"timestamp_ms\":$(( MARKER_MS - (119 - i) * 1000 )),\"value\":$MARKER_VALUE},"
    done
    STATUS=$(curl -s -o /dev/null -w "%{http_code}" --max-time 10 -X POST "http://localhost:18080/api/v1/ingest/metrics" \
        -H "Content-Type: application/json" -d "{\"metrics\":[${FLUSH_METRICS%,}]}")
    if [ "$STATUS" = "204" ]; then
        log_pass "seeded flush marker (120 samples, HTTP 204)"
    else
        log_fail "seeding the flush marker returned HTTP $STATUS, want 204"
    fi
fi
kill "$PF_PID" 2>/dev/null; PF_PID=""

# ---- Restart ---------------------------------------------------------

# restart_pod <statefulset> [ordinal] — deletes the StatefulSet's pod (ordinal
# 0 unless given) and proves a new object replaced it. A StatefulSet pod keeps its name across a reschedule, so
# only the UID can show that the replacement is a different process; without
# that, a delete that never happened would leave every later check satisfied by
# the original pod.
restart_pod() {
    local sts="$1" pod="$1-${2:-0}" old_uid new_uid poll_start
    echo ""
    echo "-- Deleting pod $pod --"
    old_uid=$(kubectl get pod "$pod" -n "$NS" -o jsonpath='{.metadata.uid}' 2>/dev/null)
    [ -n "$old_uid" ] || log_fail "could not read $pod's UID before the restart"
    if kubectl delete pod "$pod" -n "$NS" --wait=true --timeout="$ROLLOUT_TIMEOUT"; then
        log_pass "deleted $pod"
    else
        log_fail "deleting $pod failed — nothing below this actually tests a restart"
    fi
    if kubectl rollout status "statefulset/$sts" -n "$NS" --timeout="$ROLLOUT_TIMEOUT"; then
        log_pass "$sts rescheduled its pod"
    else
        log_fail "$sts's pod did not come back"
    fi
    # `rollout status` can report complete before the replacement pod object
    # itself exists yet, which would make the `kubectl wait` below fail
    # immediately with NotFound rather than actually waiting. Poll for a new
    # object (a UID that differs from old_uid) with its own deadline first;
    # if it never appears, `kubectl wait` below still runs and fails on its
    # own terms.
    poll_start=$SECONDS
    while [ $((SECONDS - poll_start)) -lt 60 ]; do
        new_uid=$(kubectl get pod "$pod" -n "$NS" -o jsonpath='{.metadata.uid}' 2>/dev/null)
        [ -n "$new_uid" ] && [ "$new_uid" != "$old_uid" ] && break
        sleep 2
    done
    # `delete --wait` returns once the object is gone, not once its replacement
    # runs, and rollout status can read stale status; wait for Ready explicitly.
    if kubectl wait --for=condition=Ready "pod/$pod" -n "$NS" --timeout="$ROLLOUT_TIMEOUT"; then
        log_pass "replacement $pod reached Ready"
    else
        log_fail "replacement $pod did not reach Ready"
    fi
    new_uid=$(kubectl get pod "$pod" -n "$NS" -o jsonpath='{.metadata.uid}' 2>/dev/null)
    if [ -z "$new_uid" ]; then
        log_fail "could not read $pod's UID after the restart"
    elif [ -n "$old_uid" ] && [ "$new_uid" = "$old_uid" ]; then
        log_fail "$pod was never replaced — UID is still $old_uid"
    elif [ -n "$old_uid" ]; then
        log_pass "$pod is a new object ($old_uid -> $new_uid)"
    fi
}

# marker_reads_back <label> <series> — reads <series> through the gateway and
# requires MARKER_VALUE by value. A retried port-forward, and an explicit
# empty-body guard, for the reasons the original single-restart check gave.
#
# The port-forward lands on the gateway, which never restarted — but in split,
# the gateway/querier reach the ingester and store through ClusterIP Services
# whose endpoints and kube-proxy rules update asynchronously, and a component
# may still hold a keep-alive connection to the pod that just went away. A
# single query right after the restart can therefore answer with a transient
# error before that settles, so this polls with a deadline (the same
# success-flag pattern as prometheus_target_up) instead of asserting once.
marker_reads_back() {
    local label="$1" series="$2" body="" match attempt ok=0 start
    PF_PID=""
    for attempt in 1 2 3; do
        kubectl port-forward -n "$NS" svc/observability-backend 18080:8080 >/dev/null 2>&1 &
        PF_PID=$!
        sleep 1
        if kill -0 "$PF_PID" 2>/dev/null && wait_for_port "backend port-forward is ready ($label, attempt $attempt)" 15 "http://localhost:18080/healthz"; then
            break
        fi
        kill "$PF_PID" 2>/dev/null; PF_PID=""
    done
    start=$SECONDS
    while [ $((SECONDS - start)) -lt 60 ]; do
        body=$(curl -sg --max-time 15 "http://localhost:18080/api/v1/query?query=$series")
        if [ -n "$body" ]; then
            match=$(printf '%s' "$body" | jq -r --argjson want "$MARKER_VALUE" \
                '([.. | strings | tonumber? // empty] | index($want)) // "null"' 2>/dev/null)
            if [ -n "$match" ] && [ "$match" != "null" ]; then
                ok=1
                break
            fi
        fi
        sleep 2
    done
    if [ "$ok" -eq 1 ]; then
        log_pass "$label — $series value $MARKER_VALUE read back ($((SECONDS - start))s)"
    elif [ -z "$body" ]; then
        log_fail "$label — empty response body (connection or port-forward failure)"
    else
        log_fail "$label — $series value $MARKER_VALUE not found after 60s; body: $body"
    fi
    kill "$PF_PID" 2>/dev/null; PF_PID=""
}

if [ "$TOPOLOGY" = split ]; then
    # The ring decides which ingester owns each marker (with three members both
    # land on ingester-2), so restart every replica: the owner's graceful stop
    # then flushes its marker to the store and its restart replays the WAL,
    # whichever pod that is. Restarting only ordinal 0 could pass without
    # touching either marker.
    INGESTERS=$(kubectl get statefulset observability-ingester -n "$NS" -o jsonpath='{.spec.replicas}' 2>/dev/null)
    if [ -z "$INGESTERS" ] || [ "$INGESTERS" -lt 1 ]; then
        log_fail "could not read the ingester StatefulSet's replica count; restarting ordinal 0 only"
        INGESTERS=1
    fi
    for i in $(seq 0 $((INGESTERS - 1))); do
        restart_pod observability-ingester "$i"
    done
    marker_reads_back "data persists across every ingester's restart" k8s_e2e_marker
    marker_reads_back "the flushed series persists across every ingester's restart" k8s_e2e_flush_marker
    restart_pod observability-store
    marker_reads_back "data persists across the store's restart" k8s_e2e_marker
    marker_reads_back "the flushed series persists across the store's restart" k8s_e2e_flush_marker
else
    restart_pod observability-backend
    marker_reads_back "data persists across pod restart" k8s_e2e_marker
fi

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
if grep -q '"status":"OK"' <<<"$HEALTH"; then
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
elif grep -q '"error":"' <<<"$DSQ"; then
    log_fail "Grafana query returned an error: $DSQ"
else
    COLS=$(numeric_columns "$DSQ")
    if [ "${COLS:-0}" -ge 2 ]; then
        log_pass "Grafana queries the backend inside Kubernetes — marker series returned with samples"
    else
        log_fail "Grafana query returned no numeric samples for k8s_e2e_marker (cols=${COLS:-0}); body: $DSQ"
    fi
fi

# ---- Self-observability (Phase 5.3) ---------------------------------
echo ""
echo "-- Platform self-observability --"

# Fetch the dashboard by uid through Grafana's own API before querying any of
# its panels. Without this, a dashboard ConfigMap that failed to provision
# in-cluster (a bad mount, a rename that broke the uid) would go unnoticed:
# the panel-query check below hits the internals Prometheus datasource
# directly and would still pass even with no such dashboard loaded.
IDASH=$(curl -s --max-time 15 -u "admin:e2e-only" \
    "http://localhost:13000/api/dashboards/uid/obs-self-v1")
if grep -q '"uid":"obs-self-v1"' <<<"$IDASH" \
        && grep -q '"title":"Observability Platform Internals"' <<<"$IDASH"; then
    log_pass "internals dashboard provisioned in-cluster (uid obs-self-v1)"
else
    log_fail "internals dashboard not provisioned as expected: $IDASH"
fi

# One self-observability panel query, through Grafana, against the internals
# datasource — the same path a dashboard panel takes. A freshly installed
# Prometheus has not scraped yet, so this polls rather than asserting once.
internals_panel_has_data() {
    local body cols
    body="$(curl -s --max-time 10 -u "admin:e2e-only" -H 'Content-Type: application/json' \
        -X POST "http://localhost:13000/api/ds/query" \
        -d '{"queries":[{"refId":"A","datasource":{"type":"prometheus","uid":"obs-internals"},"expr":"obs_active_series","range":true,"intervalMs":15000,"maxDataPoints":100}],"from":"now-15m","to":"now"}' 2>/dev/null)"
    cols="$(numeric_columns "$body")"
    [ "${cols:-0}" -ge 2 ]
}

# The success flag, rather than a post-loop elapsed-time check, is what decides
# the verdict: the winning attempt's own curl can push the clock past the
# deadline, and an elapsed-time recheck would then log a FAIL alongside the PASS
# it just logged — corrupting $FAIL and the exit code on a run that passed.
panel_ok=0
start=$SECONDS
while [ $((SECONDS - start)) -lt 120 ]; do
    if internals_panel_has_data; then
        log_pass "Grafana served an internals panel query in-cluster ($((SECONDS - start))s)"
        panel_ok=1
        break
    fi
    sleep 5
done
[ "$panel_ok" -eq 1 ] || log_fail "internals panel query returned no numeric data within 120s"
kill "$PF_PID" 2>/dev/null; PF_PID=""

# The "up" half of the claim: necessary but not sufficient on its own (a scrape
# that connects and returns zero series still reports up: 1), which is why the
# panel query above is the assertion that actually matters here. Checked
# through a dedicated port-forward straight to the in-cluster Prometheus,
# mirroring compose_smoke.sh's up check against its own Prometheus service.
kubectl port-forward -n "$NS" svc/observability-prometheus 19090:9090 >/dev/null 2>&1 &
PF_PID=$!
wait_for_port "prometheus port-forward is ready" 30 "http://localhost:19090/-/healthy"

prometheus_target_up() {
    local q want body
    if [ "$TOPOLOGY" = split ]; then
        q='count(up{service="observability-platform"} == 1)'; want=7
    else
        q='up{job="observability-platform-backend"}'; want=1
    fi
    body="$(curl -s --max-time 10 -G "http://localhost:19090/api/v1/query" --data-urlencode "query=$q" 2>/dev/null)"
    [ "$(printf '%s' "$body" | jq -r '.data.result[0].value[1] // "0"' 2>/dev/null)" = "$want" ]
}
# Same success-flag rule as the panel loop above.
target_ok=0
start=$SECONDS
while [ $((SECONDS - start)) -lt 90 ]; do
    if prometheus_target_up; then
        log_pass "in-cluster Prometheus reports every scrape target up ($((SECONDS - start))s)"
        target_ok=1
        break
    fi
    sleep 2
done
[ "$target_ok" -eq 1 ] || log_fail "in-cluster Prometheus never reported every scrape target up within 90s"

# Split only: the ring spreads the producers' writes over all three ingesters.
# The backend image is distroless (no shell or wget), so read each pod's
# obs_samples_ingested_total through this same Prometheus, which scrapes every
# pod by its DNS name (the instance label). Polled because the first scrape
# after a write can lag by one interval. Mirrors compose_smoke.sh's check.
if [ "$TOPOLOGY" = split ]; then
    ingester_ingested() { # <pod> -> sample count on stdout, nonzero exit if none yet
        local n
        n="$(curl -s --max-time 10 -G "http://localhost:19090/api/v1/query" \
            --data-urlencode "query=sum(obs_samples_ingested_total{instance=\"$1.observability-ingester-headless:8080\"})" 2>/dev/null \
            | jq -r '.data.result[0].value[1] // "0"' 2>/dev/null)"
        echo "${n:-0}"
        awk -v c="${n:-0}" 'BEGIN { exit !(c > 0) }'
    }
    for i in 0 1 2; do
        pod="observability-ingester-$i"
        COUNT=0
        spread_start=$SECONDS
        while [ $((SECONDS - spread_start)) -lt 45 ]; do
            COUNT="$(ingester_ingested "$pod")" && break
            sleep 3
        done
        if awk -v c="${COUNT:-0}" 'BEGIN { exit !(c > 0) }'; then
            log_pass "$pod ingested ${COUNT} samples through the ring"
        else
            log_fail "$pod ingested no samples; the ring is not routing to it"
        fi
    done
fi
kill "$PF_PID" 2>/dev/null; PF_PID=""

echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
