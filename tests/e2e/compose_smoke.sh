#!/usr/bin/env bash
#
# Phase 4.5 real-stack test: brings up the Docker Compose demo and drives it
# through Grafana's own HTTP API, not the backend's.
#
# The other two checks in `make smoke-logs` deliberately stop short of this.
# tests/e2e/provisioning_test.go reads the provisioning files without starting
# anything, and tests/e2e/logs_smoke.sh talks straight to the backend. Neither
# notices a volume mounted at the wrong path, a datasource Grafana refuses to
# load, a dashboard that never provisions, a sample-app container that exits at
# startup, or a Grafana version that stops speaking to this backend — every one
# of which leaves the demo broken with both suites green.
#
# Everything below therefore goes through Grafana on port 3000: datasource
# health (the "Save & test" button), the provisioned dashboard, the variable
# dropdowns through the datasource resource proxy, and the panel expressions
# through /api/ds/query. The last two checks cover the storage path the demo's
# flush override exists to exercise — chunks are written, and the data survives
# a backend restart.
#
# Not `set -e`: this script counts failures and must always reach its teardown
# and summary, so failures are checked explicitly rather than aborting the run.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$REPO_ROOT/deployments/docker/docker-compose.yml"

# A project name of its own, so this never reuses, restarts, or deletes the
# volumes of a stack someone started with `make local-up`.
PROJECT="${OBS_COMPOSE_PROJECT:-obs-compose-e2e}"
GRAFANA="${GRAFANA_ADDR:-http://localhost:3000}"
BACKEND="${BACKEND_ADDR:-http://localhost:8080}"
GRAFANA_AUTH="${GRAFANA_AUTH:-admin:admin}"
KEEP_UP="${OBS_COMPOSE_KEEP_UP:-0}"

READY_TIMEOUT="${OBS_COMPOSE_READY_TIMEOUT:-180}"
FLUSH_TIMEOUT="${OBS_COMPOSE_FLUSH_TIMEOUT:-180}"

# Every HTTP request and every docker call is individually bounded. A polling
# loop's deadline only means something if each attempt is guaranteed to return:
# a service that accepts the connection and then stalls would otherwise hang
# inside one curl forever, and the loop would never get to check its own
# deadline again. Only the job timeout would stop it, with no useful output.
CURL_TIMEOUTS=(--connect-timeout 3 --max-time 15)

PASS=0
FAIL=0
RUN_ID="run$(date +%s%N | tr 0-9 a-j)"

# The expected running set. load-generator has no assertion of its own anywhere
# else in this script, so without this it could exit at startup unnoticed.
EXPECTED_SERVICES="backend grafana load-generator sample-app"

# dc runs a compose command with a bound generous enough for an image build.
dc() { timeout "${DC_TIMEOUT:-1800}" docker compose -p "$PROJECT" -f "$COMPOSE_FILE" "$@"; }
# dcq is for the short compose commands that run inside polling loops, where a
# hung docker call would blow through the loop's deadline the same way a hung
# curl would.
dcq() { timeout 60 docker compose -p "$PROJECT" -f "$COMPOSE_FILE" "$@"; }

log_pass() { echo "  PASS: $1"; PASS=$((PASS + 1)); }
log_fail() { echo "  FAIL: $1"; FAIL=$((FAIL + 1)); }

check_contains() {
    local label="$1" body="$2" needle="$3"
    if printf '%s' "$body" | grep -qF -- "$needle"; then
        log_pass "$label"
    else
        log_fail "$label — missing '$needle' in: $(printf '%s' "$body" | head -c 400)"
    fi
}

check_absent() {
    local label="$1" body="$2" needle="$3"
    if [ -z "$body" ]; then
        log_fail "$label — empty response body"
    elif printf '%s' "$body" | grep -qF -- "$needle"; then
        log_fail "$label — unexpected '$needle' in: $(printf '%s' "$body" | head -c 400)"
    else
        log_pass "$label"
    fi
}

gapi() { curl -s "${CURL_TIMEOUTS[@]}" -u "$GRAFANA_AUTH" "$GRAFANA$1"; }

# dsquery <logql> [from] [to] — one panel query through Grafana's /api/ds/query,
# exactly as a dashboard panel issues it. from/to default to the dashboard's own
# 15-minute window and also accept epoch milliseconds. The expression is embedded
# with jq-free quoting: callers pass LogQL with \" already escaped for JSON.
dsquery() {
    local from="${2:-now-15m}" to="${3:-now}"
    curl -s "${CURL_TIMEOUTS[@]}" -u "$GRAFANA_AUTH" -H 'Content-Type: application/json' \
        -X POST "$GRAFANA/api/ds/query" \
        -d "{\"queries\":[{\"refId\":\"A\",\"datasource\":{\"type\":\"loki\",\"uid\":\"obs-loki\"},\"expr\":\"$1\",\"queryType\":\"range\",\"maxLines\":100,\"intervalMs\":1000,\"maxDataPoints\":100}],\"from\":\"$from\",\"to\":\"$to\"}"
}

# check_services asserts the exact set of running compose services. Called after
# startup and again at the end: a container that exits partway through leaves
# every earlier assertion true and every later one reading stale data.
check_services() {
    local label="$1" got
    got="$(dcq ps --services --filter status=running 2>/dev/null | sort | tr '\n' ' ' | sed 's/ *$//')"
    if [ "$got" = "$EXPECTED_SERVICES" ]; then
        log_pass "$label — running: $got"
    else
        log_fail "$label — running services are [$got], want [$EXPECTED_SERVICES]"
        dcq ps -a --format '{{.Service}}: {{.State}} ({{.Status}})' 2>/dev/null | sed 's/^/       /'
    fi
}

# chunk_count prints the number of persisted log chunk files. `docker compose cp`
# is used rather than `exec`, because the runtime image is distroless and has no
# shell to run `ls` in.
chunk_count() {
    local tmp
    tmp="$(mktemp -d)"
    if dcq cp backend:/data/logs/chunks "$tmp/chunks" >/dev/null 2>&1; then
        find "$tmp/chunks" -name '*.chunk' 2>/dev/null | wc -l
    else
        echo 0
    fi
    rm -rf "$tmp"
}

teardown() {
    local rc=$?
    if [ "$FAIL" -ne 0 ] || [ "$rc" -ne 0 ]; then
        echo ""
        echo "-- Container state (run failed) --"
        dcq ps -a 2>&1 | tail -20
        for svc in backend grafana sample-app; do
            echo ""
            echo "-- Last 40 log lines: $svc --"
            dcq logs --tail 40 "$svc" 2>&1 | tail -40
        done
    fi
    if [ "$KEEP_UP" = "1" ]; then
        echo ""
        echo "OBS_COMPOSE_KEEP_UP=1 — leaving the stack running; tear it down with:"
        echo "  docker compose -p $PROJECT -f $COMPOSE_FILE down -v"
        return
    fi
    echo ""
    echo "-- Tearing down --"
    dc down -v --remove-orphans >/dev/null 2>&1
}
trap teardown EXIT

# wait_for <label> <timeout-seconds> <command...> — polls until the command
# succeeds. Every readiness wait is bounded and reports its own failure, so a
# stack that never comes up fails here rather than as a confusing assertion
# failure further down.
wait_for() {
    local label="$1" timeout="$2"
    shift 2
    local start=$SECONDS
    while [ $((SECONDS - start)) -lt "$timeout" ]; do
        if "$@" >/dev/null 2>&1; then
            log_pass "$label ($((SECONDS - start))s)"
            return 0
        fi
        sleep 2
    done
    log_fail "$label — not ready after ${timeout}s"
    return 1
}

grafana_up()     { curl -sf "${CURL_TIMEOUTS[@]}" -u "$GRAFANA_AUTH" "$GRAFANA/api/health" | grep -q '"database": *"ok"'; }
datasource_ok()  { gapi /api/datasources/uid/obs-loki/health | grep -q '"status":"OK"'; }
sample_app_up()  { gapi /api/datasources/uid/obs-loki/resources/label/service/values | grep -q '"worker"'; }

echo "=== Phase 4.5 Compose stack test: project=$PROJECT (run_id=$RUN_ID) ==="

# ---- Preflight ------------------------------------------------------
if ! docker compose version >/dev/null 2>&1; then
    echo "FATAL: 'docker compose' is not available; this test needs Docker with the Compose plugin." >&2
    exit 2
fi
for port in 3000 8080; do
    if curl -s -o /dev/null --connect-timeout 2 --max-time 5 "http://localhost:$port" 2>/dev/null; then
        echo "FATAL: port $port is already serving. Stop the other stack (make local-down) and retry." >&2
        exit 2
    fi
done

# ---- Bring the stack up ---------------------------------------------
echo ""
echo "-- Starting the stack (building images; this can take a few minutes) --"
dc down -v --remove-orphans >/dev/null 2>&1
# Build output goes to a file rather than /dev/null: a build failure leaves no
# containers, so the container logs printed on teardown would be empty and the
# compiler error that actually caused it would be the one thing not shown.
BUILD_LOG="$(mktemp)"
if ! dc up -d --build >"$BUILD_LOG" 2>&1; then
    echo "FATAL: 'docker compose up --build' failed" >&2
    tail -60 "$BUILD_LOG" >&2
    rm -f "$BUILD_LOG"
    exit 2
fi
rm -f "$BUILD_LOG"
log_pass "compose up --build returned success"

echo ""
echo "-- Waiting for readiness --"
wait_for "Grafana is up" "$READY_TIMEOUT" grafana_up
# The datasource health check is Grafana's "Save & test" button: it proves
# Grafana resolves http://backend:8080 over the compose network and that the
# backend answers the Loki health query.
wait_for "Loki datasource health check passes (Save & test)" "$READY_TIMEOUT" datasource_ok
# Both demo services must actually be producing; a sample-app that crash-looped
# would otherwise show up only as an empty dashboard.
wait_for "sample-app is pushing streams" "$READY_TIMEOUT" sample_app_up

# `compose up` returning success only means the containers were created. This is
# the first point where all four are expected to be up and stable.
check_services "all four services running after startup"

# ---- Provisioning, as Grafana loaded it -----------------------------
echo ""
echo "-- Provisioned assets --"

BODY=$(gapi /api/datasources/uid/obs-loki)
check_contains "datasource provisioned — name" "$BODY" '"name":"observability-platform-logs"'
check_contains "datasource provisioned — type" "$BODY" '"type":"loki"'
check_contains "datasource provisioned — url reaches the backend service" "$BODY" '"url":"http://backend:8080"'

DASH=$(gapi /api/dashboards/uid/obs-logs-v1)
check_contains "dashboard provisioned — uid" "$DASH" '"uid":"obs-logs-v1"'
check_contains "dashboard provisioned — title" "$DASH" '"title":"Observability Platform Logs"'

# The expressions below are asserted against the dashboard Grafana actually
# serves, so the queries this script runs cannot drift from the ones the panels
# run. A panel edit that changes an expression fails here first.
PANEL1_EXPR='{service=\"$service\"} |= \"$search\"'
PANEL2_EXPR='{service=\"$service\", level=\"$level\"} |= \"$search\"'
check_contains "dashboard panel 1 expression is the one tested below" "$DASH" "$PANEL1_EXPR"
check_contains "dashboard panel 2 expression is the one tested below" "$DASH" "$PANEL2_EXPR"

PANEL3_EXPR='sum by (level) (count_over_time({service=\"$service\"} |= \"$search\" [$__interval]))'
check_contains "dashboard volume panel expression is the one tested below" "$DASH" "$PANEL3_EXPR"

# ---- Seed a marker stream -------------------------------------------
echo ""
echo "-- Seeding marker streams --"

# Every assertion that names a specific line or label value below reads this
# marker rather than the demo's own output. The sample app picks levels at
# random — error is 8% of api lines — so on a short run "level=error exists" is
# a coin flip. Seeding first makes those checks deterministic while the
# sample-app's own liveness is still proven separately, by the Service dropdown
# and by a panel query over its data.
NOW_NS=$(date +%s%N)
INFO_LINE="GET /api/v1/query 200 in 12ms run_id=$RUN_ID"
ERR_LINE="GET /api/v1/query 503 in 9ms run_id=$RUN_ID upstream timeout after 30s"
PUSH_STATUS=$(curl -s "${CURL_TIMEOUTS[@]}" -o /dev/null -w "%{http_code}" -X POST "$BACKEND/loki/api/v1/push" \
    -H "Content-Type: application/json" \
    -d "{\"streams\":[
        {\"stream\":{\"service\":\"compose-e2e\",\"level\":\"info\",\"env\":\"local\"},\"values\":[[\"$NOW_NS\",\"$INFO_LINE\"]]},
        {\"stream\":{\"service\":\"compose-e2e\",\"level\":\"error\",\"env\":\"local\"},\"values\":[[\"$NOW_NS\",\"$ERR_LINE\"]]}
    ]}")
if [ "$PUSH_STATUS" = "204" ]; then
    log_pass "seed marker streams (HTTP 204)"
else
    log_fail "seed marker streams — got HTTP $PUSH_STATUS"
fi

echo ""
echo "-- Template variables (through Grafana's datasource resource proxy) --"

# api and worker come from the sample app and appear on its first batch, so
# unlike the level values they need no seeding to be deterministic.
BODY=$(gapi /api/datasources/uid/obs-loki/resources/label/service/values)
check_contains "Service dropdown — api (sample-app)" "$BODY" '"api"'
check_contains "Service dropdown — worker (sample-app)" "$BODY" '"worker"'
check_contains "Service dropdown — the seeded marker" "$BODY" '"compose-e2e"'

BODY=$(gapi /api/datasources/uid/obs-loki/resources/label/level/values)
check_contains "Level dropdown — info" "$BODY" '"info"'
check_contains "Level dropdown — error" "$BODY" '"error"'

BODY=$(gapi /api/datasources/uid/obs-loki/resources/labels)
check_contains "label browser — env is discoverable" "$BODY" '"env"'

# ---- Panel queries through /api/ds/query ----------------------------
echo ""
echo "-- Panel queries (Grafana's /api/ds/query) --"

# Panel 1 with the variables Grafana would interpolate, including the empty
# Search box every viewer sees on first render.
BODY=$(dsquery '{service=\"compose-e2e\"} |= \"\"')
check_contains "panel 1 query returns rows — info line" "$BODY" "200 in 12ms run_id=$RUN_ID"
check_contains "panel 1 query returns rows — error line" "$BODY" "503 in 9ms run_id=$RUN_ID"

# Panel 2 narrows by the Level variable.
BODY=$(dsquery '{service=\"compose-e2e\", level=\"error\"} |= \"\"')
check_contains "panel 2 query narrows by level — error line kept" "$BODY" "503 in 9ms run_id=$RUN_ID"
check_absent   "panel 2 query narrows by level — info line excluded" "$BODY" "200 in 12ms run_id=$RUN_ID"

# A non-empty Search box.
BODY=$(dsquery '{service=\"compose-e2e\"} |= \"timeout\"')
check_contains "Search box filters — error line kept" "$BODY" "503 in 9ms run_id=$RUN_ID"
check_absent   "Search box filters — info line excluded" "$BODY" "200 in 12ms run_id=$RUN_ID"

# The demo's own data must reach the panels too, not just the seeded marker.
BODY=$(dsquery '{service=\"api\"} |= \"\"')
check_contains "panel 1 query returns sample-app rows" "$BODY" "request_id="

# Two different expressions produce this histogram, and both have to work:
#
#   1. the dashboard panel's own expression, interpolated as Grafana would
#      ($__interval resolves from panel width and time range)
#   2. the expression Explore generates for its log-volume histogram, which
#      appends `| drop __error__`
#
# Only (1) is pinned against the dashboard Grafana actually serves, above. An
# earlier version of this script ran (2) in (1)'s place, which left the panel's
# real expression untested while reading as though it were covered.
#
# On the assertions: this response is Grafana's dataframe JSON, not Loki's
# envelope, and its exact layout could not be observed while writing this (no
# Docker in the authoring environment). They pin things only a working grouped
# metric query produces — "info" is a seeded level value that appears nowhere in
# the request, so it cannot arrive by echo, and a numeric frame must carry a
# values array. The error check looks for the error *key*, not the bare word:
# this query groups by level, and one of those levels is literally "error".
# Tighten these against the real frame shape on the first Docker run.
BODY=$(dsquery 'sum by (level) (count_over_time({service=\"compose-e2e\"} |= \"\" [1m]))')
check_contains "volume panel query — info level series" "$BODY" '"info"'
check_contains "volume panel query — numeric frame data" "$BODY" '"values":[['
check_absent   "volume panel query — no datasource error" "$BODY" '"error":"'

# Explore's own form. Same data, one extra stage: if this 400s, the histogram
# above Explore's log lines is broken even while the dashboard panel renders.
BODY=$(dsquery 'sum by (level) (count_over_time({service=\"compose-e2e\"} |= \"\" | drop __error__[1m]))')
check_contains "Explore volume query (with drop) — info level series" "$BODY" '"info"'
check_absent   "Explore volume query (with drop) — no datasource error" "$BODY" '"error":"'

# ---- Storage path: chunks and restart readback ----------------------
echo ""
echo "-- Storage path --"

# The compose file lowers OBS_LOGS_FLUSH_THRESHOLD_BYTES to 16 KiB so the demo
# actually reaches the chunk/index path. Waiting for the count to rise proves
# the override is in effect and a flush completed inside the container.
BEFORE=$(chunk_count)
chunks_grew() { [ "$(chunk_count)" -gt "$BEFORE" ]; }
if wait_for "log chunks are written to disk (was $BEFORE)" "$FLUSH_TIMEOUT" chunks_grew; then
    AFTER=$(chunk_count)
    echo "     chunk files: $BEFORE -> $AFTER"
fi

# A flush checkpoints the WAL, so the marker pushed before it is now served from
# chunks rather than replayed from the WAL. Restarting proves the persisted path
# is readable, which is the phase's durability claim.
echo ""
echo "-- Restarting the backend --"
if dc restart backend >/dev/null 2>&1; then
    log_pass "backend container restarted"
else
    log_fail "backend container restart failed"
fi
wait_for "datasource health passes again after restart" "$READY_TIMEOUT" datasource_ok

BODY=$(dsquery '{service=\"compose-e2e\"} |= \"\"')
check_contains "marker survives the restart — info line" "$BODY" "200 in 12ms run_id=$RUN_ID"
check_contains "marker survives the restart — error line" "$BODY" "503 in 9ms run_id=$RUN_ID"

BODY=$(gapi /api/datasources/uid/obs-loki/resources/label/service/values)
check_contains "stream index survives the restart" "$BODY" '"compose-e2e"'

# ---- The demo is still live -----------------------------------------
echo ""
echo "-- Still live at the end of the run --"

# Every check so far could pass against a stack whose producers died right after
# startup: label values and seeded lines persist, so "the sample app ran once"
# and "the sample app is running" look identical. This window opens now, after
# the restart, so only lines written from here on can satisfy it — a producer
# that stopped earlier, or one the restart failed to reconnect, fails here.
FRESH_FROM_MS=$(( $(date +%s) * 1000 ))
FRESH_TO_MS=$(( FRESH_FROM_MS + 120000 ))
sample_app_producing_now() {
    dsquery '{service=\"api\"}' "$FRESH_FROM_MS" "$FRESH_TO_MS" | grep -q 'request_id='
}
wait_for "sample-app timestamps advance (new rows after the restart)" 60 sample_app_producing_now

# The demo's other producer writes metrics, not logs, so nothing above would
# notice it dying; and any service can exit between startup and here.
check_services "all four services still running at the end"

# ---- Summary --------------------------------------------------------
echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
