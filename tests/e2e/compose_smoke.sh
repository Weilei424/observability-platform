#!/usr/bin/env bash
#
# Real-stack test: brings up the Docker Compose demo and drives it through
# Grafana's own HTTP API, not the backend's. Covers both halves of the demo —
# the Loki datasource, logs dashboard, and storage path from Phase 4.5, and the
# Prometheus datasource plus both metric dashboards from Phase 5.1.
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

# jq parses Grafana's dataframe responses below. Substring matching cannot tell
# "two series came back" from "one did", nor an empty samples array from a full
# one, and both distinctions are the point of the volume-panel checks.
if ! command -v jq >/dev/null 2>&1; then
    echo "FATAL: jq is required (parses Grafana's /api/ds/query responses)." >&2
    echo "       Install it, or run the rest of the suite with: make smoke-logs" >&2
    exit 1
fi

PASS=0
FAIL=0
RUN_ID="run$(date +%s%N | tr 0-9 a-j)"

# The expected running set. Every service is asserted on its own data elsewhere
# in this run, but a producer can also exit between assertions, so the exact set
# is checked after startup and again at the end.
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

# promquery <expr> — one metrics panel query through Grafana's /api/ds/query,
# exactly as a dashboard panel issues it, over the dashboards' own 5-minute
# window. Callers pass PromQL with \" already escaped for JSON.
promquery() {
    curl -s "${CURL_TIMEOUTS[@]}" -u "$GRAFANA_AUTH" -H 'Content-Type: application/json' \
        -X POST "$GRAFANA/api/ds/query" \
        -d "{\"queries\":[{\"refId\":\"A\",\"datasource\":{\"type\":\"prometheus\",\"uid\":\"obs-prometheus\"},\"expr\":\"$1\",\"range\":true,\"intervalMs\":5000,\"maxDataPoints\":100}],\"from\":\"now-5m\",\"to\":\"now\"}"
}

# numeric_columns <body> — how many non-empty all-numeric arrays the dataframe
# response carries. One series contributes two (timestamps and values), so this
# distinguishes "a frame came back" from "a frame came back with samples in it",
# which substring matching cannot.
numeric_columns() {
    printf '%s' "$1" | jq '[.. | arrays | select(length > 0) | select(all(.[]; type == "number"))] | length' 2>/dev/null || echo 0
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
prom_datasource_ok()   { gapi /api/datasources/uid/obs-prometheus/health | grep -q '"status":"OK"'; }

# A present-but-empty dataframe would satisfy a substring check on "values" —
# exactly what the numeric_columns comment above calls out — so this waits for
# at least 2 non-empty numeric columns (time+value) instead, the same bar the
# stricter checks later in this script hold metrics queries to.
sample_app_metrics_up() {
    local cols
    cols="$(numeric_columns "$(promquery 'sample_app_active_workers')")"
    [ "${cols:-0}" -ge 2 ]
}

echo "=== Compose stack test: project=$PROJECT (run_id=$RUN_ID) ==="

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
# The metrics counterpart of the Loki health check: Grafana's "Save & test" for
# the Prometheus datasource, which proves it resolves http://backend:8080 over
# the compose network and that the backend answers the Prometheus health query.
wait_for "Prometheus datasource health check passes (Save & test)" "$READY_TIMEOUT" prom_datasource_ok
# The sample app now emits metrics as well as logs; without this a crash-looping
# metrics half would show up only as an empty dashboard.
wait_for "sample-app is pushing metrics" "$READY_TIMEOUT" sample_app_metrics_up
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

BODY=$(gapi /api/datasources/uid/obs-prometheus)
check_contains "prometheus datasource provisioned — name" "$BODY" '"name":"observability-platform"'
check_contains "prometheus datasource provisioned — type" "$BODY" '"type":"prometheus"'
check_contains "prometheus datasource provisioned — url reaches the backend service" "$BODY" '"url":"http://backend:8080"'

MDASH=$(gapi /api/dashboards/uid/obs-metrics-v1)
check_contains "metrics dashboard provisioned — uid" "$MDASH" '"uid":"obs-metrics-v1"'
check_contains "metrics dashboard provisioned — title" "$MDASH" '"title":"Observability Platform Metrics"'

SDASH=$(gapi /api/dashboards/uid/obs-sample-app-v1)
check_contains "sample-app dashboard provisioned — uid" "$SDASH" '"uid":"obs-sample-app-v1"'
check_contains "sample-app dashboard provisioned — title" "$SDASH" '"title":"Observability Platform Sample App"'

# As with the logs panels, the expressions run below are pinned against the
# dashboards Grafana actually serves, so a panel edit fails here first.
LOADGEN_EXPR='sum by (method)(rate(http_requests_total[1m]))'
check_contains "metrics dashboard panel expression is the one tested below" "$MDASH" "$LOADGEN_EXPR"
SAMPLE_RATE_EXPR='sum by (method)(rate(sample_app_requests_total[1m]))'
check_contains "sample-app dashboard panel expression is the one tested below" "$SDASH" "$SAMPLE_RATE_EXPR"
# The bare expression, not a '"expr": ...' fragment: Grafana re-serializes the
# dashboard, so its key spacing is its own and must not be assumed.
check_contains "sample-app dashboard workers panel expression is the one tested below" "$SDASH" 'sample_app_active_workers'

# ---- Seed a marker stream -------------------------------------------
echo ""
echo "-- Seeding markers --"

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

# A metric marker for the restart check further down. Its value is unique to this
# run and nothing writes this series afterwards, so reading that exact value back
# after the restart proves the persisted path rather than the producer merely
# having resumed. RANDOM keeps it distinct even if a previous run's volume
# somehow survived (OBS_COMPOSE_KEEP_UP skips the `down -v` teardown).
MARKER_VALUE=$(( (RANDOM % 90000) + 10000 ))
MARKER_MS=$(( $(date +%s) * 1000 ))
METRIC_STATUS=$(curl -s "${CURL_TIMEOUTS[@]}" -o /dev/null -w "%{http_code}" -X POST "$BACKEND/api/v1/ingest/metrics" \
    -H "Content-Type: application/json" \
    -d "{\"metrics\":[{\"name\":\"compose_e2e_marker\",\"labels\":{\"run_id\":\"$RUN_ID\"},\"timestamp_ms\":$MARKER_MS,\"value\":$MARKER_VALUE}]}")
if [ "$METRIC_STATUS" = "204" ]; then
    log_pass "seed metric marker (HTTP 204, value $MARKER_VALUE)"
else
    log_fail "seed metric marker — got HTTP $METRIC_STATUS"
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
# The error check below looks for the error *key*, not the bare word: this query
# groups by level, and one of those levels is literally "error".
BODY=$(dsquery 'sum by (level) (count_over_time({service=\"compose-e2e\"} |= \"\" [1m]))')
check_absent "volume panel query — no datasource error" "$BODY" '"error":"'

# Parse the multi-series response rather than substring-matching it. Substrings
# cannot make the two distinctions that matter here: whether BOTH series came
# back (whichever one is missing, the other still supplies every match), and
# whether their samples are actually there ('"values":[[' matches an empty
# array just as happily as a full one). Querying each level separately does not
# cover it either — Grafana could drop or merge a frame only while handling a
# genuinely multi-series result, which is the case the panel actually issues.
#
# The filters use recursive descent instead of a fixed path, so they assert what
# must hold of Grafana's dataframe JSON without pinning its exact nesting. If a
# filter ever mismatches the real shape it fails loudly and prints the body, so
# the log shows what to correct — the one failure direction worth having.
LEVELS=$(printf '%s' "$BODY" | jq -c '[.. | objects | select(has("level")) | .level] | unique' 2>/dev/null || echo 'jq-error')
if [ "$LEVELS" = '["error","info"]' ]; then
    log_pass "volume panel query — exactly the info and error series returned"
else
    log_fail "volume panel query — level series are $LEVELS, want [\"error\",\"info\"]; body: $BODY"
fi

# Every frame contributes a timestamp column and a values column, both numeric
# and both non-empty when the series has samples. Two series therefore mean at
# least four non-empty numeric columns; a frame whose samples are empty drops
# the count below that.
NUMCOLS=$(numeric_columns "$BODY")
if [ "${NUMCOLS:-0}" -ge 4 ]; then
    log_pass "volume panel query — both series carry non-empty numeric samples"
else
    log_fail "volume panel query — ${NUMCOLS:-0} non-empty numeric columns, want >= 4 (two series x time+value); body: $BODY"
fi

# Explore's own form. Same data, one extra stage: if this 400s, the histogram
# above Explore's log lines is broken even while the dashboard panel renders.
BODY=$(dsquery 'sum by (level) (count_over_time({service=\"compose-e2e\"} |= \"\" | drop __error__[1m]))')
check_contains "Explore volume query (with drop) — info level series" "$BODY" '"info"'
check_absent   "Explore volume query (with drop) — no datasource error" "$BODY" '"error":"'

echo ""
echo "-- Metrics panel queries (Grafana's /api/ds/query) --"

# Bare selectors, not rate(): rate needs two samples inside its window, so an
# assertion on it would depend on how long the stack has been up. These prove
# the series exist and carry samples; the rate expression is checked separately
# for the absence of an error.
BODY=$(promquery 'sample_app_active_workers')
check_absent "sample-app metrics query — no datasource error" "$BODY" '"error":"'
COLS=$(numeric_columns "$BODY")
if [ "${COLS:-0}" -ge 2 ]; then
    log_pass "sample-app metrics query — the series carries non-empty numeric samples"
else
    log_fail "sample-app metrics query — ${COLS:-0} non-empty numeric columns, want >= 2 (time+value); body: $BODY"
fi

# The load generator has produced metrics since Phase 2.5 and has never had a
# data-level assertion anywhere — only the running-service check would notice if
# it died mid-run while still leaving its earlier samples queryable.
BODY=$(promquery 'http_requests_total')
check_absent "load-generator metrics query — no datasource error" "$BODY" '"error":"'
COLS=$(numeric_columns "$BODY")
if [ "${COLS:-0}" -ge 2 ]; then
    log_pass "load-generator metrics query — the series carries non-empty numeric samples"
else
    log_fail "load-generator metrics query — ${COLS:-0} non-empty numeric columns, want >= 2 (time+value); body: $BODY"
fi

# The panel expressions themselves. Their values depend on uptime, so only the
# absence of a datasource error is asserted — the same rule the volume panel
# check follows.
BODY=$(promquery 'sum by (method)(rate(sample_app_requests_total[1m]))')
check_absent "sample-app rate panel — no datasource error" "$BODY" '"error":"'
BODY=$(promquery 'sum by (method)(rate(http_requests_total[1m]))')
check_absent "metrics dashboard rate panel — no datasource error" "$BODY" '"error":"'

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
wait_for "prometheus datasource health passes again after restart" "$READY_TIMEOUT" prom_datasource_ok

# The seeded marker is what makes this a persistence claim. Querying a live
# series such as sample_app_active_workers cannot prove persistence: the sample
# app keeps pushing throughout the restart, so fresh samples would satisfy the
# assertion even if every pre-restart sample had been lost. Nothing writes the
# marker after the restart, so its value can only come from persisted data.
BODY=$(promquery 'compose_e2e_marker')
check_absent "metric marker survives the restart — no datasource error" "$BODY" '"error":"'
if printf '%s' "$BODY" | jq -e --argjson want "$MARKER_VALUE" \
        '[.. | arrays | select(length > 0) | select(all(.[]; type == "number")) | .[]] | index($want)' >/dev/null 2>&1; then
    log_pass "metric marker survives the restart — value $MARKER_VALUE read back"
else
    log_fail "metric marker survives the restart — value $MARKER_VALUE absent from response; body: $BODY"
fi

# Separately, the producer must still be writing. This is a LIVENESS claim, not a
# persistence one — keeping the two apart is the point of the marker above.
BODY=$(promquery 'sample_app_active_workers')
COLS=$(numeric_columns "$BODY")
if [ "${COLS:-0}" -ge 2 ]; then
    log_pass "sample-app metrics still readable after the restart"
else
    log_fail "sample-app metrics still readable after the restart — ${COLS:-0} non-empty numeric columns, want >= 2; body: $BODY"
fi

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
