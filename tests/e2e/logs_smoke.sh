#!/usr/bin/env bash
set -euo pipefail

# Every check here is a live HTTP request against a running backend. The static
# half of the demo — that loki.yml and logs.json are the provisioning files
# Grafana needs — is validated by tests/e2e/provisioning_test.go, which needs no
# backend and runs in `go test ./...`. `make smoke-logs` runs both.
BACKEND="${BACKEND_ADDR:-http://localhost:8080}"
PASS=0
FAIL=0

# Every line carries this token and every assertion filters on it, so the script
# never races live sample-app data and repeated runs cannot interfere.
RUN_ID="run$(date +%s%N | tr 0-9 a-j)"
INFO_LINE="GET /api/v1/query 200 in 12ms run_id=$RUN_ID"
ERR_LINE="GET /api/v1/query 503 in 9ms run_id=$RUN_ID upstream timeout after 30s"

log_pass() { echo "  PASS: $1"; PASS=$((PASS + 1)); }
log_fail() { echo "  FAIL: $1"; FAIL=$((FAIL + 1)); }

check_contains() {
    local label="$1" body="$2" needle="$3"
    if printf '%s' "$body" | grep -qF -- "$needle"; then
        log_pass "$label"
    else
        log_fail "$label — missing '$needle' in: $body"
    fi
}

check_absent() {
    local label="$1" body="$2" needle="$3"
    if [ "$body" = "curl-error" ]; then
        log_fail "$label — query failed, no response body"
    elif printf '%s' "$body" | grep -qF -- "$needle"; then
        log_fail "$label — unexpected '$needle' in: $body"
    else
        log_pass "$label"
    fi
}

echo "=== logs smoke test: $BACKEND (run_id=$RUN_ID) ==="

# ---- Push -----------------------------------------------------------
echo ""
echo "-- Pushing log streams --"

NOW_NS=$(date +%s%N)
PAYLOAD=$(cat <<EOF
{"streams":[
  {"stream":{"service":"smoke-test","level":"info","env":"local"},
   "values":[["$NOW_NS","$INFO_LINE"]]},
  {"stream":{"service":"smoke-test","level":"error","env":"local"},
   "values":[["$NOW_NS","$ERR_LINE"]]}
]}
EOF
)

HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    -X POST "$BACKEND/loki/api/v1/push" \
    -H "Content-Type: application/json" \
    -d "$PAYLOAD" || true)

if [ "$HTTP_STATUS" = "204" ]; then
    log_pass "push log streams (HTTP 204)"
else
    log_fail "push log streams — got HTTP $HTTP_STATUS"
fi

# ---- Queries --------------------------------------------------------
echo ""
echo "-- Querying --"

NOW_S=$(date +%s)
START_S=$(( NOW_S - 300 ))
END_S=$(( NOW_S + 60 ))   # half-open [start, end): end must be past the entries

# lokq <logql> — range query over the smoke window.
lokq() {
    curl -s -G "$BACKEND/loki/api/v1/query_range" \
        --data-urlencode "query=$1" \
        --data-urlencode "start=$START_S" \
        --data-urlencode "end=$END_S" \
        --data-urlencode "limit=100" || echo 'curl-error'
}

BODY=$(lokq '{service="smoke-test"}')
check_contains "label-only query — streams envelope" "$BODY" '"resultType":"streams"'
check_contains "label-only query — info line" "$BODY" "200 in 12ms run_id=$RUN_ID"
check_contains "label-only query — error line" "$BODY" "503 in 9ms run_id=$RUN_ID"

BODY=$(lokq '{service="smoke-test", level="error"}')
check_contains "level=error — error line kept" "$BODY" "503 in 9ms run_id=$RUN_ID"
check_absent   "level=error — info line excluded" "$BODY" "200 in 12ms run_id=$RUN_ID"

BODY=$(lokq '{service="smoke-test"} |= "timeout"')
check_contains "line filter |= — error line kept" "$BODY" "503 in 9ms run_id=$RUN_ID"
check_absent   "line filter |= — info line excluded" "$BODY" "200 in 12ms run_id=$RUN_ID"

# LogQL lexes strings with Go's rules, so the regex needs doubled backslashes.
BODY=$(lokq '{service="smoke-test"} |~ "5\\d\\d"')
check_contains "line filter |~ — 5xx line kept" "$BODY" "503 in 9ms run_id=$RUN_ID"
check_absent   "line filter |~ — 200 line excluded" "$BODY" "200 in 12ms run_id=$RUN_ID"

# Every viewer's first dashboard render is an empty Search box, i.e. |= "". The
# runbook states this matches every line — assert it actually does.
BODY=$(lokq '{service="smoke-test"} |= ""')
check_contains "empty line filter |= \"\" — streams envelope" "$BODY" '"resultType":"streams"'
check_contains "empty line filter |= \"\" — info line kept" "$BODY" "200 in 12ms run_id=$RUN_ID"
check_contains "empty line filter |= \"\" — error line kept" "$BODY" "503 in 9ms run_id=$RUN_ID"

# ---- Label discovery ------------------------------------------------
echo ""
echo "-- Label discovery --"

BODY=$(curl -s "$BACKEND/loki/api/v1/labels" || echo 'curl-error')
check_contains "/labels — service" "$BODY" '"service"'
check_contains "/labels — level" "$BODY" '"level"'
check_contains "/labels — env" "$BODY" '"env"'

BODY=$(curl -s "$BACKEND/loki/api/v1/label/service/values" || echo 'curl-error')
check_contains "/label/service/values — smoke-test" "$BODY" '"smoke-test"'

# ---- Grafana datasource health check --------------------------------
echo ""
echo "-- Grafana compatibility --"

# The exact request Grafana 11.1.0's pkg/tsdb/loki/healthcheck.go sends. This is
# the automated stand-in for the datasource "Save & test" button going green.
BODY=$(curl -s -G "$BACKEND/loki/api/v1/query" \
    --data-urlencode "query=vector(1)+vector(1)" \
    --data-urlencode "time=4000000000" \
    --data-urlencode "direction=backward" || echo 'curl-error')
check_contains "datasource health check — vector envelope" "$BODY" '"resultType":"vector"'
check_contains "datasource health check — value 2" "$BODY" ',"2"]'

# Explore's log-volume histogram sends this shape. Phase 4.6 answers it, so the
# check that used to assert a 400 now asserts the matrix.
#
# The line filter on the run token is load-bearing: repeated smoke runs push into
# the same service="smoke-test" streams, so an unfiltered count would grow between
# runs and could not assert a value. It also exercises a line filter inside a
# metric query.
BODY=$(lokq 'sum by (level) (count_over_time({service="smoke-test"} |= "run_id='"$RUN_ID"'" [5m]))')
check_contains "log-volume query — matrix envelope" "$BODY" '"resultType":"matrix"'
check_contains "log-volume query — info group" "$BODY" '"level":"info"'
check_contains "log-volume query — error group" "$BODY" '"level":"error"'
check_contains "log-volume query — one line per level" "$BODY" '"1"'

BODY=$(lokq 'rate({service="smoke-test"} |= "run_id='"$RUN_ID"'" [5m])')
check_contains "rate query — matrix envelope" "$BODY" '"resultType":"matrix"'

# The guardrail keeps a live test: what is still outside the subset must still
# fail loudly rather than return a plausible-looking number.
CODE=$(curl -s -o /dev/null -w "%{http_code}" -G "$BACKEND/loki/api/v1/query_range" \
    --data-urlencode 'query=avg_over_time({service="smoke-test"} | unwrap duration [5m])' \
    --data-urlencode "start=$START_S" \
    --data-urlencode "end=$END_S" || true)
if [ "$CODE" = "400" ]; then
    log_pass "unsupported metric LogQL still rejected with 400"
else
    log_fail "unsupported metric LogQL — expected HTTP 400, got $CODE"
fi

# ---- Summary --------------------------------------------------------
echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
