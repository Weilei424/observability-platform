#!/usr/bin/env bash
set -euo pipefail

BACKEND="${BACKEND_ADDR:-http://localhost:8080}"
PASS=0
FAIL=0

log_pass() { echo "  PASS: $1"; PASS=$((PASS + 1)); }
log_fail() { echo "  FAIL: $1"; FAIL=$((FAIL + 1)); }

check_success() {
    local label="$1" body="$2"
    if echo "$body" | grep -q '"status":"success"'; then
        log_pass "$label"
    else
        log_fail "$label — response: $body"
    fi
}

# Like check_success but also verifies the result is non-empty (catches rate() returning empty vector).
check_nonempty_success() {
    local label="$1" body="$2"
    if ! echo "$body" | grep -q '"status":"success"'; then
        log_fail "$label — not success: $body"
    elif echo "$body" | grep -q '"result":\[\]'; then
        log_fail "$label — empty result (no data in rate window)"
    else
        log_pass "$label"
    fi
}

echo "=== Phase 2.5 smoke test: $BACKEND ==="

# ---- Inject samples ------------------------------------------------
echo ""
echo "-- Injecting samples --"

NOW_MS=$(( $(date +%s) * 1000 ))
PREV_MS=$(( NOW_MS - 50000 ))    # 50 seconds ago — both samples within the [1m] rate window

PAYLOAD=$(cat <<EOF
{
  "metrics": [
    {"name":"http_requests_total","labels":{"service":"api","method":"GET","status":"200"},"timestamp_ms":$PREV_MS,"value":10},
    {"name":"http_requests_total","labels":{"service":"api","method":"GET","status":"200"},"timestamp_ms":$NOW_MS,"value":20},
    {"name":"http_requests_total","labels":{"service":"api","method":"POST","status":"201"},"timestamp_ms":$PREV_MS,"value":5},
    {"name":"http_requests_total","labels":{"service":"api","method":"POST","status":"201"},"timestamp_ms":$NOW_MS,"value":12},
    {"name":"http_errors_total","labels":{"service":"api","method":"GET","status":"500"},"timestamp_ms":$PREV_MS,"value":0},
    {"name":"http_errors_total","labels":{"service":"api","method":"GET","status":"500"},"timestamp_ms":$NOW_MS,"value":1},
    {"name":"http_request_duration_seconds","labels":{"service":"api","method":"GET"},"timestamp_ms":$NOW_MS,"value":0.042},
    {"name":"http_request_duration_seconds","labels":{"service":"api","method":"POST"},"timestamp_ms":$NOW_MS,"value":0.120},
    {"name":"active_connections","labels":{"service":"api"},"timestamp_ms":$NOW_MS,"value":14}
  ]
}
EOF
)

HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    -X POST "$BACKEND/api/v1/ingest/metrics" \
    -H "Content-Type: application/json" \
    -d "$PAYLOAD")

if [ "$HTTP_STATUS" = "204" ] || [ "$HTTP_STATUS" = "200" ]; then
    log_pass "ingest samples (HTTP $HTTP_STATUS)"
else
    log_fail "ingest samples — got HTTP $HTTP_STATUS"
fi

# ---- Query assertions ----------------------------------------------
echo ""
echo "-- Querying --"

NOW_S=$(date +%s)
START_S=$(( NOW_S - 300 ))

# Panel 1: Request Rate by Method (range query)
BODY=$(curl -s -G "$BACKEND/api/v1/query_range" \
    --data-urlencode "query=sum by (method)(rate(http_requests_total[1m]))" \
    --data-urlencode "start=$START_S" \
    --data-urlencode "end=$NOW_S" \
    --data-urlencode "step=30" || echo '{"status":"curl-error"}')
check_nonempty_success "Panel 1 — sum by (method)(rate(http_requests_total[1m]))" "$BODY"

# Panel 2: Error Rate (instant query)
BODY=$(curl -s -G "$BACKEND/api/v1/query" \
    --data-urlencode "query=rate(http_errors_total[1m])" \
    --data-urlencode "time=$NOW_S" || echo '{"status":"curl-error"}')
check_nonempty_success "Panel 2 — rate(http_errors_total[1m])" "$BODY"

# Panel 3: Total RPS (instant query)
BODY=$(curl -s -G "$BACKEND/api/v1/query" \
    --data-urlencode "query=sum(rate(http_requests_total[1m]))" \
    --data-urlencode "time=$NOW_S" || echo '{"status":"curl-error"}')
check_nonempty_success "Panel 3 — sum(rate(http_requests_total[1m]))" "$BODY"

# Panel 4: Request Duration (instant query)
BODY=$(curl -s -G "$BACKEND/api/v1/query" \
    --data-urlencode "query=http_request_duration_seconds" \
    --data-urlencode "time=$NOW_S" || echo '{"status":"curl-error"}')
check_success "Panel 4 — http_request_duration_seconds" "$BODY"

# Panel 5: Active Connections (instant query)
BODY=$(curl -s -G "$BACKEND/api/v1/query" \
    --data-urlencode "query=active_connections" \
    --data-urlencode "time=$NOW_S" || echo '{"status":"curl-error"}')
check_success "Panel 5 — active_connections" "$BODY"

# ---- Summary -------------------------------------------------------
echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
