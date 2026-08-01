#!/usr/bin/env bash
set -euo pipefail

# Resolve paths relative to this script's own location so both `make smoke-logs`
# (cwd = repo root) and a direct `bash tests/e2e/logs_smoke.sh` from anywhere work.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

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

echo "=== Phase 4.5 logs smoke test: $BACKEND (run_id=$RUN_ID) ==="

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

# Explore's log-volume histogram sends this. Rejecting it is the documented
# guardrail "unsupported LogQL features return explicit errors" (Phase 4.6 closes
# the gap). Status only: this query takes the generic parse-error branch, not the
# targeted metric-query one, so the message text would pin the wrong branch.
CODE=$(curl -s -o /dev/null -w "%{http_code}" -G "$BACKEND/loki/api/v1/query_range" \
    --data-urlencode 'query=sum by (level) (count_over_time({service="smoke-test"}[5m]))' \
    --data-urlencode "start=$START_S" \
    --data-urlencode "end=$END_S" || true)
if [ "$CODE" = "400" ]; then
    log_pass "metric LogQL rejected with 400 (log-volume gap, by design)"
else
    log_fail "metric LogQL — expected HTTP 400, got $CODE"
fi

# ---- Grafana provisioning files (static, no backend required) -------
echo ""
echo "-- Grafana provisioning files --"

# This script stands in for manually clicking through Grafana. Without this
# section, a typo in loki.yml or a malformed logs.json would leave every check
# above green while Grafana actually shows "datasource not found" — so these
# checks read the two provisioning files directly, no backend involved.
LOKI_YML="$REPO_ROOT/observability/grafana/datasources/loki.yml"
LOGS_JSON="$REPO_ROOT/observability/grafana/dashboards/logs.json"

# The validator's output is captured rather than piped through process
# substitution, because a process substitution's exit status is invisible to the
# script: if python3 or PyYAML were missing, the loop would read nothing, emit no
# results, and the run would still report all-green having silently skipped every
# provisioning check. A check that cannot fail when its own machinery is absent
# is not a check.
PROV_RC=0
PROV_OUT=$(python3 - "$LOKI_YML" "$LOGS_JSON" <<'PY'
import json
import sys

import yaml

loki_path, dash_path = sys.argv[1], sys.argv[2]
results = []


def ok(label):
    results.append(("PASS", label, ""))


def bad(label, detail):
    results.append(("FAIL", label, detail))


# loki.yml parses as YAML and its datasource has type: loki and uid: obs-loki.
ds_uid = None
try:
    with open(loki_path) as f:
        doc = yaml.safe_load(f)
    datasources = (doc or {}).get("datasources") or []
    if not datasources:
        bad("loki.yml — parses as YAML with a datasource entry", "no datasources entry")
    else:
        ok("loki.yml — parses as YAML")
        ds = datasources[0]
        if ds.get("type") == "loki":
            ok("loki.yml — datasource type is loki")
        else:
            bad("loki.yml — datasource type is loki", "got type=%r" % (ds.get("type"),))
        ds_uid = ds.get("uid")
        if ds_uid == "obs-loki":
            ok("loki.yml — datasource uid is obs-loki")
        else:
            bad("loki.yml — datasource uid is obs-loki", "got uid=%r" % (ds_uid,))
except Exception as e:
    bad("loki.yml — parses as YAML", str(e))

# logs.json parses as JSON and its uid is obs-logs-v1.
dash = None
try:
    with open(dash_path) as f:
        dash = json.load(f)
    ok("logs.json — parses as JSON")
    dash_uid = dash.get("uid")
    if dash_uid == "obs-logs-v1":
        ok("logs.json — dashboard uid is obs-logs-v1")
    else:
        bad("logs.json — dashboard uid is obs-logs-v1", "got uid=%r" % (dash_uid,))
except Exception as e:
    bad("logs.json — parses as JSON", str(e))

# Every panel target's datasource uid in logs.json must equal the datasource's
# uid from loki.yml — a real cross-reference, not two independent constants.
if dash is not None and ds_uid is not None:
    mismatches = []
    target_count = 0
    for panel in dash.get("panels", []):
        for target in panel.get("targets", []):
            target_count += 1
            t_uid = (target.get("datasource") or {}).get("uid")
            if t_uid != ds_uid:
                mismatches.append(
                    "panel %r target %r: uid=%r" % (panel.get("id"), target.get("refId"), t_uid)
                )
    label = "logs.json — panel target datasource uids match loki.yml's %r" % (ds_uid,)
    if target_count == 0:
        bad(label, "no panel targets found")
    elif mismatches:
        bad(label, "; ".join(mismatches))
    else:
        ok("logs.json — all %d panel target(s) match loki.yml datasource uid" % (target_count,))
else:
    bad(
        "logs.json — panel target datasource uids match loki.yml's datasource uid",
        "skipped: loki.yml or logs.json failed to parse above",
    )

for status, label, detail in results:
    print("%s|%s|%s" % (status, label, detail))
PY
) || PROV_RC=$?

if [ "$PROV_RC" -ne 0 ] || [ -z "$PROV_OUT" ]; then
    log_fail "Grafana provisioning checks — validator did not run (exit $PROV_RC, $(printf '%s' "$PROV_OUT" | wc -l) lines); is python3 with PyYAML available?"
else
    while IFS='|' read -r status label detail; do
        [ -z "$status" ] && continue
        if [ "$status" = "PASS" ]; then
            log_pass "$label"
        else
            log_fail "$label — $detail"
        fi
    done <<< "$PROV_OUT"
fi

# ---- Summary --------------------------------------------------------
echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
