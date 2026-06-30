#!/usr/bin/env bash
# Hermetic k6 runner: build -> start backend on a temp data dir -> seed ->
# run k6 scenarios -> collect JSON summaries -> tear down.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

RESULTS_DIR="$ROOT/bench/results"
K6_DIR="$ROOT/bench/k6"
ADDR="${OBS_HTTP_ADDR:-127.0.0.1:8089}"
export BASE_URL="http://${ADDR}"

# Profiles (override via env). Defaults are a short smoke profile.
SEED_VUS="${SEED_VUS:-10}"
SEED_DURATION="${SEED_DURATION:-15s}"
RUN_VUS="${RUN_VUS:-10}"
RUN_DURATION="${RUN_DURATION:-20s}"
export CARDINALITY="${CARDINALITY:-1000}"
export BATCH="${BATCH:-100}"

# 1. Resolve k6.
if ! command -v k6 >/dev/null 2>&1; then
  if [ -x "$(go env GOPATH)/bin/k6" ]; then
    export PATH="$(go env GOPATH)/bin:$PATH"
  else
    echo "k6 not found. Install with:" >&2
    echo "  go install go.k6.io/k6@latest" >&2
    exit 1
  fi
fi

mkdir -p "$RESULTS_DIR"

# 2. Build the server.
BIN_DIR="$(mktemp -d)"
BIN="$BIN_DIR/obs-server"
echo ">> building server"
go build -o "$BIN" ./cmd/server

# 3. Start the server on a fresh data dir.
DATA_DIR="$(mktemp -d)"
echo ">> starting server addr=$ADDR data_dir=$DATA_DIR"
OBS_HTTP_ADDR="$ADDR" OBS_DATA_DIR="$DATA_DIR" OBS_LOG_LEVEL=warn "$BIN" &
SERVER_PID=$!

cleanup() {
  echo ">> stopping server"
  kill "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
  rm -rf "$DATA_DIR" "$BIN_DIR"
}
trap cleanup EXIT

# 4. Wait for readiness.
echo ">> waiting for /readyz"
ready=0
for _ in $(seq 1 50); do
  if curl -fsS "${BASE_URL}/readyz" >/dev/null 2>&1; then ready=1; break; fi
  sleep 0.2
done
if [ "$ready" -ne 1 ]; then
  echo "server did not become ready at ${BASE_URL}/readyz" >&2
  exit 1
fi

# run_k6 runs one scenario, tolerating a thresholds-breached exit (k6 code 99)
# as a non-fatal warning so a slow/loaded box still records result JSON; any
# other non-zero exit (script error, connection failure) aborts the run.
run_k6() {
  local script=$1 vus=$2 dur=$3
  set +e
  VUS="$vus" DURATION="$dur" k6 run "$script"
  local rc=$?
  set -e
  if [ "$rc" -eq 99 ]; then
    echo "  ⚠ k6 thresholds breached for $(basename "$script") (non-fatal; numbers recorded)"
  elif [ "$rc" -ne 0 ]; then
    echo "k6 failed for $(basename "$script") (exit $rc)" >&2
    exit "$rc"
  fi
}

# 5. Seed + measure ingest.
echo ">> k6 ingest (seed + throughput)"
run_k6 "$K6_DIR/ingest.js" "$SEED_VUS" "$SEED_DURATION"

# 6. Query scenarios.
echo ">> k6 instant_query"
run_k6 "$K6_DIR/instant_query.js" "$RUN_VUS" "$RUN_DURATION"

echo ">> k6 range_query"
run_k6 "$K6_DIR/range_query.js" "$RUN_VUS" "$RUN_DURATION"

echo ">> done. summaries:"
ls -1 "$RESULTS_DIR"
