#!/usr/bin/env bash
# Hermetic k6 runner: build -> start backend on a temp data dir -> seed ->
# run k6 scenarios -> collect JSON summaries -> tear down.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

RESULTS_DIR="$ROOT/bench/results"
K6_DIR="$ROOT/bench/k6"

# Listen address: honor a pinned OBS_HTTP_ADDR, else bind an OS-assigned free
# ephemeral port (":0"). Binding ":0" lets the kernel hand our own process a
# guaranteed-free port with no probe and no TOCTOU window; the server reports the
# actual bound address via OBS_ADDR_FILE, which we read back below. We therefore
# only ever talk to the address our own process reported — never an assumed port a
# foreign backend might occupy.
LISTEN_ADDR="${OBS_HTTP_ADDR:-127.0.0.1:0}"

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

# 2. Pre-flight guard (only meaningful for a pinned address): refuse to run if a
# backend already answers there, so we neither benchmark nor clobber a foreign
# process. Runs before any temp files exist, so an early abort leaves nothing
# behind. (For the default ":0" the kernel guarantees a free port, so there is
# nothing to pre-check.)
if [ -n "${OBS_HTTP_ADDR:-}" ] && curl -fsS --max-time 2 "http://${OBS_HTTP_ADDR}/readyz" >/dev/null 2>&1; then
  echo "a backend is already serving http://${OBS_HTTP_ADDR}/readyz — refusing to benchmark a foreign process. Free the address or unset OBS_HTTP_ADDR." >&2
  exit 1
fi

# 3. Create the temp workspace and install cleanup immediately, so every later
# exit path (build failure, bind failure, k6 error) tears it down.
BIN_DIR="$(mktemp -d)"
DATA_DIR="$(mktemp -d)"
ADDR_FILE="$(mktemp)"
SERVER_PID=""
cleanup() {
  if [ -n "$SERVER_PID" ]; then
    echo ">> stopping server"
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  rm -rf "$DATA_DIR" "$BIN_DIR" "$ADDR_FILE"
}
trap cleanup EXIT

# 4. Build the server.
BIN="$BIN_DIR/obs-server"
echo ">> building server"
go build -o "$BIN" ./cmd/server

# 5. Start the server. It binds LISTEN_ADDR (":0" → ephemeral) and writes the
# actual bound address to OBS_ADDR_FILE once listening.
echo ">> starting server listen=$LISTEN_ADDR data_dir=$DATA_DIR"
OBS_HTTP_ADDR="$LISTEN_ADDR" OBS_ADDR_FILE="$ADDR_FILE" OBS_DATA_DIR="$DATA_DIR" OBS_LOG_LEVEL=warn "$BIN" &
SERVER_PID=$!

# 6. Discover the bound address. A bind failure exits the server synchronously
# before it writes the file, so a dead PID here unambiguously means bind failed.
echo ">> waiting for bound address"
for _ in $(seq 1 50); do
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "server exited before binding ${LISTEN_ADDR} (address already in use?)" >&2
    exit 1
  fi
  if [ -s "$ADDR_FILE" ]; then break; fi
  sleep 0.1
done
if [ ! -s "$ADDR_FILE" ]; then
  echo "server did not report a bound address within timeout" >&2
  exit 1
fi
ADDR="$(tr -d '[:space:]' < "$ADDR_FILE")"
export BASE_URL="http://${ADDR}"

# 7. Wait for readiness.
echo ">> waiting for /readyz at $BASE_URL"
ready=0
for _ in $(seq 1 50); do
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "server process $SERVER_PID exited before becoming ready" >&2
    exit 1
  fi
  if curl -fsS "${BASE_URL}/readyz" >/dev/null 2>&1; then ready=1; break; fi
  sleep 0.2
done
if [ "$ready" -ne 1 ]; then
  echo "server did not become ready at ${BASE_URL}/readyz" >&2
  exit 1
fi

# run_k6 runs one scenario. Failure gates (both HARD, per the Phase 3.5 design —
# "a gross regression fails the k6 run with a non-zero exit code"):
#   - Correctness (k6 checks): a FAIL status file — written when any check fails,
#     e.g. HTTP 200 with an empty result set — aborts the run.
#   - Latency thresholds (k6 exit 99): a breach aborts the run, UNLESS
#     BENCH_ALLOW_THRESHOLD_BREACH=1, the documented escape hatch for a known-slow
#     box, which downgrades it to a recorded warning.
# Any other non-zero exit (script error, connection failure) aborts the run.
run_k6() {
  local script=$1 vus=$2 dur=$3
  local name status_file
  name="$(basename "$script" .js)"
  status_file="$RESULTS_DIR/$name.status"
  rm -f "$status_file"
  set +e
  VUS="$vus" DURATION="$dur" k6 run "$script"
  local rc=$?
  set -e
  if [ -f "$status_file" ] && grep -q FAIL "$status_file"; then
    echo "k6 correctness checks FAILED for $name (see $status_file)" >&2
    exit 1
  fi
  if [ "$rc" -eq 99 ]; then
    if [ "${BENCH_ALLOW_THRESHOLD_BREACH:-0}" = "1" ]; then
      echo "  ⚠ k6 latency thresholds breached for $name (tolerated via BENCH_ALLOW_THRESHOLD_BREACH=1; numbers recorded)"
    else
      echo "k6 latency thresholds breached for $name — regression gate failed. Set BENCH_ALLOW_THRESHOLD_BREACH=1 to tolerate on a known-slow box." >&2
      exit "$rc"
    fi
  elif [ "$rc" -ne 0 ]; then
    echo "k6 failed for $name (exit $rc)" >&2
    exit "$rc"
  fi
}

# 8. Seed a fixed, deterministic dataset (fixed iterations, one sample per series)
# so the query scenarios always run against the same cardinality and history.
echo ">> k6 seed (deterministic fixed dataset)"
run_k6 "$K6_DIR/seed.js" "$SEED_VUS" "$SEED_DURATION"

# 9. Query scenarios (run against the seeded dataset).
echo ">> k6 instant_query"
run_k6 "$K6_DIR/instant_query.js" "$RUN_VUS" "$RUN_DURATION"

echo ">> k6 range_query"
run_k6 "$K6_DIR/range_query.js" "$RUN_VUS" "$RUN_DURATION"

# 10. Ingest throughput (random live-load model). Runs LAST so its random series
# and timestamps do not perturb the deterministic dataset the queries measured.
echo ">> k6 ingest (throughput)"
run_k6 "$K6_DIR/ingest.js" "$SEED_VUS" "$SEED_DURATION"

echo ">> done. summaries:"
ls -1 "$RESULTS_DIR"
