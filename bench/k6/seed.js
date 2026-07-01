import http from 'k6/http';
import { check } from 'k6';
import exec from 'k6/execution';
import { BASE_URL, SEED_ITERATIONS, buildSeedBody, summaryHandler } from './lib.js';

// Deterministic seed stage: shared-iterations writes every series exactly once at
// a single timestamp, producing a fixed CARDINALITY×1 dataset regardless of
// machine speed. This runs BEFORE the query scenarios so they always query the
// same cardinality and history — separate from the (random) ingest throughput
// scenario. VUS is honored for seed parallelism; total iterations are fixed.
export const options = {
  scenarios: {
    seed: {
      executor: 'shared-iterations',
      vus: parseInt(__ENV.VUS || '10', 10),
      iterations: SEED_ITERATIONS,
      maxDuration: '5m',
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
  },
};

// setup() runs exactly once (not per-VU) and its return value is passed to every
// default() invocation, so the whole dataset shares one timestamp. Module-level
// init would run separately in each VU runtime and yield divergent timestamps.
export function setup() {
  return { seedTs: Date.now() };
}

export default function (data) {
  // iterationInTest is the scenario-global 0-based iteration index across all VUs,
  // so each series partition is written by exactly one iteration (unlike __ITER,
  // which is per-VU).
  const body = buildSeedBody(exec.scenario.iterationInTest, data.seedTs);
  const res = http.post(BASE_URL + '/api/v1/ingest/metrics', body, {
    headers: { 'Content-Type': 'application/json' },
  });
  check(res, { 'status is 204': (r) => r.status === 204 });
}

export const handleSummary = summaryHandler('seed');
