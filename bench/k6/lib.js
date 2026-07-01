// Shared config and helpers for the Phase 3.5 k6 load tests.
// All scripts import from here so the query selectors match what ingest seeds.

export const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
export const CARDINALITY = parseInt(__ENV.CARDINALITY || '1000', 10);
export const BATCH = parseInt(__ENV.BATCH || '100', 10);
export const METRIC_NAME = 'bench_http_requests_total';

// SEED_ITERATIONS covers every series exactly once (BATCH series per iteration).
// The seed scenario runs as shared-iterations so the persisted dataset is fixed
// and reproducible — CARDINALITY series, one sample each — independent of machine
// speed or wall-clock duration.
export const SEED_ITERATIONS = Math.ceil(CARDINALITY / BATCH);

// Percentiles surfaced in every script's summary.
export const TREND_STATS = ['avg', 'min', 'med', 'max', 'p(50)', 'p(95)', 'p(99)'];

// buildIngestBody returns a JSON ingest payload of BATCH samples spread across
// CARDINALITY series. instance has 50 distinct values; series is unique. Used by
// the ingest *throughput* scenario, where random series/values model live load.
export function buildIngestBody(nowMs) {
  const metrics = [];
  for (let i = 0; i < BATCH; i++) {
    const s = Math.floor(Math.random() * CARDINALITY);
    metrics.push({
      name: METRIC_NAME,
      labels: { job: 'bench', instance: 'inst-' + (s % 50), series: 's-' + s },
      timestamp_ms: nowMs,
      value: Math.random() * 1000,
    });
  }
  return JSON.stringify({ metrics: metrics });
}

// buildSeedBody deterministically writes the BATCH series owned by iteration
// iterIndex: series indices [iterIndex*BATCH, iterIndex*BATCH+BATCH). Each series
// is written exactly once (indices past CARDINALITY-1 are skipped), so across all
// SEED_ITERATIONS the persisted dataset is exactly CARDINALITY series × 1 sample.
// Value is a fixed function of the series index (no randomness).
export function buildSeedBody(iterIndex, nowMs) {
  const metrics = [];
  for (let i = 0; i < BATCH; i++) {
    const s = iterIndex * BATCH + i;
    if (s >= CARDINALITY) break;
    metrics.push({
      name: METRIC_NAME,
      labels: { job: 'bench', instance: 'inst-' + (s % 50), series: 's-' + s },
      timestamp_ms: nowMs,
      value: s,
    });
  }
  return JSON.stringify({ metrics: metrics });
}

// instantSelector matches all seeded series (a high-cardinality instant query).
export function instantSelector() {
  return METRIC_NAME + '{job="bench"}';
}

// rangeSelector matches one instance bucket (~CARDINALITY/50 series) to keep the
// per-tick range evaluation bounded.
export function rangeSelector() {
  return METRIC_NAME + '{instance="inst-7"}';
}

function fmt(v) {
  return v === undefined ? 'n/a' : v.toFixed(2);
}

// summaryHandler returns a k6 handleSummary function that writes the full JSON
// summary to bench/results/<name>.json and a compact, fully-offline text summary
// to stdout (no remote jslib import, so runs work without network at run time).
export function summaryHandler(name) {
  return function (data) {
    const dur = (data.metrics.http_req_duration || {}).values || {};
    const reqs = (data.metrics.http_reqs || {}).values || {};
    const lines = [
      '',
      '=== ' + name + ' ===',
      'http_reqs    : ' + (reqs.count || 0) + ' (' + (reqs.rate || 0).toFixed(1) + '/s)',
      'p50 (ms)     : ' + fmt(dur['p(50)']),
      'p95 (ms)     : ' + fmt(dur['p(95)']),
      'p99 (ms)     : ' + fmt(dur['p(99)']),
      'max (ms)     : ' + fmt(dur.max),
    ];
    const samples = data.metrics.samples_sent;
    if (samples) {
      const s = samples.values;
      lines.push('samples_sent : ' + s.count + ' (' + (s.rate || 0).toFixed(0) + '/s)');
    }

    // Correctness gate: any failed check (e.g. HTTP 200 with an empty result set)
    // marks the run FAIL. This is written to a status file the runner treats as a
    // HARD failure, tracked separately from the latency thresholds (k6 exit 99) so
    // a correctness regression is never masked by the latency escape hatch.
    const checks = (data.metrics.checks || {}).values || {};
    const checksOk = checks.rate === undefined || checks.rate >= 1;
    lines.push('checks       : ' + (checksOk ? 'OK' : 'FAIL (rate=' + (checks.rate || 0).toFixed(3) + ')'));
    lines.push('');

    const out = {};
    out['stdout'] = lines.join('\n') + '\n';
    out['bench/results/' + name + '.json'] = JSON.stringify(data, null, 2);
    out['bench/results/' + name + '.status'] = checksOk ? 'OK\n' : 'FAIL\n';
    return out;
  };
}
