// Shared config and helpers for the Phase 3.5 k6 load tests.
// All scripts import from here so the query selectors match what ingest seeds.

export const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
export const CARDINALITY = parseInt(__ENV.CARDINALITY || '1000', 10);
export const BATCH = parseInt(__ENV.BATCH || '100', 10);
export const METRIC_NAME = 'bench_http_requests_total';

// Percentiles surfaced in every script's summary.
export const TREND_STATS = ['avg', 'min', 'med', 'max', 'p(50)', 'p(95)', 'p(99)'];

// buildIngestBody returns a JSON ingest payload of BATCH samples spread across
// CARDINALITY series. instance has 50 distinct values; series is unique.
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
    lines.push('');
    const out = {};
    out['stdout'] = lines.join('\n') + '\n';
    out['bench/results/' + name + '.json'] = JSON.stringify(data, null, 2);
    return out;
  };
}
