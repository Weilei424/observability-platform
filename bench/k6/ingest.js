import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';
import { BASE_URL, BATCH, TREND_STATS, buildIngestBody, summaryHandler } from './lib.js';

export const options = {
  scenarios: {
    ingest: {
      executor: 'constant-vus',
      vus: parseInt(__ENV.VUS || '10', 10),
      duration: __ENV.DURATION || '30s',
    },
  },
  summaryTrendStats: TREND_STATS,
  thresholds: {
    // Generous regression guard, not an SLA: the ingest path fsyncs every record
    // by default, so absolute latency is high (more so under WSL2). bench/run.sh
    // also tolerates a thresholds-breached exit so a slow box still records numbers.
    http_req_duration: ['p(95)<2000'],
    http_req_failed: ['rate<0.01'],
  },
};

const samplesSent = new Counter('samples_sent');

export default function () {
  const res = http.post(BASE_URL + '/api/v1/ingest/metrics', buildIngestBody(Date.now()), {
    headers: { 'Content-Type': 'application/json' },
  });
  check(res, { 'status is 204': (r) => r.status === 204 });
  if (res.status === 204) {
    samplesSent.add(BATCH);
  }
}

export const handleSummary = summaryHandler('ingest');
