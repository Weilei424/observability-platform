import http from 'k6/http';
import { check } from 'k6';
import { BASE_URL, TREND_STATS, instantSelector, summaryHandler } from './lib.js';

export const options = {
  scenarios: {
    instant: {
      executor: 'constant-vus',
      vus: parseInt(__ENV.VUS || '10', 10),
      duration: __ENV.DURATION || '30s',
    },
  },
  summaryTrendStats: TREND_STATS,
  thresholds: {
    http_req_duration: ['p(95)<500'],
    http_req_failed: ['rate<0.01'],
  },
};

export default function () {
  const url = BASE_URL + '/api/v1/query?query=' + encodeURIComponent(instantSelector());
  const res = http.get(url);
  check(res, {
    'status 200': (r) => r.status === 200,
    'success': (r) => r.json('status') === 'success',
    'non-empty': (r) => Array.isArray(r.json('data.result')) && r.json('data.result').length > 0,
  });
}

export const handleSummary = summaryHandler('instant_query');
