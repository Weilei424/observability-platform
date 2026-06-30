import http from 'k6/http';
import { check } from 'k6';
import { BASE_URL, TREND_STATS, rangeSelector, summaryHandler } from './lib.js';

export const options = {
  scenarios: {
    range: {
      executor: 'constant-vus',
      vus: parseInt(__ENV.VUS || '10', 10),
      duration: __ENV.DURATION || '30s',
    },
  },
  summaryTrendStats: TREND_STATS,
  thresholds: {
    http_req_duration: ['p(95)<1000'],
    http_req_failed: ['rate<0.01'],
  },
};

export default function () {
  const end = Math.floor(Date.now() / 1000);
  const start = end - 3600; // 1h window
  const url =
    BASE_URL +
    '/api/v1/query_range?query=' +
    encodeURIComponent(rangeSelector()) +
    '&start=' + start + '&end=' + end + '&step=15';
  const res = http.get(url);
  check(res, {
    'status 200': (r) => r.status === 200,
    'matrix': (r) => r.json('data.resultType') === 'matrix',
    'non-empty': (r) => Array.isArray(r.json('data.result')) && r.json('data.result').length > 0,
  });
}

export const handleSummary = summaryHandler('range_query');
