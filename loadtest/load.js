// Load test: redirects (80% of traffic on 20% of links, like the 80/20
// assumption in README) and link creation running at the same time.
// Run with scripts/loadtest.sh, which starts the service.
import http from 'k6/http';
import { check } from 'k6';

const BASE = __ENV.BASE_URL;
const LINKS = parseInt(__ENV.LINKS || '1000');
const REDIRECT_RATE = parseInt(__ENV.REDIRECT_RATE || '2000');
const CREATE_RATE = parseInt(__ENV.CREATE_RATE || '100');
const DURATION = __ENV.DURATION || '30s';

export const options = {
  // Do not follow redirects: we measure the shortener, not the target site.
  maxRedirects: 0,
  scenarios: {
    redirects: {
      executor: 'constant-arrival-rate', exec: 'redirect',
      rate: REDIRECT_RATE, timeUnit: '1s', duration: DURATION,
      preAllocatedVUs: 200, maxVUs: 1000,
    },
    creates: {
      executor: 'constant-arrival-rate', exec: 'create',
      rate: CREATE_RATE, timeUnit: '1s', duration: DURATION,
      preAllocatedVUs: 50, maxVUs: 300,
    },
  },
  // The non-functional requirements: redirect p99 < 100 ms, create p99 < 500 ms.
  thresholds: {
    'http_req_duration{scenario:redirects}': ['p(99)<100'],
    'http_req_duration{scenario:creates}': ['p(99)<500'],
    'checks{scenario:redirects}': ['rate>0.99'],
    'checks{scenario:creates}': ['rate>0.99'],
  },
  summaryTrendStats: ['avg', 'p(50)', 'p(95)', 'p(99)', 'max'],
};

export function setup() {
  const user = http.post(`${BASE}/api/v1/users`, JSON.stringify({ email: `load-${Date.now()}@example.com` }));
  const key = user.json('api_key');
  const headers = { Authorization: `Bearer ${key}` };
  const keys = [];
  for (let i = 0; i < LINKS; i++) {
    const res = http.post(`${BASE}/api/v1/links`, JSON.stringify({ long_url: `https://example.com/${i}` }), { headers });
    if (res.status !== 201) throw new Error(`setup create: ${res.status} ${res.body}`);
    keys.push(res.json('key'));
  }
  return { keys, headers };
}

export function redirect(data) {
  const hot = Math.floor(data.keys.length * 0.2);
  // 80% of requests go to the first 20% of the links.
  const i = Math.random() < 0.8
    ? Math.floor(Math.random() * hot)
    : hot + Math.floor(Math.random() * (data.keys.length - hot));
  const res = http.get(`${BASE}/${data.keys[i]}`, { tags: { name: 'GET /{key}' } });
  check(res, { 'redirect is 301': (r) => r.status === 301 });
}

export function create(data) {
  const res = http.post(`${BASE}/api/v1/links`, JSON.stringify({ long_url: 'https://example.com/new' }),
    { headers: data.headers, tags: { name: 'POST /api/v1/links' } });
  check(res, { 'create is 201': (r) => r.status === 201 });
}
