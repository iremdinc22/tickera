import http from 'k6/http';
import { check } from 'k6';
import exec from 'k6/execution';

const RATE = Number(__ENV.RATE || 10);
const FIRST_SEAT_ID = Number(__ENV.FIRST_SEAT_ID);

export const options = {
  scenarios: {
    capacity_test: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: '30s',
      preAllocatedVUs: 200,
      maxVUs: 500,
    },
  },

  thresholds: {
    http_req_duration: ['p(95)<1000'],
    http_req_failed: ['rate==0'],
  },
};

export default function () {
  const iteration = exec.scenario.iterationInTest;
  const seatId = FIRST_SEAT_ID + iteration;

  const payload = JSON.stringify({
    seatId,
    userId: `capacity-user-${RATE}-${iteration}`,
  });

  const response = http.post(
    'http://localhost:8080/bookings',
    payload,
    {
      headers: {
        'Content-Type': 'application/json',
      },
      tags: {
        endpoint: 'capacity-test',
        rate: String(RATE),
      },
    }
  );

  check(response, {
    'booking created': (r) => r.status === 201,
  });
}
