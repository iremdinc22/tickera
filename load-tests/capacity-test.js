import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';
import exec from 'k6/execution';
import {
  BASE_URL,
  bookingParams,
  bookingPayload,
  duration,
  positiveInt,
} from './lib/config.js';

/*
 * Capacity / saturation workload.
 *
 * Every iteration books a fresh seat, so a 409 means the test
 * data range was exhausted or the seat was not available at the
 * start of the run.
 *
 * Resource-intensive by design. Do not run high request rates
 * without explicit approval.
 */

const unexpectedResponses = new Counter('unexpected_responses');

const FIRST_SEAT_ID = positiveInt('FIRST_SEAT_ID');
const RATE = positiveInt('RATE', 10);
const DURATION = duration('DURATION', '30s');
const PRE_ALLOCATED_VUS = positiveInt('PRE_ALLOCATED_VUS', 200);
const MAX_VUS = positiveInt('MAX_VUS', 500);

export const options = {
  scenarios: {
    capacity_test: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: PRE_ALLOCATED_VUS,
      maxVUs: MAX_VUS,
    },
  },

  thresholds: {
    http_req_duration: ['p(95)<1000'],
    http_req_failed: ['rate==0'],
    unexpected_responses: ['count==0'],
  },
};

export default function () {
  const iteration =
    exec.scenario.iterationInTest;

  const seatId =
    FIRST_SEAT_ID + iteration;

  const response = http.post(
    `${BASE_URL}/bookings`,
    bookingPayload(
      seatId,
      `capacity-user-${RATE}-${iteration}`
    ),
    bookingParams(
      'capacity',
      {
        endpoint: 'capacity-test',
        rate: String(RATE),
      }
    )
  );

  if (response.status !== 201) {
    unexpectedResponses.add(1);
  }

  check(response, {
    'booking created': (r) => r.status === 201,
  });
}
