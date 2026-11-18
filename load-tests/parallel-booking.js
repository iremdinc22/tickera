import http from 'k6/http';
import { check } from 'k6';
import { Counter, Rate } from 'k6/metrics';
import exec from 'k6/execution';
import {
  BASE_URL,
  bookingParams,
  bookingPayload,
  duration,
  positiveInt,
} from './lib/config.js';

/*
 * Parallel-seat booking.
 *
 * Every iteration books a different seat. There is no logical
 * contention on a single row, so this workload isolates general
 * request overhead from hot-row contention.
 *
 * Test data: SEAT_COUNT available, consecutive seats starting at
 * FIRST_SEAT_ID are required.
 */

const bookingCreated = new Counter('booking_created');
const bookingConflict = new Counter('booking_conflict');
const unexpectedResponses = new Counter('unexpected_responses');

const validBookingResponse = new Rate('valid_booking_response');

const FIRST_SEAT_ID = positiveInt('FIRST_SEAT_ID');
const VUS = positiveInt('VUS', 100);
const MAX_DURATION = duration('MAX_DURATION', '30s');

export const options = {
  scenarios: {
    parallel_load: {
      executor: 'per-vu-iterations',
      vus: VUS,
      iterations: 1,
      maxDuration: MAX_DURATION,
    },
  },

  thresholds: {
    valid_booking_response: ['rate==1'],
    http_req_duration: ['p(95)<1000'],
    unexpected_responses: ['count==0'],
  },
};

export default function () {
  const iteration =
    exec.scenario.iterationInTest;

  /*
   * Her iteration farklı seat kullanır.
   */
  const seatId =
    FIRST_SEAT_ID + iteration;

  const response = http.post(
    `${BASE_URL}/bookings`,
    bookingPayload(
      seatId,
      `parallel-user-${iteration}`
    ),
    bookingParams(
      'parallel',
      {
        endpoint: 'parallel-booking',
      }
    )
  );

  if (response.status === 201) {
    bookingCreated.add(1);
    validBookingResponse.add(true);
  } else if (response.status === 409) {
    bookingConflict.add(1);
    validBookingResponse.add(true);
  } else {
    unexpectedResponses.add(1);
    validBookingResponse.add(false);
  }

  check(response, {
    'booking response is valid': (r) =>
      r.status === 201 || r.status === 409,
  });
}
