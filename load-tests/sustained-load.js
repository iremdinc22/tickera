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
 * Sustained arrival-rate workload.
 *
 * A constant number of booking attempts is started per second.
 * Each iteration uses the next seat in the configured range.
 *
 * Test data: SEAT_COUNT available, consecutive seats starting at
 * FIRST_SEAT_ID are required. Keep RATE × DURATION within
 * SEAT_COUNT if all attempts are expected to succeed.
 */

const bookingCreated = new Counter('booking_created');
const bookingConflict = new Counter('booking_conflict');
const unexpectedResponses = new Counter('unexpected_responses');

const validBookingResponse = new Rate('valid_booking_response');

const FIRST_SEAT_ID = positiveInt('FIRST_SEAT_ID');
const SEAT_COUNT = positiveInt('SEAT_COUNT', 1000);
const RATE = positiveInt('RATE', 10);
const DURATION = duration('DURATION', '30s');
const PRE_ALLOCATED_VUS = positiveInt('PRE_ALLOCATED_VUS', 20);
const MAX_VUS = positiveInt('MAX_VUS', 100);

export const options = {
  scenarios: {
    sustained_load: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: PRE_ALLOCATED_VUS,
      maxVUs: MAX_VUS,
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
   * Seat aralığı içinde sırayla ilerle.
   */
  const seatId =
    FIRST_SEAT_ID + (iteration % SEAT_COUNT);

  const response = http.post(
    `${BASE_URL}/bookings`,
    bookingPayload(
      seatId,
      `sustained-user-${iteration}`
    ),
    bookingParams(
      'sustained',
      {
        endpoint: 'sustained-booking',
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
