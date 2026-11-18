import http from 'k6/http';
import { check } from 'k6';
import { Counter, Rate } from 'k6/metrics';
import {
  BASE_URL,
  bookingParams,
  bookingPayload,
  positiveInt,
} from './lib/config.js';

/*
 * Fixed-iteration concurrent booking.
 *
 * All attempts target the same seat. One booking may succeed;
 * every other attempt must conflict.
 *
 * Use this script when a fixed number of attempts is required
 * (for example, reproducing a specific concurrency level).
 */

const bookingCreated = new Counter('booking_created');
const bookingConflict = new Counter('booking_conflict');
const unexpectedResponses = new Counter('unexpected_responses');

const validBookingResponse = new Rate('valid_booking_response');

const SEAT_ID = positiveInt('SEAT_ID');
const VUS = positiveInt('VUS', 100);
const ITERATIONS = positiveInt('ITERATIONS', 100);

export const options = {
  vus: VUS,
  iterations: ITERATIONS,

  thresholds: {
    valid_booking_response: ['rate==1'],
    http_req_duration: ['p(95)<1000'],
    unexpected_responses: ['count==0'],
  },
};

export default function () {
  const response = http.post(
    `${BASE_URL}/bookings`,
    bookingPayload(
      SEAT_ID,
      `concurrent-user-${__VU}-${__ITER}`
    ),
    bookingParams(
      'concurrent',
      {
        endpoint: 'create-booking',
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
