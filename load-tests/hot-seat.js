import http from 'k6/http';
import { check } from 'k6';
import { Counter, Rate } from 'k6/metrics';
import {
  BASE_URL,
  bookingParams,
  bookingPayload,
  duration,
  positiveInt,
} from './lib/config.js';

/*
 * Hot-seat contention.
 *
 * Every virtual user attempts to book the same seat with an
 * independent Idempotency-Key. Exactly one attempt may succeed;
 * the remaining attempts must receive HTTP 409 Conflict.
 */

const bookingCreated = new Counter('booking_created');
const bookingConflict = new Counter('booking_conflict');
const unexpectedResponses = new Counter('unexpected_responses');

const validBookingResponse = new Rate('valid_booking_response');

const SEAT_ID = positiveInt('SEAT_ID');
const VUS = positiveInt('VUS', 100);
const MAX_DURATION = duration('MAX_DURATION', '30s');

export const options = {
  scenarios: {
    hot_seat: {
      executor: 'per-vu-iterations',
      vus: VUS,
      iterations: 1,
      maxDuration: MAX_DURATION,
    },
  },

  thresholds: {
    valid_booking_response: ['rate==1'],
    unexpected_responses: ['count==0'],
  },
};

export default function () {
  const response = http.post(
    `${BASE_URL}/bookings`,
    bookingPayload(
      SEAT_ID,
      `hot-seat-user-${__VU}`
    ),
    bookingParams(
      'hot-seat',
      {
        endpoint: 'hot-seat',
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
