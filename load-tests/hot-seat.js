import http from 'k6/http';
import { check } from 'k6';
import { Counter, Rate } from 'k6/metrics';

const bookingCreated = new Counter('booking_created');
const bookingConflict = new Counter('booking_conflict');
const unexpectedResponses = new Counter('unexpected_responses');

const validBookingResponse = new Rate('valid_booking_response');

const SEAT_ID = Number(__ENV.SEAT_ID);

export const options = {
  scenarios: {
    hot_seat: {
      executor: 'per-vu-iterations',
      vus: 100,
      iterations: 1,
      maxDuration: '30s',
    },
  },

  thresholds: {
    valid_booking_response: ['rate==1'],
    unexpected_responses: ['count==0'],
  },
};

export default function () {
  const payload = JSON.stringify({
    seatId: SEAT_ID,
    userId: `hot-seat-user-${__VU}`,
  });

  const response = http.post(
    'http://localhost:8080/bookings',
    payload,
    {
      headers: {
        'Content-Type': 'application/json',
      },
      tags: {
        endpoint: 'hot-seat',
      },
    }
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