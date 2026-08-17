import http from 'k6/http';
import { check } from 'k6';
import { Counter, Rate } from 'k6/metrics';

const bookingCreated = new Counter('booking_created');
const bookingConflict = new Counter('booking_conflict');
const unexpectedResponses = new Counter('unexpected_responses');

const validBookingResponse = new Rate('valid_booking_response');

export const options = {
  scenarios: {
    parallel_load: {
      executor: 'constant-vus',
      vus: 100,
      duration: '10s',
    },
  },

  thresholds: {
    valid_booking_response: ['rate==1'],
    http_req_duration: ['p(95)<1000'],
    unexpected_responses: ['count==0'],
  },
};

export default function () {
  const firstSeatId = 413;

  const seatId = firstSeatId + (__VU - 1);

  const payload = JSON.stringify({
    seatId,
    userId: `parallel-user-${__VU}-${__ITER}`,
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
    tags: {
      endpoint: 'parallel-booking',
    },
  };

  const response = http.post(
    'http://localhost:8080/bookings',
    payload,
    params
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