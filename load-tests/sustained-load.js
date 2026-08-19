import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate } from 'k6/metrics';
import exec from 'k6/execution';

const bookingCreated = new Counter('booking_created');
const bookingConflict = new Counter('booking_conflict');
const unexpectedResponses = new Counter('unexpected_responses');

const validBookingResponse = new Rate('valid_booking_response');

export const options = {
  scenarios: {
    sustained_load: {
      executor: 'constant-arrival-rate',

      // Saniyede 10 yeni booking attempt
      rate: 10,
      timeUnit: '1s',

      // 30 saniye boyunca devam et
      duration: '30s',

      preAllocatedVUs: 20,
      maxVUs: 100,
    },
  },

  thresholds: {
    valid_booking_response: ['rate==1'],
    http_req_duration: ['p(95)<1000'],
    unexpected_responses: ['count==0'],
  },
};

export default function () {
  // Sustained Load Event
  // Seat IDs: 1013 - 2012
  const firstSeatId = 2013;
  const totalSeats = 1000;
  
  const iteration = exec.scenario.iterationInTest;

  // 1000 seat içinde sırayla ilerle
  const seatOffset = iteration % totalSeats;
  const seatId = firstSeatId + seatOffset;

  const payload = JSON.stringify({
    seatId,
    userId: `sustained-user-${iteration}`,
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
    tags: {
      endpoint: 'sustained-booking',
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

  sleep(0.1);
}