import http from 'k6/http';
import { check } from 'k6';
import { Counter, Rate } from 'k6/metrics';
import exec from 'k6/execution';
import {
  BASE_URL,
  bookingPayload,
  idempotencyKey,
  positiveInt,
} from './lib/config.js';

/*
 * Idempotency replay contract.
 *
 * For each iteration:
 *
 * 1. Create a booking with a fresh Idempotency-Key       → 201
 * 2. Repeat the same key and payload                     → 201,
 *    returning the same booking ID
 * 3. Reuse the same key with a different seat            → 409
 *
 * This is a functional smoke test, not a throughput test.
 * Test data: two consecutive seats per iteration starting at
 * FIRST_SEAT_ID.
 */

const created = new Counter('replay_original_created');
const replayed = new Counter('replay_existing_returned');
const keyConflict = new Counter('replay_key_conflict');
const unexpectedResponses = new Counter('unexpected_responses');

const validReplay = new Rate('valid_idempotency_replay');

const FIRST_SEAT_ID = positiveInt('FIRST_SEAT_ID');
const VUS = positiveInt('VUS', 10);

export const options = {
  scenarios: {
    idempotency_replay: {
      executor: 'per-vu-iterations',
      vus: VUS,
      iterations: 1,
      maxDuration: '30s',
    },
  },

  thresholds: {
    valid_idempotency_replay: ['rate==1'],
    unexpected_responses: ['count==0'],
  },
};

export default function () {
  const iteration =
    exec.scenario.iterationInTest;

  const seatId =
    FIRST_SEAT_ID + (iteration * 2);

  const otherSeatId =
    seatId + 1;

  const key =
    idempotencyKey('replay');

  const params = {
    headers: {
      'Content-Type': 'application/json',
      'Idempotency-Key': key,
    },
    tags: {
      endpoint: 'idempotency-replay',
    },
  };

  const first = http.post(
    `${BASE_URL}/bookings`,
    bookingPayload(
      seatId,
      `replay-user-${iteration}`
    ),
    params
  );

  const second = http.post(
    `${BASE_URL}/bookings`,
    bookingPayload(
      seatId,
      `replay-user-${iteration}`
    ),
    params
  );

  const third = http.post(
    `${BASE_URL}/bookings`,
    bookingPayload(
      otherSeatId,
      `replay-other-user-${iteration}`
    ),
    params
  );

  let sameBooking = false;

  try {
    sameBooking =
      JSON.parse(first.body).id ===
      JSON.parse(second.body).id;
  } catch (error) {
    sameBooking = false;
  }

  if (first.status === 201) {
    created.add(1);
  }

  if (second.status === 201) {
    replayed.add(1);
  }

  if (third.status === 409) {
    keyConflict.add(1);
  }

  const valid =
    first.status === 201 &&
    second.status === 201 &&
    sameBooking &&
    third.status === 409;

  validReplay.add(valid);

  if (!valid) {
    unexpectedResponses.add(1);
  }

  check(first, {
    'original request created a booking': (r) =>
      r.status === 201,
  });

  check(second, {
    'replay returned the existing booking': (r) =>
      r.status === 201,
  });

  check(third, {
    'same key with different request conflicts': (r) =>
      r.status === 409,
  });
}
