import exec from 'k6/execution';

/*
 * Shared configuration helpers for Tickera k6 scripts.
 *
 * All scripts read the target URL and the test-data parameters
 * from environment variables so that the same script can target
 * a local application, a remote host or a CI environment.
 */

export const BASE_URL = (
  __ENV.BASE_URL || 'http://localhost:8080'
).replace(/\/+$/, '');

/*
 * A per-run identifier keeps idempotency keys unique across
 * separate executions. Without it, re-running a script with the
 * same key would turn real booking attempts into replays of an
 * older booking.
 */
const RUN_ID =
  __ENV.RUN_ID || Date.now().toString(36);

export function idempotencyKey(prefix) {
  return [
    prefix,
    RUN_ID,
    exec.vu.idInTest,
    exec.scenario.iterationInTest,
  ].join('-');
}

export function positiveInt(name, defaultValue) {
  const raw = __ENV[name];

  if (raw === undefined || raw === '') {
    if (defaultValue === undefined) {
      throw new Error(
        `${name} is required. ` +
        'Run load-tests/prepare-test-data.sh and export its output.'
      );
    }

    return defaultValue;
  }

  const value = Number(raw);

  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(
      `${name} must be a positive integer, got: ${raw}`
    );
  }

  return value;
}

export function duration(name, defaultValue) {
  const raw = __ENV[name];

  if (raw === undefined || raw === '') {
    return defaultValue;
  }

  if (!/^\d+[smh]$/.test(raw)) {
    throw new Error(
      `${name} must be a duration such as 30s, 5m or 1h, got: ${raw}`
    );
  }

  return raw;
}

/*
 * POST /bookings accepts exactly seatId and userId.
 */
export function bookingPayload(seatId, userId) {
  return JSON.stringify({
    seatId,
    userId,
  });
}

/*
 * POST /bookings always requires an Idempotency-Key header.
 *
 * Each independent booking attempt gets a unique key. Scripts
 * that intentionally replay a request reuse one key explicitly.
 */
export function bookingParams(keyPrefix, tags) {
  return {
    headers: {
      'Content-Type': 'application/json',
      'Idempotency-Key': idempotencyKey(keyPrefix),
    },
    tags,
  };
}
