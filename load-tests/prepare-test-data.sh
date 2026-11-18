#!/usr/bin/env bash

#
# Creates a fresh event with consecutive seats for the k6 scripts.
#
# Usage:
#   ./load-tests/prepare-test-data.sh [SEAT_COUNT]
#
# Environment:
#   BASE_URL    Application base URL (default: http://localhost:8080)
#   SEAT_COUNT  Number of seats to create (default: 1000)
#   EVENT_NAME  Event name (default: k6 Load Test <timestamp>)
#   EVENT_DATE  Future event date (default: 2030-01-01T20:00:00)
#
# Requirements:
#   curl, jq
#
# The script prints export statements. Source them before running a
# k6 script:
#
#   eval "$(./load-tests/prepare-test-data.sh 100)"
#   k6 run load-tests/parallel-booking.js
#

set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8080}"
BASE_URL="${BASE_URL%/}"

SEAT_COUNT="${1:-${SEAT_COUNT:-1000}}"
EVENT_NAME="${EVENT_NAME:-k6 Load Test $(date +%s)}"
EVENT_DATE="${EVENT_DATE:-2030-01-01T20:00:00}"

if ! command -v curl >/dev/null 2>&1; then
  echo "curl is required" >&2
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required" >&2
  exit 1
fi

if ! [[ "$SEAT_COUNT" =~ ^[1-9][0-9]*$ ]]; then
  echo "SEAT_COUNT must be a positive integer, got: $SEAT_COUNT" >&2
  exit 1
fi

seats_json="$(
  seq 1 "$SEAT_COUNT" |
    jq -R . |
    jq -s .
)"

payload="$(
  jq -n \
    --arg name "$EVENT_NAME" \
    --arg venue "Load Test Venue" \
    --arg eventDate "$EVENT_DATE" \
    --argjson seatNumbers "$seats_json" \
    '{
      name: $name,
      venue: $venue,
      eventDate: $eventDate,
      seatNumbers: $seatNumbers
    }'
)"

response="$(
  curl -sS \
    -X POST \
    "$BASE_URL/events" \
    -H 'Content-Type: application/json' \
    -d "$payload"
)"

event_id="$(jq -r '.id // empty' <<<"$response")"

if [[ -z "$event_id" ]]; then
  echo "Failed to create event:" >&2
  echo "$response" >&2
  exit 1
fi

first_seat_id="$(
  jq -r '.seats[0].id // empty' <<<"$response"
)"

last_seat_id="$(
  jq -r \
    --argjson count "$SEAT_COUNT" \
    '.seats[$count - 1].id // empty' \
    <<<"$response"
)"

if [[ -z "$first_seat_id" ]]; then
  echo "Event created but no seats were returned:" >&2
  echo "$response" >&2
  exit 1
fi

echo "# Tickera k6 test data"
echo "# Event: $EVENT_NAME"
echo "export BASE_URL=$BASE_URL"
echo "export EVENT_ID=$event_id"
echo "export FIRST_SEAT_ID=$first_seat_id"
echo "export SEAT_COUNT=$SEAT_COUNT"
echo "export SEAT_ID=$first_seat_id"

expected_last=$((first_seat_id + SEAT_COUNT - 1))

if [[ "$last_seat_id" != "$expected_last" ]]; then
  echo "# WARNING: seat IDs are not consecutive." >&2
  echo "# Expected last seat $expected_last, got $last_seat_id." >&2
  echo "# Scripts that address seats as FIRST_SEAT_ID + offset" >&2
  echo "# may not work with this data. Create a new event." >&2
fi
