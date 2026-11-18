# Tickera Load Tests (k6)

These scripts exercise the Tickera booking API with real HTTP traffic.

They are engineering tools for comparing behavior under concurrency, not
production capacity benchmarks.

Historical results are documented in
[Performance Testing](../docs/performance-testing.md). Those measurements
were collected on an earlier version of the API and are not directly
comparable with the current implementation.

---

## Prerequisites

- k6 (`https://k6.io`)
- Docker Compose infrastructure running
- Tickera application running on `http://localhost:8080` (or a custom
  `BASE_URL`)
- `curl` and `jq` for test-data preparation

```bash
docker compose up -d
./mvnw spring-boot:run
```

The scripts assume the database already contains an event with enough
available seats. Use the preparation script below; do not reuse seats
from a previous run.

---

## API contract used by the scripts

```http
POST /bookings
Content-Type: application/json
Idempotency-Key: <unique key>

{"seatId": 123, "userId": "load-user-1"}
```

- `201 Created` — booking created, or an idempotent replay of the same
  request
- `409 Conflict` — seat already booked, or the same key was reused with
  a different request

Every script sends an `Idempotency-Key`. Keys are unique per run, VU and
iteration. The only intentional key reuse happens in
`idempotency-replay.js`.

---

## Scripts

| Script | Workload | Required | Optional |
|---|---|---|---|
| `hot-seat.js` | All VUs book the same seat | `SEAT_ID` | `VUS` (100), `MAX_DURATION` (`30s`) |
| `parallel-booking.js` | Each VU books a different seat | `FIRST_SEAT_ID` | `VUS` (100), `MAX_DURATION` (`30s`) |
| `concurrent-booking.js` | Fixed number of attempts on one seat | `SEAT_ID` | `VUS` (100), `ITERATIONS` (100) |
| `sustained-load.js` | Constant arrival rate across a seat range | `FIRST_SEAT_ID` | `SEAT_COUNT` (1000), `RATE` (10), `DURATION` (`30s`), `PRE_ALLOCATED_VUS` (20), `MAX_VUS` (100) |
| `capacity-test.js` | Fresh seat per iteration, all must succeed | `FIRST_SEAT_ID` | `RATE` (10), `DURATION` (`30s`), `PRE_ALLOCATED_VUS` (200), `MAX_VUS` (500) |
| `idempotency-replay.js` | Create → replay → key conflict contract | `FIRST_SEAT_ID` | `VUS` (10) |

Common environment variables:

```text
BASE_URL    Default: http://localhost:8080
RUN_ID      Optional run identifier; defaults to a timestamp
```

Expected outcomes:

```text
hot-seat.js          1 × 201 and (VUS - 1) × 409
concurrent-booking   1 × 201 and (ITERATIONS - 1) × 409
parallel-booking     VUS × 201 (fresh seats)
sustained-load       201 per fresh seat; 409 only if the range wraps
capacity-test        201 for every iteration (fresh seats)
idempotency-replay   201, then 201 with the same booking ID,
                     then 409 for the same key with a different seat
```

---

## Test data preparation

Create a new event with consecutive, available seats:

```bash
./load-tests/prepare-test-data.sh 100
```

The script prints export statements:

```text
export BASE_URL=http://localhost:8080
export EVENT_ID=42
export FIRST_SEAT_ID=1004000
export SEAT_COUNT=100
export SEAT_ID=1004000
```

Load them into the current shell:

```bash
eval "$(./load-tests/prepare-test-data.sh 100)"
```

The scripts address seats as `FIRST_SEAT_ID + offset` (or use `SEAT_ID`
directly), so the event must provide enough consecutive seats. The
preparation script warns if the returned seat IDs are not consecutive.

Create a separate event for each run so previously booked seats do not
turn fresh attempts into conflicts.

### Data sizing

```text
hot-seat              1 seat
concurrent-booking    1 seat
parallel-booking      VUS seats
idempotency-replay    VUS × 2 seats
sustained-load        at least RATE × DURATION seconds
capacity-test         at least RATE × DURATION + 1 seats
```

`constant-arrival-rate` can produce one extra iteration at the end of the
window, so capacity runs need one spare seat.

---

## Smoke runs

Small, controlled runs that validate the scripts and the API contract:

```bash
eval "$(./load-tests/prepare-test-data.sh 1)"
k6 run -e VUS=5 load-tests/hot-seat.js

eval "$(./load-tests/prepare-test-data.sh 10)"
k6 run -e VUS=5 load-tests/parallel-booking.js

eval "$(./load-tests/prepare-test-data.sh 1)"
k6 run -e VUS=5 -e ITERATIONS=5 load-tests/concurrent-booking.js

eval "$(./load-tests/prepare-test-data.sh 20)"
k6 run \
  -e RATE=2 \
  -e DURATION=5s \
  -e PRE_ALLOCATED_VUS=2 \
  -e MAX_VUS=5 \
  load-tests/sustained-load.js

eval "$(./load-tests/prepare-test-data.sh 20)"
k6 run \
  -e RATE=2 \
  -e DURATION=5s \
  -e PRE_ALLOCATED_VUS=5 \
  -e MAX_VUS=10 \
  load-tests/capacity-test.js

eval "$(./load-tests/prepare-test-data.sh 20)"
k6 run -e VUS=3 load-tests/idempotency-replay.js
```

`eval` replaces the environment of the current shell; run one block at a
time.

---

## Full runs

```bash
eval "$(./load-tests/prepare-test-data.sh 100)"
k6 run load-tests/hot-seat.js

eval "$(./load-tests/prepare-test-data.sh 100)"
k6 run load-tests/parallel-booking.js

eval "$(./load-tests/prepare-test-data.sh 1000)"
k6 run load-tests/sustained-load.js
```

Capacity runs are intentionally resource-intensive. Do not run high
rates without approval and without understanding the local machine's
limits:

```bash
eval "$(./load-tests/prepare-test-data.sh 3001)"
k6 run -e RATE=100 -e DURATION=30s load-tests/capacity-test.js
```

---

## Interpreting results

- `booking_created` and `booking_conflict` are business outcomes.
- `unexpected_responses` must remain zero; any value indicates an
  unexpected status code.
- `http_req_failed` counts HTTP 4xx/5xx responses. A hot-seat run will
  therefore report failures even when the system behaves correctly.
- `valid_booking_response` accepts only the expected status codes for
  the workload.
- `http_req_duration` thresholds are local example limits, not SLAs.
- Dropped iterations usually mean the load generator or the application
  is saturated; they are not automatically an application bug.

Local results depend on the machine, JDK, Docker resources, PostgreSQL
and Kafka state. Report them with the exact run configuration.

---

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| `400 Bad Request` | Missing `Idempotency-Key` (the scripts add it) |
| `404 Not Found` / `400` on every request | Seat range does not exist; prepare new test data |
| `409` on `parallel-booking` or `capacity-test` | Seats were already booked; create a fresh event |
| `Insufficient VUs` warning | Increase `MAX_VUS` |
| Threshold failure on latency | Local machine saturation; check Prometheus and Grafana |
