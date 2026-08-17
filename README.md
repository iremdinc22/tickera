# Tickera — High-Concurrency Ticket Booking System

Tickera is a backend engineering project focused on one core problem:

> How can a ticket booking system remain correct when many users try to reserve the same limited set of seats at the same time?

Building a basic booking API is straightforward. Preventing the same seat from being sold twice under concurrent load is not.

Tickera explores how a simple booking service evolves as concurrency, traffic, and system complexity increase.

The project follows a problem-driven approach:

```text
Reproduce the failure
        │
        ▼
Understand why it happens
        │
        ▼
Implement a solution
        │
        ▼
Test it under concurrency
        │
        ▼
Measure the result
        │
        ▼
Identify the next bottleneck
```

The current focus is concurrency control and database locking. Future stages will introduce distributed coordination, idempotency, event-driven processing, observability, and failure handling only when the system reaches problems that justify them.

---

## The Core Problem

Consider the last available seat for a concert.

Two users attempt to book the same seat at nearly the same time:

```text
User A                         User B
   │                              │
   ├──── Book Seat A1             │
   │                              ├──── Book Seat A1
   │                              │
   ▼                              ▼
READ AVAILABLE                 READ AVAILABLE
   │                              │
   ▼                              ▼
UPDATE BOOKED                  UPDATE BOOKED
   │                              │
   ▼                              ▼
CREATE BOOKING                 CREATE BOOKING

              💥 DOUBLE BOOKING
```

A naive implementation may appear correct when requests are processed sequentially:

```java
Seat seat = seatRepository.findById(seatId);

if (seat.getStatus() != SeatStatus.AVAILABLE) {
    throw new SeatNotAvailableException();
}

seat.setStatus(SeatStatus.BOOKED);
bookingRepository.save(booking);
```

The problem appears when multiple transactions execute concurrently.

Both transactions may read:

```text
Seat A1 → AVAILABLE
```

before either transaction updates the row.

That creates a race condition.

---

## System Invariant

The first concurrency invariant defined for Tickera is:

> A seat must never be successfully booked more than once.

This invariant must remain true regardless of how many users attempt to book the seat concurrently.

The first phase of the project was therefore designed to:

1. Reproduce a real double-booking race condition.
2. Verify the failure at the database level.
3. Introduce a concurrency-control strategy.
4. Repeat the same experiment.
5. Validate that the invariant now holds.

---

## Reproducing the Race Condition

Before adding concurrency protection, two booking requests were sent simultaneously for the same seat.

```bash
curl -s -X POST http://localhost:8080/bookings \
  -H "Content-Type: application/json" \
  -d '{"seatId":4,"userId":"user-a"}' &

curl -s -X POST http://localhost:8080/bookings \
  -H "Content-Type: application/json" \
  -d '{"seatId":4,"userId":"user-b"}' &

wait
```

Both requests succeeded.

The database confirmed that two booking records had been created for the same seat:

```text
 id | seat_id | user_id | status
----+---------+---------+---------
  4 |       4 | user-b  | PENDING
  5 |       4 | user-a  | PENDING
```

The system had violated its core invariant:

```text
Same seat
   │
   ├──── user-a → booking created
   │
   └──── user-b → booking created

❌ DOUBLE BOOKING
```

This confirmed that sequential correctness was not enough.

---

## First Solution — Pessimistic Row Locking

The first concurrency-control strategy implemented in Tickera is database-level pessimistic locking.

The seat is loaded with a write lock:

```java
@Lock(LockModeType.PESSIMISTIC_WRITE)
@Query("SELECT s FROM Seat s WHERE s.id = :id")
Optional<Seat> findByIdForUpdate(@Param("id") Long id);
```

The booking transaction uses the locking query:

```java
Seat seat = seatRepository.findByIdForUpdate(request.seatId())
        .orElseThrow(() -> new RuntimeException("Seat not found"));
```

Conceptually, PostgreSQL performs an operation similar to:

```sql
SELECT *
FROM seats
WHERE id = ?
FOR UPDATE;
```

The selected seat row remains locked until the transaction commits or rolls back.

---

## How Pessimistic Locking Changes the Flow

Without locking:

```text
Transaction A                  Transaction B

READ AVAILABLE                 READ AVAILABLE
      │                              │
      ▼                              ▼
UPDATE BOOKED                  UPDATE BOOKED
      │                              │
      ▼                              ▼
CREATE BOOKING                 CREATE BOOKING

                ❌
```

With pessimistic locking:

```text
Transaction A                  Transaction B

SELECT FOR UPDATE 🔒
      │
      │                        SELECT FOR UPDATE
      │                              │
      │                           WAITING
      ▼                              │
UPDATE BOOKED                       │
      │                              │
CREATE BOOKING                      │
      │                              │
COMMIT 🔓                            │
                                     ▼
                               READ BOOKED
                                     │
                                     ▼
                                409 Conflict
```

Only one transaction can successfully claim the same seat.

The second transaction waits until the first finishes, then reads the updated state and receives a conflict response.

---

## Manual Concurrency Verification

After introducing pessimistic locking, the same concurrent test was repeated.

Result:

```text
user-a → 201 Created
user-b → 409 Conflict
```

Instead of:

```text
201
201
```

the system now produced:

```text
201
409
```

Only one booking was persisted.

This verified that pessimistic row locking prevented the observed double-booking race condition.

---

## Concurrency Testing Note

Race conditions are timing-dependent and can be difficult to reproduce consistently.

During the initial experiment, an artificial delay was temporarily introduced between reading the seat and updating it:

```java
Thread.sleep(3000);
```

The delay widened the race-condition window and made concurrent behavior easier to observe.

After the race condition had been reproduced and pessimistic locking had been verified, the artificial delay was removed.

The current booking flow contains no artificial delay.

---

# Concurrent Load Testing

Manual two-request tests proved correctness for a small concurrency scenario, but they were not sufficient to evaluate the system under heavier contention.

k6 was introduced to generate repeatable concurrent traffic and collect latency and outcome metrics.

The first load test used:

```text
50 virtual users
        │
        ▼
50 concurrent booking attempts
        │
        ▼
The same AVAILABLE seat
```

This deliberately creates a highly contended workload.

The expected result is:

```text
50 booking attempts
        │
        ├──── exactly 1 → 201 Created
        │
        └──── remaining → 409 Conflict
```

---

## k6 Test Scenario

The test is located at:

```text
load-tests/concurrent-booking.js
```

The initial configuration:

```javascript
export const options = {
  vus: 50,
  iterations: 50,
};
```

Custom metrics are used to distinguish business conflicts from actual system failures:

```text
booking_created
booking_conflict
unexpected_responses
valid_booking_response
```

The test also defines thresholds:

```javascript
thresholds: {
  valid_booking_response: ['rate==1'],
  http_req_duration: ['p(95)<1000'],
  unexpected_responses: ['count==0'],
}
```

The current expectations are:

- Every response must be either `201 Created` or `409 Conflict`.
- No unexpected `500`, `404`, or other responses should occur.
- The p95 response latency should remain below the current local-development threshold.

---

## 50-User Contention Test Results

The first k6 contention test produced:

```text
booking_created.............: 1
booking_conflict............: 49
unexpected_responses........: 0
valid_booking_response......: 100.00%
```

Exactly one booking succeeded.

The remaining 49 booking attempts were correctly rejected.

```text
50 concurrent requests
        │
        ▼
     Same Seat
        │
   ┌────┴─────┐
   │          │
   ▼          ▼
1 × 201     49 × 409
Created     Conflict
```

No unexpected server responses occurred.

---

## Initial Local Performance Baseline

The same test produced the following latency measurements:

```text
Average response time : 80.36 ms
Median response time  : 80.96 ms
p90 response time     : 91.04 ms
p95 response time     : 91.62 ms
Maximum response time : 92.98 ms
```

Approximate request rate during the run:

```text
440 requests / second
```

These values are not intended to represent production performance.

The test was executed in a local development environment with:

- One Spring Boot application instance
- PostgreSQL running locally in Docker
- 50 total booking requests
- A single highly contended seat
- No distributed deployment

These numbers are treated only as a baseline for future architectural comparisons.

---

## Database-Level Verification

HTTP responses alone are not enough to prove correctness.

After the 50-user load test, the contested seat was queried directly:

```sql
SELECT id, seat_id, user_id, status, created_at
FROM bookings
WHERE seat_id = 7;
```

The database contained exactly one booking:

```text
 id | seat_id |  user_id  | status  |         created_at
----+---------+-----------+---------+----------------------------
  9 |       7 | user-13-0 | PENDING | 2026-08-17 21:25:59.001086

(1 row)
```

Therefore the full experiment resulted in:

```text
50 concurrent booking attempts
             │
             ▼
        HTTP Layer
             │
      ┌──────┴──────┐
      ▼             ▼
  1 Created     49 Conflicts
      │
      ▼
    Database
      │
      ▼
Exactly 1 booking
```

The invariant remained valid under the tested workload:

> At most one booking may successfully claim a seat.

---

## Expected Conflict vs. System Failure

k6 reports non-2xx responses such as `409 Conflict` as failed HTTP requests by default.

This means the contention test may show:

```text
http_req_failed: 98%
```

That number does not mean the application failed 98% of the time.

For this workload, most conflicts are expected:

```text
1 × 201 Created     → expected
49 × 409 Conflict   → expected
```

For this reason, custom k6 metrics are used to separate expected business outcomes from actual application failures.

The important metrics are:

```text
booking_created
booking_conflict
unexpected_responses
valid_booking_response
```

---

# Why Pessimistic Locking Is Not the Final Answer

Pessimistic locking currently preserves correctness, but it introduces another problem: contention.

If many users compete for the same seat:

```text
Request 1 ─────► 🔒 Seat A1

Request 2 ─────► waiting...
Request 3 ─────► waiting...
Request 4 ─────► waiting...
Request 5 ─────► waiting...
Request 6 ─────► waiting...
```

The system may remain correct while latency increases and throughput decreases.

This introduces the next set of engineering questions:

> How does pessimistic locking behave at 100, 500, or 1000 concurrent requests?

> What changes when users book different seats instead of the same seat?

> Is the bottleneck row-lock contention, database capacity, or application throughput?

> Would optimistic concurrency behave differently?

These questions drive the next stage of the project.

---

# Current Architecture

The current architecture is intentionally simple:

```text
                 ┌─────────────────┐
                 │     Client      │
                 └────────┬────────┘
                          │
                          ▼
                 ┌─────────────────┐
                 │    REST API     │
                 └────────┬────────┘
                          │
                          ▼
                 ┌─────────────────┐
                 │BookingController│
                 └────────┬────────┘
                          │
                          ▼
                 ┌─────────────────┐
                 │ BookingService  │
                 └────────┬────────┘
                          │
               ┌──────────┴──────────┐
               ▼                     ▼
       ┌──────────────┐       ┌──────────────┐
       │SeatRepository│       │BookingRepo   │
       └───────┬──────┘       └───────┬──────┘
               │                      │
               └──────────┬───────────┘
                          ▼
                 ┌─────────────────┐
                 │   PostgreSQL    │
                 │                 │
                 │ SELECT ...      │
                 │ FOR UPDATE      │
                 └─────────────────┘
```

More infrastructure will be introduced only when a demonstrated problem justifies it.

Redis, Kafka, distributed coordination, retries, observability, and other components are intentionally not added prematurely.

---

# Current Features

The system currently supports:

- Event creation
- Seat creation
- Seat availability tracking
- Booking creation
- Booking conflict handling
- PostgreSQL persistence
- Transactional booking operations
- Pessimistic row locking
- Protection against concurrent double booking
- Swagger / OpenAPI documentation
- Dockerized PostgreSQL
- k6 concurrent load testing
- Custom booking outcome metrics
- Latency measurement
- Database-level concurrency verification

---

# Tech Stack

## Backend

- Java 21
- Spring Boot
- Spring Data JPA
- Hibernate
- Maven

## Database

- PostgreSQL 17

## Infrastructure

- Docker
- Docker Compose

## API

- REST
- OpenAPI
- Swagger UI

## Performance Testing

- k6

## Planned

- Testcontainers
- Redis
- Kafka
- Prometheus
- Grafana
- GitHub Actions

---

# Engineering Roadmap

```text
Basic Event & Seat Model
          │
          ▼
Basic Booking Flow
          │
          ▼
Reproduce Double Booking
          │
          ▼
Pessimistic Row Locking
          │
          ▼
Verify Concurrent Correctness
          │
          ▼
Remove Artificial Test Delay
          │
          ▼
k6 Concurrent Load Testing
          │
          ▼
Hot-Seat Contention Testing        ← CURRENT
          │
          ▼
Parallel Multi-Seat Testing
          │
          ▼
Optimistic Concurrency
          │
          ▼
Compare Concurrency Strategies
          │
          ▼
Redis / Distributed Coordination
          │
          ▼
Idempotency
          │
          ▼
Kafka & Event-Driven Processing
          │
          ▼
Failure Handling
          │
          ▼
Observability
          │
          ▼
Performance & Scalability Testing
```

---

# Current Findings

The experiments performed so far demonstrate several important properties of the system.

### Sequential correctness does not guarantee concurrent correctness

The initial booking flow behaved correctly when requests arrived sequentially but allowed double booking under concurrent execution.

### The race condition can be reproduced

Concurrent requests successfully demonstrated that two transactions could observe the same seat as `AVAILABLE`.

### PostgreSQL pessimistic locking prevents the observed double booking

Using a pessimistic write lock serializes competing transactions for the same seat.

### Business conflicts are not the same as system failures

A `409 Conflict` is an expected outcome when another transaction has already claimed the seat.

### The booking invariant holds under the current tested workload

With 50 concurrent booking attempts against the same seat:

```text
1 successful booking
49 booking conflicts
0 unexpected responses
1 persisted booking
```

The system remained correct.

---

# Next Milestone

The next stage is to compare two different workload patterns.

## Scenario 1 — Hot Seat

Many users compete for the same seat:

```text
50 / 100 / 500 / 1000 users
              │
              ▼
           Seat A1
              │
              ▼
       Heavy contention
```

This scenario stresses the pessimistic locking strategy.

Metrics of interest:

- Successful bookings
- Conflict count
- Unexpected failures
- Average latency
- p95 latency
- p99 latency
- Throughput

## Scenario 2 — Parallel Seats

Users attempt to book different seats:

```text
User 1 ─────► Seat A1
User 2 ─────► Seat A2
User 3 ─────► Seat A3
User 4 ─────► Seat A4
   ...           ...
```

This workload introduces far less row-level contention.

Comparing both scenarios will help separate:

```text
General application/database cost
              │
              ├────────► Lock contention cost
              │
              ▼
        Measured behavior
```

The results will guide future concurrency and architecture decisions.

---

# Project Philosophy

Tickera is intentionally not developed by adding technologies just because they are commonly associated with distributed systems.

Each architectural component should answer a concrete engineering problem.

```text
Problem
   │
   ▼
Experiment
   │
   ▼
Measurement
   │
   ▼
Trade-off
   │
   ▼
Architecture Decision
```

The goal is not to build the largest possible technology stack.

The goal is to understand why each architectural decision exists and how it changes the correctness, performance, and scalability characteristics of the system.