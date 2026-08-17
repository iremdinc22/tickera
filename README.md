# Tickera — High-Concurrency Ticket Booking System

Tickera is a backend engineering project focused on a deceptively simple problem:

> What happens when thousands of users try to book the same limited set of seats at the same time?

A basic booking API is easy to build. Making sure that the same seat is never sold twice under concurrent load is not.

This project explores the engineering problems behind high-demand ticketing systems: concurrency control, database locking, distributed coordination, event-driven processing, failure handling, idempotency, and observability.

---

## The Problem

Consider the last available seat for a concert.

Two users send a booking request at almost exactly the same time:

```text
User A                         User B
   │                              │
   ├──── Book Seat A1 ───────────►│
   │                              │
   │      Seat = AVAILABLE        │
   │                              │
   │                    Seat = AVAILABLE
   │                              │
   ▼                              ▼
Booking created              Booking created

              💥 DOUBLE BOOKING
```

A naive implementation may appear perfectly correct when requests are processed sequentially:

```java
Seat seat = seatRepository.findById(seatId);

if (seat.getStatus() != SeatStatus.AVAILABLE) {
    throw new SeatNotAvailableException();
}

seat.setStatus(SeatStatus.BOOKED);
bookingRepository.save(booking);
```

The problem appears when multiple requests execute concurrently.

Both transactions can read the seat while its state is still `AVAILABLE`.

---

## Reproducing the Race Condition

Before implementing a concurrency-control mechanism, the race condition was intentionally reproduced.

Two booking requests were sent simultaneously for the same seat:

```bash
curl -X POST http://localhost:8080/bookings \
  -H "Content-Type: application/json" \
  -d '{"seatId":4,"userId":"user-a"}' &

curl -X POST http://localhost:8080/bookings \
  -H "Content-Type: application/json" \
  -d '{"seatId":4,"userId":"user-b"}' &

wait
```

Both requests successfully created a booking.

The database contained:

```text
 id | seat_id | user_id | status
----+---------+---------+---------
  4 |       4 | user-b  | PENDING
  5 |       4 | user-a  | PENDING
```

The same seat had been booked twice.

This establishes an important invariant for the system:

> A seat must never have more than one successful booking.

---

## First Solution — Pessimistic Locking

The first concurrency strategy implemented in Tickera is database-level pessimistic locking.

The seat is retrieved using a write lock:

```java
@Lock(LockModeType.PESSIMISTIC_WRITE)
@Query("SELECT s FROM Seat s WHERE s.id = :id")
Optional<Seat> findByIdForUpdate(@Param("id") Long id);
```

Conceptually, PostgreSQL performs an operation similar to:

```sql
SELECT *
FROM seats
WHERE id = ?
FOR UPDATE;
```

The selected seat row remains locked for the duration of the transaction.

The booking flow becomes:

```text
User A                         User B
   │                              │
   ├── SELECT seat FOR UPDATE     │
   │          🔒                  │
   │                              │
   │                    SELECT same seat
   │                              │
   │                       waits for lock
   │                              │
   ├── Seat → BOOKED              │
   ├── COMMIT                     │
   │          🔓                  │
   │                              │
   │                    reads Seat → BOOKED
   │                              │
   │                       409 Conflict
   ▼                              ▼
201 Created                  Booking rejected
```

The concurrency experiment was then repeated.

The result:

```text
user-a → 201 Created
user-b → 409 Conflict
```

Only one booking was successfully created.

---

## Why Pessimistic Locking Works

Without locking, two transactions can perform the following sequence:

```text
Transaction A                  Transaction B

READ AVAILABLE                 READ AVAILABLE
      │                              │
      ▼                              ▼
UPDATE BOOKED                  UPDATE BOOKED
      │                              │
      ▼                              ▼
CREATE BOOKING                 CREATE BOOKING
```

Each transaction makes its decision using stale information.

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
COMMIT 🔓                            │
                                     ▼
                               READ BOOKED
                                     │
                                     ▼
                                409 Conflict
```

Only one transaction can modify the seat at a time.

---

## Why This Is Not the Final Architecture

Pessimistic locking solves the immediate correctness problem, but introduces another engineering concern: lock contention.

Imagine thousands of users competing for the same seat:

```text
Request 1 ──────► 🔒 Seat A1

Request 2 ──────► waiting...
Request 3 ──────► waiting...
Request 4 ──────► waiting...
Request 5 ──────► waiting...
Request 6 ──────► waiting...
```

The system remains correct, but throughput and latency may suffer.

This introduces the next engineering question:

> How does the booking system behave when hundreds or thousands of concurrent requests compete for the same resources?

Correctness alone is not enough.

The system must eventually balance:

- Correctness
- Throughput
- Latency
- Lock contention
- Scalability
- Failure recovery

---

## Architecture

The current architecture is intentionally simple.

```text
Client
  │
  ▼
REST API
  │
  ▼
BookingController
  │
  ▼
BookingService
  │
  ├──── SeatRepository
  │          │
  │          ▼
  │      PostgreSQL
  │
  └──── BookingRepository
             │
             ▼
         PostgreSQL
```

More infrastructure will be introduced only when the system encounters a problem that requires it.

---

## Engineering Roadmap

Tickera is developed incrementally.

Each stage introduces a new engineering problem before implementing a solution.

```text
Basic Booking
      │
      ▼
Race Condition
      │
      ▼
Pessimistic Locking
      │
      ▼
Concurrent Load Testing
      │
      ▼
Optimistic Concurrency
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
Performance Testing
```

The goal is not simply to build another ticket booking API.

The goal is to explore how a booking system evolves as concurrency, traffic, and distributed-system complexity increase.

---

## Tech Stack

### Backend

- Java 21
- Spring Boot
- Spring Data JPA
- Hibernate

### Database

- PostgreSQL 17

### Infrastructure

- Docker

### API

- REST
- OpenAPI / Swagger

### Planned

- Redis
- Kafka
- Testcontainers
- k6
- Prometheus
- Grafana
- GitHub Actions

---

## Current Features

The system currently supports:

- Event creation
- Seat creation
- Seat availability tracking
- Booking creation
- Conflict handling for unavailable seats
- Concurrent booking protection
- PostgreSQL pessimistic row locking

---

## Concurrency Experiment

The development process intentionally reproduced a real double-booking scenario before implementing the solution.

### Without Locking

```text
2 concurrent requests
        │
        ▼
2 successful bookings

❌ Same seat booked twice
```

### With Pessimistic Locking

```text
2 concurrent requests
        │
        ├────► 201 Created
        │
        └────► 409 Conflict

✅ Only one booking persisted
```

This experiment verifies the first concurrency invariant of Tickera:

> At most one booking may successfully claim a seat.

---

## Development Note

During the initial concurrency experiment, an artificial delay is temporarily introduced between reading and updating the seat:

```java
try {
    Thread.sleep(3000);
} catch (InterruptedException e) {
    Thread.currentThread().interrupt();
}
```

This delay intentionally widens the race-condition window so that concurrent behavior can be reproduced consistently during development.

It is not part of the production design and will be removed after the experiment.

---

## Next Milestone

The next step is to remove the artificial delay and test the booking endpoint under higher concurrent load.

The objective will be to measure:

```text
Concurrent Requests
        │
        ▼
Booking Service
        │
        ▼
PostgreSQL Row Locks
        │
        ├── Successful bookings
        ├── Conflicts
        ├── Response latency
        └── Lock contention
```

These measurements will guide the next architectural decisions rather than introducing infrastructure without a demonstrated need.