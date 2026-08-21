# Tickera — High-Concurrency Ticket Booking System

Tickera is a backend engineering project exploring how a ticket booking system can remain correct, reliable, and observable under concurrent load.

The project started with a simple question:

> What happens when many users try to book the same seat at the same time?

It evolved into experiments around concurrency control, idempotency, temporary seat reservations, distributed coordination, load testing, database behavior, observability, event-driven processing, reliable event publishing, and duplicate event handling.

---

## Core Problem

A naive booking flow may look correct when requests are processed sequentially:

```java
Seat seat = seatRepository.findById(seatId);

if (seat.getStatus() != SeatStatus.AVAILABLE) {
    throw new SeatNotAvailableException();
}

seat.setStatus(SeatStatus.BOOKED);
bookingRepository.save(booking);
```

Under concurrent requests, multiple transactions may observe the same seat as `AVAILABLE` before either commits.

```text
User A                     User B
   │                          │
   ▼                          ▼
READ AVAILABLE            READ AVAILABLE
   │                          │
   ▼                          ▼
UPDATE BOOKED             UPDATE BOOKED
   │                          │
   ▼                          ▼
CREATE BOOKING            CREATE BOOKING
          DOUBLE BOOKING
```

The main invariant of the system is:

> A seat must never be successfully booked more than once.

---

## Concurrency Control

Tickera implements and compares two database-level concurrency strategies.

### Pessimistic Locking

Seats can be retrieved using a write lock:

```java
@Lock(LockModeType.PESSIMISTIC_WRITE)
@Query("SELECT s FROM Seat s WHERE s.id = :id")
Optional<Seat> findByIdForUpdate(@Param("id") Long id);
```

Conceptually:

```sql
SELECT *
FROM seats
WHERE id = ?
FOR UPDATE;
```

The first transaction locks the seat row until commit. Concurrent requests wait and later observe the seat as already booked.

### Optimistic Locking

The `Seat` entity also contains a version field:

```java
@Version
@Column(nullable = false)
private Long version;
```

Concurrent updates using an outdated version fail with an optimistic locking conflict.

---

## Pessimistic vs Optimistic Locking

Both strategies were tested under hot-seat and parallel-seat workloads.

| Workload | Strategy | Result | Avg Latency | p95 Latency | Throughput |
|---|---|---:|---:|---:|---:|
| Hot seat | Pessimistic | 1 created / 99 conflicts | 224.24 ms | 245.34 ms | ~359.76 req/s |
| Hot seat | Optimistic | 1 created / 99 conflicts | 317.55 ms | 324.06 ms | ~292.18 req/s |
| Parallel seats | Pessimistic | 100 created | 135.07 ms | 180.91 ms | ~467.76 req/s |
| Parallel seats | Optimistic | 100 created | 135.43 ms | 155.89 ms | ~531.19 req/s |

Observed locally:

- Pessimistic locking performed better under extreme hot-seat contention.
- Optimistic locking performed better in throughput and p95 latency when users booked independent seats.
- Neither strategy should be considered universally superior.
- The contention pattern matters.

---

## Automated Concurrency Testing

Concurrency correctness is protected with JUnit and Testcontainers.

Tests run against PostgreSQL 17.

```text
JUnit
  │
  ▼
Testcontainers
  │
  ▼
PostgreSQL 17
  │
  ▼
20 concurrent operations
  │
  ▼
Verify invariants
```

The tests verify that:

- Exactly one booking is created for a contested seat.
- The seat ends in `BOOKED`.
- Pessimistic locking preserves the invariant.
- Optimistic locking preserves the invariant.

```bash
./mvnw test
```

---

## Idempotent Booking

Concurrency control prevents competing transactions from successfully claiming the same seat. It does not solve repeated delivery of the same logical request.

```text
Client
  │
  ├── POST /bookings ─────► Server
  │                           │
  │                           ▼
  │                      Booking created
  │
  │      response lost
  │
  └── retry ──────────────► Server
```

Tickera supports an `Idempotency-Key` header:

```http
POST /bookings
Idempotency-Key: test-key-001
```

Each idempotency record stores:

```text
idempotency_key
request_hash
status
booking_id
created_at
```

A SHA-256 request fingerprint prevents the same key from being reused for a different booking request.

```text
same key + same request
        ↓
return existing booking

same key + different request
        ↓
409 Conflict
```

### Concurrent Idempotency

Multiple identical retries can arrive at the same time and initially observe that the idempotency key does not exist.

Tickera therefore uses explicit states:

```text
PROCESSING
COMPLETED
```

The database-level unique constraint on `idempotency_key` acts as an ownership mechanism.

> Seat locking protects a resource. Idempotency protects a logical operation.

---

## Redis Seat Holds

Tickera uses Redis for short-lived seat reservations before the final booking is committed to PostgreSQL.

```text
AVAILABLE
    │
    ▼
Redis HOLD
SET NX + TTL
    │
    ▼
Hold ownership validation
    │
    ▼
PostgreSQL booking
    │
    ▼
BOOKED
    │
    ▼
Redis hold released
```

Seat holds provide:

- Atomic acquisition using Redis `SET NX`
- Configurable expiration using TTL
- Owner-aware atomic release
- Protection against booking a hold owned by another user
- Automatic hold release after successful booking
- Validation against non-existing and already booked seats
- Shared coordination across multiple application instances

Redis is used for temporary reservation state. PostgreSQL remains the source of truth for permanent booking state and final transaction correctness.

---

## Multi-Instance Coordination

Two independent Tickera instances were run against the same Redis and PostgreSQL infrastructure.

Concurrent hold requests sent through different instances for the same seat produced one winner and one conflict.

This verifies that hold state is shared distributed state rather than instance-local JVM state.

---

# Kafka & Event-Driven Processing

Successful bookings produce a `BookingCreatedEvent` delivered through Kafka.

```text
Booking
   │
   ▼
BookingCreatedEvent
   │
   ▼
Kafka: booking-created
   │
   ▼
BookingCreatedEventConsumer
```

The initial implementation exposed an important distributed-systems failure window:

```text
PostgreSQL COMMIT ✅
        │
        ▼
Kafka publish ❌
        │
        ▼
Booking exists, but event can be lost
```

A database transaction cannot atomically commit both PostgreSQL state and a normal Kafka publish.

Tickera therefore uses the **Transactional Outbox Pattern**.

---

## Transactional Outbox

Booking state and the event that must eventually be published are persisted in the same PostgreSQL transaction.

```text
Booking request
      │
      ▼
PostgreSQL transaction
      │
      ├── Booking / Seat changes
      │
      └── INSERT OutboxEvent
      │
      ▼
    COMMIT
      │
      ▼
outbox_events
   PENDING
      │
      ▼
OutboxPublisher
      │
      ▼
    Kafka
      │
      ▼
outbox_events
  PUBLISHED
```

If Kafka is unavailable, the booking transaction can still commit and the outbox event remains durable as `PENDING`.

When Kafka becomes available again, the publisher retries the pending event.

Only a successful Kafka publish transitions the event to `PUBLISHED`.

This removes the original database-to-Kafka event-loss window.

---

## Multi-Instance Outbox Publishing

Outbox publishing was also exercised with multiple Tickera instances sharing PostgreSQL and Kafka.

```text
              PostgreSQL
                  │
            outbox_events
                  │
          ┌───────┴───────┐
          ▼               ▼
     Tickera A        Tickera B
          │               │
          └───────┬───────┘
                  ▼
                Kafka
```

This matters because multiple scheduled publishers must not independently publish the same pending event.

For the tested booking event, Kafka inspection confirmed a single record for the target booking.

---

## Idempotent Kafka Consumer

Reliable publishing does not mean duplicate delivery can never occur.

For example:

```text
Kafka publish ✅
      │
      ▼
application crash
      │
      ▼
local PUBLISHED state not recorded
      │
      ▼
event may be published again
```

Tickera therefore tracks consumed event IDs in:

```text
processed_events
```

Consumer flow:

```text
Kafka event
    │
    ▼
Consumer
    │
    ▼
event_id already processed?
    │
 ┌──┴───┐
 │      │
NO     YES
 │      │
 ▼      ▼
PROCESS IGNORE
 │
 ▼
processed_events
```

`event_id` is the primary key of `processed_events`, providing a durable duplicate-detection boundary.

An integration test publishes the same `BookingCreatedEvent` twice with the same event ID and verifies that it is logically processed only once.

Kafka integration tests use a real Kafka broker through Testcontainers.

---

## Load Testing with k6

k6 is used for performance and load testing.

JUnit + Testcontainers answers:

> Is the system correct under concurrency?

k6 answers:

> How does the system behave under load?

Workloads include:

- Hot-seat contention
- Parallel-seat booking
- Sustained load
- Capacity testing
- Constant arrival-rate experiments

A 100-user hot-seat test produced:

```text
1 booking created
99 conflicts
```

confirming that the single-booking invariant remained valid under load.

---

## Capacity Investigation

The system was gradually tested from low sustained traffic toward local saturation.

```text
10 req/s       Stable
50 req/s       Stable
100 req/s      Stable
200 req/s      Stable
400 req/s      Stable
800 req/s      Minor dropped iterations
1200 req/s     Increased latency and drops
2000 req/s     Clear saturation signals
```

These are local development-machine measurements and should not be interpreted as production capacity.

---

## Bottleneck Experiments

Several possible bottlenecks were investigated independently.

- Increasing HikariCP from 10 to 20 did not improve throughput.
- Representative PostgreSQL booking queries were very fast.
- Temporarily disabling synchronous commits did not materially improve throughput.
- Increasing Tomcat worker threads from 200 to 400 removed thread saturation but did not materially improve throughput.
- Increasing k6 concurrency increased latency rather than useful throughput near saturation.

> Near saturation, adding more concurrency can increase waiting without increasing useful throughput.

---

## Observability

Runtime behavior is monitored using:

```text
Spring Boot
    │
    ▼
Actuator
    │
    ▼
Micrometer
    │
    ▼
Prometheus
    │
    ▼
Grafana
```

The dashboard tracks:

- HTTP request rate
- Booking API p95 latency
- HikariCP active / idle / pending connections
- Tomcat worker threads
- JVM heap usage
- CPU usage
- Booking transaction duration
- Seat lock duration
- Booking persistence duration

---

## Test Infrastructure

Integration testing uses real infrastructure through Testcontainers where appropriate.

```text
PostgreSQL
├── Pessimistic concurrency
├── Optimistic concurrency
└── Idempotency

Redis
├── Concurrent seat holds
├── TTL expiration
├── Invalid seat validation
├── Hold ownership
└── Hold → Booking → Release

Kafka
└── Duplicate BookingCreatedEvent → process once
```

---

## Engineering Documentation

Detailed technical notes and experiment results are available in the `docs/` directory:

- [Concurrency Control](docs/concurrency.md)
- [Idempotency](docs/idempotency.md)
- [Redis Seat Holds](docs/redis-seat-holds.md)
- [Performance Testing](docs/performance-testing.md)
- [Observability](docs/observability.md)
- [Kafka & Reliable Event Processing](docs/kafka-event-processing.md)

---

## Current Architecture

```text
                         Client / k6
                              │
                              ▼
                        Spring Boot API
                              │
                  ┌───────────┴────────────┐
                  ▼                        ▼
         SeatHoldController        BookingController
                  │                        │
                  ▼                        ▼
          SeatHoldService            BookingService
                  │                        │
                  ▼              ┌─────────┴─────────────┐
                Redis            │ PostgreSQL Transaction │
           SET NX + TTL          │                       │
                                 ├── Booking / Seat      │
                                 ├── Idempotency         │
                                 └── OutboxEvent         │
                                          │
                                          ▼
                                   outbox_events
                                      PENDING
                                          │
                                          ▼
                                   OutboxPublisher
                                          │
                                          ▼
                                        Kafka
                                  booking-created
                                          │
                                          ▼
                             BookingCreatedEventConsumer
                                          │
                                          ▼
                                  processed_events
```

PostgreSQL remains the source of truth for permanent booking state.

Redis coordinates temporary seat holds.

Kafka carries asynchronous domain events, while the outbox and processed-event records provide durable reliability boundaries around event delivery.

---

## Tech Stack

### Backend

- Java 21
- Spring Boot 4
- Spring Data JPA
- Spring Data Redis
- Spring Kafka
- Hibernate
- Maven

### Data & Messaging

- PostgreSQL 17
- Redis 7.4
- Apache Kafka
- HikariCP
- `pg_stat_statements`

### Testing

- JUnit 5
- Testcontainers
- PostgreSQL Testcontainers
- Redis Testcontainers
- Kafka Testcontainers
- Spring Kafka Test
- k6

### Observability

- Spring Boot Actuator
- Micrometer
- Prometheus
- Grafana

### API & Infrastructure

- REST
- Swagger / OpenAPI
- Docker
- Docker Compose

---

## Current Features

- Event and seat management
- Booking creation
- Seat availability tracking
- Pessimistic row locking
- Optimistic locking with `@Version`
- Double-booking protection
- Idempotent booking requests
- SHA-256 request fingerprinting
- Idempotency key conflict detection
- `PROCESSING` / `COMPLETED` idempotency states
- Concurrent idempotency coordination
- Redis-based temporary seat holds
- Atomic hold acquisition using `SET NX`
- Configurable hold TTL
- Owner-aware atomic hold release
- Hold ownership enforcement during booking
- Hold-to-booking lifecycle
- Multi-instance Redis coordination
- Kafka `BookingCreatedEvent` producer and consumer
- Transactional Outbox Pattern
- Durable outbox lifecycle
- Kafka failure recovery through outbox retry
- Multi-instance outbox publishing
- Idempotent Kafka consumer
- `processed_events` duplicate detection
- Kafka integration testing with Testcontainers
- JUnit concurrency integration tests
- PostgreSQL Testcontainers tests
- Redis integration tests
- k6 load and capacity testing
- Prometheus / Grafana observability
- HikariCP monitoring
- Tomcat thread monitoring
- PostgreSQL performance investigation

---

## Engineering Roadmap

```text
Basic Booking Flow
        │
        ▼
Reproduce Double Booking
        │
        ▼
Pessimistic Locking
        │
        ▼
Load Testing
        │
        ▼
Observability
        │
        ▼
Capacity Investigation
        │
        ▼
Optimistic Locking
        │
        ▼
Compare Locking Strategies
        │
        ▼
Automated Concurrency Tests
        │
        ▼
Idempotency
        │
        ▼
Concurrent Idempotency
        │
        ▼
Redis Seat Holds                       ✓
        │
        ▼
TTL & Hold Ownership                   ✓
        │
        ▼
Multi-Instance Coordination            ✓
        │
        ▼
Kafka / Event-Driven Processing        ✓
        │
        ▼
Reliable Event Publishing              ✓
        │
        ▼
Transactional Outbox                   ✓
        │
        ▼
Kafka Failure & Recovery               ✓
        │
        ▼
Multi-Instance Outbox Publishing       ✓
        │
        ▼
Idempotent Consumers                   ✓
        │
        ▼
Production Retry / Backoff             ← NEXT
        │
        ▼
GitHub Actions / CI
```

---

## Key Engineering Findings

- Sequential correctness does not imply concurrent correctness.
- Database locking can prevent double booking.
- Optimistic and pessimistic locking have different trade-offs depending on contention.
- Connection-pool saturation does not automatically mean the pool should be enlarged.
- More application threads do not automatically produce more throughput.
- More load-generator concurrency does not automatically produce more throughput.
- SQL execution time alone does not explain end-to-end system latency.
- Idempotency and locking solve different problems.
- Application-level `check → insert` logic is insufficient for concurrency-safe idempotency.
- Database constraints can participate in coordination and ownership mechanisms.
- Redis is well suited to short-lived reservation state with TTL.
- Redis seat holds do not replace PostgreSQL transaction correctness.
- Shared Redis state can coordinate multiple application instances.
- A successful database commit does not guarantee a subsequent Kafka publish.
- Persisting business state and an outbox event in the same transaction closes the database-to-message-broker event-loss window.
- Reliable event delivery must assume retries and possible duplicate delivery.
- Consumers should be idempotent when processing events with at-least-once characteristics.
- Correctness invariants should be protected by repeatable automated tests.

---

## Next — Production-Grade Outbox Retry

The current outbox publisher retries pending events.

The next reliability step is to make retry behavior explicit and operationally safer:

- Retry attempt count
- `FAILED` status
- Last error information
- Retry backoff
- `nextRetryAt`
- Maximum retry policy
- Avoiding infinite retries for permanently invalid events
- Metrics for pending, published, retried, and failed outbox events

---

## Project Philosophy

Tickera is intentionally developed by introducing infrastructure only when a demonstrated problem justifies it.

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

The goal is to understand why each architectural decision exists and how it affects correctness, performance, reliability, and scalability.