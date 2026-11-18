# Tickera — High-Concurrency Ticket Booking System

[![CI](https://github.com/iremdinc22/tickera/actions/workflows/ci.yml/badge.svg)](https://github.com/iremdinc22/tickera/actions/workflows/ci.yml)

**Version 1.0.0** — see the [changelog](CHANGELOG.md) and
[release notes](docs/releases/v1.0.0.md).

Tickera is a backend engineering project exploring how a ticket booking system can remain correct, reliable, and observable under concurrent load.

The project started with a simple question:

> What happens when many users try to book the same seat at the same time?

It evolved into experiments around concurrency control, idempotency, temporary seat reservations, distributed coordination, load testing, database behavior, observability, event-driven processing, reliable event publishing, and duplicate event handling.

---

## Engineering Objectives

Tickera is built around correctness first:

```text
A seat must never be booked twice
A client retry must not create a second booking
A committed booking must eventually produce an event
A duplicate event must not create a duplicate side effect
A slow dependency must not corrupt booking state
```

The project intentionally implements each mechanism only after reproducing
the failure it addresses, then measures the trade-off.

---

## Quick Start

Requirements: Java 21, Docker.

```bash
# 1. Start infrastructure (PostgreSQL, Redis, Kafka, Prometheus, Grafana)
docker compose up -d

# 2. Run the application
./mvnw spring-boot:run
```

The application starts on `http://localhost:8080`.

```text
Actuator health    http://localhost:8080/actuator/health
OpenAPI UI         http://localhost:8080/swagger-ui/index.html
Prometheus         http://localhost:9090
Grafana            http://localhost:3000
```

Run the full test suite:

```bash
./mvnw clean verify
```

The suite uses Testcontainers and does not require the Compose services to
be running.

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

## Booking Lifecycle

```text
Seat: AVAILABLE
      │
      │ optional: POST /seats/{id}/hold
      ▼
Redis hold (SET NX + TTL)
      │
      │ POST /bookings or POST /bookings/held
      ▼
PostgreSQL transaction
├── Seat → BOOKED
├── Booking INSERT
├── Idempotency record → COMPLETED
└── Outbox event → PENDING
      │
      ▼
COMMIT
      │
      ├── Redis hold released
      │
      └── Outbox publisher
              │
              ▼
        Kafka booking-created
              │
              ▼
        Idempotent consumer
              │
              ▼
        processed_events
```

Failure at any step leaves the system in a state that can be retried or
recovered:

- A booking conflict returns `409` and does not change the seat.
- A retried request with the same idempotency key returns the existing
  booking.
- A Kafka outage leaves the outbox event `PENDING`; the publisher retries.
- A duplicate Kafka delivery is ignored by the consumer.

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

These are historical local measurements from the earlier implementation.
They are not production benchmarks; see
[Performance Testing](docs/performance-testing.md).

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

## Outbox Retry, Backoff and Stale Recovery

Retry behavior is implemented, not planned.

```text
PENDING
   │
   ▼
PROCESSING
   │
   ├── Kafka ACK ──────────────► PUBLISHED
   │
   └── failure
          │
          ▼
     retryCount + 1
          │
          ├── < 5 ──► PENDING + nextRetryAt
          │
          └── = 5 ──► FAILED + lastError
```

- Backoff: `2s → 5s → 10s → 30s → FAILED`
- Maximum 5 attempts
- `nextRetryAt` prevents immediate retry loops
- `lastError` stores the most recent failure
- A stale `PROCESSING` event older than 2 minutes is recovered by a
  separate scheduled job
- `FAILED` events can be requeued manually through
  `POST /admin/outbox/{eventId}/requeue`
- Prometheus exposes published, retry, stale-recovery, pending and failed
  outbox metrics

Delivery is **at-least-once**. A publish timeout can produce a duplicate
record, so the consumer records `event_id` in `processed_events` and
ignores repeated deliveries.

---

## Load Testing with k6

k6 is used for performance and load testing.

JUnit + Testcontainers answers:

> Is the system correct under concurrency?

k6 answers:

> How does the system behave under load?

Maintained scripts:

| Script | Workload |
|---|---|
| `hot-seat.js` | All virtual users book the same seat |
| `concurrent-booking.js` | Fixed number of attempts on one seat |
| `parallel-booking.js` | Each virtual user books a different seat |
| `sustained-load.js` | Constant arrival rate across a seat range |
| `capacity-test.js` | Fresh seat per iteration, all must succeed |
| `idempotency-replay.js` | Create → replay → key-conflict contract |

All scripts use the current API contract, including the required
`Idempotency-Key` header, and accept `BASE_URL` plus workload parameters
through environment variables.

Prepare fresh test data, then run:

```bash
eval "$(./load-tests/prepare-test-data.sh 100)"
k6 run load-tests/parallel-booking.js
```

See [`load-tests/README.md`](load-tests/README.md) for data sizing,
smoke-run commands, configuration and troubleshooting.

A 100-user hot-seat test historically produced:

```text
1 booking created
99 conflicts
```

These results are from the earlier implementation. Current runs must be
measured again; see [Performance Testing](docs/performance-testing.md).

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

These are historical local development-machine measurements from the
earlier implementation. They should not be interpreted as production
capacity or as current benchmark results.

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

The provisioned dashboards track:

- HTTP request rate, 5xx error rate and p95/p99 latency
- Booking throughput, conflicts (HTTP 409), success ratio and p95 latency
- HikariCP active / idle / pending connections
- Tomcat worker threads
- JVM heap usage
- CPU usage
- Outbox publish, retry and stale-recovery rates
- Outbox pending and failed event counts

Two dashboards are provisioned from files:

```text
Tickera — Application Overview   uid: tickera-application-overview
Tickera — Outbox & Kafka Reliability  uid: tickera-outbox-kafka
```

```text
Prometheus    http://localhost:9090
Grafana       http://localhost:3000
```

Prometheus rules provide development alert examples. They have no
Alertmanager notification channel.

See [Observability](docs/observability.md) for metric names, dashboard
details and provisioning files.

---

## Automated Testing and CI

The full suite runs with:

```bash
./mvnw clean verify
```

Current status: **35 tests**, all passing on Java 21.

```text
JUnit 5 + Testcontainers
│
├── PostgreSQL
│   ├── Pessimistic concurrency
│   ├── Optimistic concurrency
│   ├── Idempotency
│   ├── Redis hold → booking lifecycle
│   ├── Outbox retry and stale recovery
│   └── Outbox metric commit / rollback semantics
│
├── Redis
│   ├── Concurrent seat holds
│   ├── TTL expiration
│   ├── Invalid seat validation
│   ├── Hold ownership
│   └── Hold → Booking → Release
│
└── Kafka
    └── Duplicate BookingCreatedEvent → process once
```

GitHub Actions runs the same Maven verification on every push and pull
request to `main`:

```text
.github/workflows/ci.yml
├── Temurin JDK 21
├── ./mvnw -B clean verify
└── Surefire / Failsafe report upload
```

[![CI](https://github.com/iremdinc22/tickera/actions/workflows/ci.yml/badge.svg)](https://github.com/iremdinc22/tickera/actions/workflows/ci.yml)

---

## Engineering Documentation

Detailed technical notes and experiment results are available in the `docs/` directory:

- [Concurrency Control](docs/concurrency.md)
- [Idempotency](docs/idempotency.md)
- [Redis Seat Holds](docs/redis-seat-holds.md)
- [Kafka & Reliable Event Processing](docs/kafka-event-processing.md)
- [Observability](docs/observability.md)
- [Performance Testing](docs/performance-testing.md)
- [k6 Load Test Scripts](load-tests/README.md)
- [Changelog](CHANGELOG.md)
- [Release Notes v1.0.0](docs/releases/v1.0.0.md)

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

### API & Infrastructure

- REST
- Swagger / OpenAPI (springdoc)
- Docker
- Docker Compose
- GitHub Actions CI

### Observability

- Spring Boot Actuator
- Micrometer
- Prometheus
- Grafana
- Provisioned dashboards and alert rules

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
- Retry backoff with `nextRetryAt`
- `FAILED` outbox state with `lastError`
- Stale `PROCESSING` recovery after 2 minutes
- Manual outbox requeue endpoint
- Outbox metrics for published, retry, stale recovery, pending and failed
- Multi-instance outbox publishing
- Idempotent Kafka consumer
- `processed_events` duplicate detection
- Kafka integration testing with Testcontainers
- JUnit concurrency integration tests
- PostgreSQL Testcontainers tests
- Redis integration tests
- k6 load and capacity scripts with current API contract
- Prometheus / Grafana observability
- Provisioned Grafana dashboards and Prometheus alert rules
- HikariCP monitoring
- Tomcat thread monitoring
- PostgreSQL performance investigation
- GitHub Actions CI on Java 21

---

## API Usage Examples

Create an event and seats:

```bash
curl -X POST http://localhost:8080/events \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "Demo Event",
    "venue": "Local Venue",
    "eventDate": "2030-01-01T20:00:00",
    "seatNumbers": ["A1", "A2"]
  }'
```

List seats and their IDs:

```bash
curl http://localhost:8080/events/1/seats
```

Optional Redis hold:

```bash
curl -X POST \
  'http://localhost:8080/seats/100/hold?userId=demo-user'
```

Create a booking from a held seat:

```bash
curl -X POST http://localhost:8080/bookings/held \
  -H 'Content-Type: application/json' \
  -d '{"seatId": 100, "userId": "demo-user"}'
```

Create an idempotent booking:

```bash
curl -X POST http://localhost:8080/bookings \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-key-1' \
  -d '{"seatId": 101, "userId": "demo-user"}'
```

Repeating the same request with the same key returns the existing booking.
Reusing the key with a different seat returns `409 Conflict`.

Requeue a permanently failed outbox event:

```bash
curl -X POST \
  http://localhost:8080/admin/outbox/<event-uuid>/requeue
```

| Response | Meaning |
|---|---|
| `201 Created` | Booking created, or idempotent replay returned |
| `409 Conflict` | Seat unavailable or idempotency-key conflict |
| `400 Bad Request` | Missing/invalid `Idempotency-Key` or payload |
| `404 Not Found` | Event or seat does not exist |

---

## Known Limitations and Engineering Trade-offs

**Seat locking**

- Pessimistic locking is simpler under heavy contention but serializes
  access to the contended row.
- Optimistic locking scales better for independent rows but returns
  conflicts under hot-seat load.
- Tickera keeps both strategies for comparison; the exposed endpoints use
  the pessimistic flow.

**Redis seat holds**

- Holds are temporary reservation state, not permanent booking state.
- A Redis outage can prevent new holds; PostgreSQL remains the source of
  truth for committed bookings.
- Hold TTL is configuration, not a guarantee that a user will complete a
  booking.

**Idempotency**

- Idempotency records are stored indefinitely; no retention policy is
  implemented.
- The request fingerprint is SHA-256 over the request payload; a changed
  request with the same key is rejected instead of merged.

**Outbox and Kafka**

- Delivery is at-least-once, not exactly-once.
- `FAILED` events require manual requeue; there is no dead-letter topic.
- Published outbox rows are not cleaned up automatically.
- Outbox counters reset when the application restarts; gauges recover
  from the database.
- Consumer side effects must remain idempotent; the current consumer
  records processed event IDs.

**Operations**

- Prometheus alert rules use development thresholds and have no
  Alertmanager notification channel.
- The Compose stack is a local development environment, not a production
  deployment topology.
- Performance numbers are local, historical measurements and not
  production capacity claims.

---

## Project Evolution

All phases below are implemented and covered by the current test suite.

```text
Basic Booking Flow                     ✓
        │
        ▼
Reproduce Double Booking               ✓
        │
        ▼
Pessimistic Locking                    ✓
        │
        ▼
Load Testing                           ✓
        │
        ▼
Observability                          ✓
        │
        ▼
Capacity Investigation                 ✓
        │
        ▼
Optimistic Locking                     ✓
        │
        ▼
Compare Locking Strategies             ✓
        │
        ▼
Automated Concurrency Tests            ✓
        │
        ▼
Idempotency                            ✓
        │
        ▼
Concurrent Idempotency                 ✓
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
Outbox Retry / Backoff / Recovery      ✓
        │
        ▼
Prometheus Alerts & Grafana Dashboards ✓
        │
        ▼
GitHub Actions CI                      ✓
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