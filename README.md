# Tickera — High-Concurrency Ticket Booking System

Tickera is a backend engineering project exploring how a ticket booking system can remain correct and observable under concurrent load.

The project started with a simple question:

> What happens when many users try to book the same seat at the same time?

It then evolved into experiments around concurrency control, idempotency, temporary seat reservations, distributed coordination, load testing, database behavior, observability, and system bottlenecks.

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

Under concurrent requests, however, multiple transactions may observe the same seat as `AVAILABLE` before either commits.

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

The first transaction locks the seat row until commit.

Concurrent requests wait and later observe the seat as already booked.

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

The contention pattern matters.

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

Concurrency regressions can be detected automatically with:

```bash
./mvnw test
```

---

## Idempotent Booking

Concurrency control prevents competing transactions from successfully claiming the same seat.

It does not solve repeated delivery of the same logical request.

For example:

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

Behavior:

```text
same key + same request
        ↓
return existing booking


same key + different request
        ↓
409 Conflict
```

---

## Concurrent Idempotency

Sequential idempotency alone is not enough.

Multiple identical retries can arrive at the same time and initially observe that the idempotency key does not exist.

Tickera therefore uses explicit states:

```text
PROCESSING
COMPLETED
```

The database-level unique constraint on `idempotency_key` acts as an ownership mechanism.

```text
Request A                  Request B
    │                          │
    ▼                          ▼
INSERT PROCESSING        INSERT PROCESSING
    │                          │
    ✅                         ❌
    │                    unique conflict
    ▼                          │
  OWNER                      RETRY
    │                          │
Create booking                │
    │                          │
    ▼                          │
COMPLETED                     │
bookingId = 42                │
    │                          │
    └───────────┬──────────────┘
                ▼
           Booking #42
```

The automated test sends:

```text
20 concurrent requests
+
same Idempotency-Key
+
same request
```

and verifies:

```text
1 logical operation
1 booking
1 idempotency record
same booking ID returned
```

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

Redis is used for temporary reservation state.

PostgreSQL remains the source of truth for permanent booking state and final transaction correctness.

---

## Multi-Instance Coordination

Two independent Tickera instances were run against the same Redis and PostgreSQL infrastructure:

```text
                 Redis
                :6380
               /     \
              /       \
     Tickera A         Tickera B
       :8081             :8082
              \         /
               \       /
              PostgreSQL
                :5436
```

Concurrent hold requests were sent through different instances for the same seat.

Observed behavior:

```text
Instance A / user A
        │
        ├──── same seat ────┐
        │                   │
Instance B / user B         │
                            ▼
                           Redis
                            │
                            ▼
                       one winner
```

One request received `200 OK`.

The competing request received `409 Conflict`.

The winning hold was then successfully used to create the booking through the other Spring Boot instance.

This verified that hold state is shared distributed state rather than instance-local JVM state.

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

Representative behavior:

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

At approximately 2000 requested iterations per second, the system showed increased latency, connection waiting, and dropped iterations.

These are local development-machine measurements and should not be interpreted as production capacity.

---

## Bottleneck Experiments

Several possible bottlenecks were investigated independently.

### HikariCP

Connection pool:

```text
10 → 20
```

Increasing the pool size did not improve throughput and increased latency.

### PostgreSQL

`pg_stat_statements` showed that individual booking queries were very fast.

Representative mean execution times:

```text
INSERT booking                ~0.082 ms
UPDATE seat                   ~0.063 ms
SELECT seat FOR UPDATE        ~0.031 ms
```

Raw SQL execution time alone therefore did not explain end-to-end latency.

### WAL

PostgreSQL synchronous commits were temporarily disabled:

```text
synchronous_commit

ON → OFF
```

This reduced WAL synchronization work but did not materially improve application throughput.

### Tomcat

Worker threads were increased:

```text
200 → 400
```

Thread saturation disappeared, but throughput remained almost unchanged.

### k6 Virtual Users

Maximum VUs were increased:

```text
500 → 1000
```

Additional concurrency increased latency instead of throughput.

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

Integration testing uses real PostgreSQL and Redis containers.

The PostgreSQL integration-test infrastructure is shared through a common base configuration.

Redis-specific tests run against Redis Testcontainers.

Current correctness coverage includes:

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
```

---

## Engineering Documentation

Detailed technical notes and experiment results are available in the `docs/` directory:

- [Concurrency Control](docs/concurrency.md) — race conditions, pessimistic vs optimistic locking, and concurrency integration testing
- [Idempotency](docs/idempotency.md) — idempotency keys, request fingerprinting, concurrent retries, and request ownership
- [Redis Seat Holds](docs/redis-seat-holds.md) — temporary reservations, TTL, ownership, atomic release, and multi-instance coordination
- [Performance Testing](docs/performance-testing.md) — k6 workloads, capacity experiments, and bottleneck analysis
- [Observability](docs/observability.md) — Prometheus, Grafana, HikariCP, Tomcat, JVM, and application-level metrics

---

## Current Architecture

```text
                         Client / k6
                              │
                              ▼
                       Spring Boot API
                              │
                 ┌────────────┴────────────┐
                 ▼                         ▼
        SeatHoldController         BookingController
                 │                         │
                 ▼                         ▼
         SeatHoldService             BookingService
                 │                         │
                 ▼                 ┌───────┴────────┐
               Redis               ▼                ▼
          SET NX + TTL           Redis          PostgreSQL
                                  │                 │
                           Hold ownership          ├── Row locking
                                                   ├── @Version
                                                   └── Idempotency
```

---

## Tech Stack

### Backend

- Java 21
- Spring Boot
- Spring Data JPA
- Spring Data Redis
- Hibernate
- Maven

### Data & Coordination

- PostgreSQL 17
- Redis 7.4
- HikariCP
- `pg_stat_statements`

### Testing

- JUnit 5
- Testcontainers
- PostgreSQL Testcontainers
- Redis Testcontainers
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
- Redis integration tests with Testcontainers
- JUnit concurrency integration tests
- PostgreSQL Testcontainers tests
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
Kafka / Event-Driven Processing        ← NEXT
        │
        ▼
Reliable Event Publishing
        │
        ▼
Transactional Outbox
        │
        ▼
Failure Handling & Retry
        │
        ▼
Idempotent Consumers
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
- Shared Redis state can coordinate multiple independent application instances.
- Redis state should be validated against the PostgreSQL source of truth.
- Correctness invariants should be protected by repeatable automated tests.

---

## Next — Kafka & Event-Driven Processing

The next phase introduces asynchronous event processing after successful bookings.

```text
Booking committed
       │
       ▼
BookingCreated
       │
       ▼
     Kafka
       │
       ▼
    Consumer
```

The first implementation will introduce basic producer and consumer behavior.

The following phase will investigate the failure window between PostgreSQL commits and Kafka publishing.

That leads into reliable event delivery patterns such as the Transactional Outbox Pattern.

The goal is to explore:

- Event-driven communication
- Kafka producers and consumers
- Delivery guarantees
- Duplicate event handling
- Failure and retry behavior
- Reliable database-to-Kafka event publishing
- Idempotent event consumption

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