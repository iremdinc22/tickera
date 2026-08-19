# Tickera — High-Concurrency Ticket Booking System

Tickera is a backend engineering project focused on one deceptively simple problem:

> How can a ticket booking system remain correct when many users try to reserve the same limited set of seats at the same time?

Building a basic booking API is straightforward.

Preventing the same seat from being sold twice under concurrent load — while keeping latency and throughput under control — is not.

Tickera explores how a simple booking service evolves as concurrency, traffic, and system complexity increase.

The project follows a problem-driven engineering approach:

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
Test under concurrency
        │
        ▼
Measure the result
        │
        ▼
Make the next architecture decision
```

Infrastructure is introduced only when a demonstrated engineering problem justifies it.

---

# The Core Problem

Consider the last available seat for a concert.

Two users attempt to book the same seat nearly simultaneously:

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

Both transactions may observe the same seat as `AVAILABLE` before either transaction commits.

---

# System Invariant

The first concurrency invariant defined for Tickera is:

> A seat must never be successfully booked more than once.

The first phase of the project therefore focused on:

- Reproducing a real double-booking race condition
- Verifying the failure directly in PostgreSQL
- Introducing a concurrency-control strategy
- Repeating the same experiment
- Validating that the invariant now holds

---

# Reproducing the Race Condition

Two booking requests were sent concurrently for the same seat.

```bash
curl -s -X POST http://localhost:8080/bookings \
  -H "Content-Type: application/json" \
  -d '{"seatId":4,"userId":"user-a"}' &

curl -s -X POST http://localhost:8080/bookings \
  -H "Content-Type: application/json" \
  -d '{"seatId":4,"userId":"user-b"}' &

wait
```

Before concurrency protection, both requests succeeded.

The database contained two bookings for the same seat:

```text
 id | seat_id | user_id | status
----+---------+---------+---------
  4 |       4 | user-b  | PENDING
  5 |       4 | user-a  | PENDING
```

```text
Same seat
   │
   ├──── user-a → booking created
   └──── user-b → booking created

❌ DOUBLE BOOKING
```

Sequential correctness was not enough.

---

# First Solution — Pessimistic Row Locking

The first concurrency strategy implemented in Tickera is database-level pessimistic locking.

```java
@Lock(LockModeType.PESSIMISTIC_WRITE)
@Query("SELECT s FROM Seat s WHERE s.id = :id")
Optional<Seat> findByIdForUpdate(@Param("id") Long id);
```

The booking transaction retrieves the seat using this locking query:

```java
Seat seat = seatRepository.findByIdForUpdate(request.seatId())
        .orElseThrow(() -> new RuntimeException("Seat not found"));
```

Conceptually, PostgreSQL executes behavior equivalent to:

```sql
SELECT *
FROM seats
WHERE id = ?
FOR UPDATE;
```

The selected row remains locked until the transaction commits or rolls back.

```text
Transaction A                  Transaction B

SELECT FOR UPDATE 🔒
      │
      │                        SELECT FOR UPDATE
      │                              │
      │                           WAITING
      ▼                              │
UPDATE BOOKED                       │
CREATE BOOKING                      │
COMMIT 🔓                            │
                                     ▼
                                READ BOOKED
                                     │
                                     ▼
                                409 Conflict
```

After introducing the lock, the same concurrent test produced:

```text
201 Created
409 Conflict
```

Only one booking was persisted.

---

# Concurrency Testing Note

To make the original race condition easier to reproduce, an artificial delay was temporarily introduced:

```java
Thread.sleep(3000);
```

The delay widened the race-condition window during development.

After the race condition had been reproduced and the locking strategy verified, the delay was removed.

The current booking flow contains no artificial delay.

---

# Load Testing with k6

Manual concurrency tests proved correctness for two competing requests, but the next step was to evaluate the system under heavier concurrency.

k6 is used to generate repeatable workloads and collect:

- Booking outcome metrics
- Latency
- Throughput
- Unexpected responses

Custom metrics distinguish expected business conflicts from actual failures:

```text
booking_created
booking_conflict
unexpected_responses
valid_booking_response
```

A `409 Conflict` is an expected business outcome when another request has already claimed the seat.

---

## 50-User Hot-Seat Test

50 virtual users attempted to book the same seat concurrently.

```text
50 concurrent requests
        │
        ▼
     Same Seat
        │
   ┌────┴─────┐
   ▼          ▼
1 × 201     49 × 409
Created     Conflict
```

Results:

```text
booking_created.............: 1
booking_conflict............: 49
unexpected_responses........: 0

Average response time.......: 80.36 ms
p95 response time...........: 91.62 ms
Throughput..................: ~441 req/s
```

A database query confirmed that exactly one booking existed for the contested seat.

The invariant remained valid:

> At most one booking may successfully claim a seat.

---

## 100-User Hot-Seat vs Parallel-Seat Test

The concurrency level was increased to 100 users.

Two workload patterns were compared.

### Hot Seat

All 100 users attempted to book the same seat.

```text
1 booking created
99 conflicts

Average latency : 377.67 ms
p95 latency     : 446.16 ms
Throughput      : ~208 req/s
```

### Parallel Seats

100 users attempted to book 100 different seats.

```text
100 bookings created
0 unexpected responses

Average latency : 375.30 ms
p95 latency     : 409.60 ms
Throughput      : ~231 req/s
```

Comparison:

```text
                   Hot Seat       Parallel Seats
------------------------------------------------
Average latency    377.67 ms      375.30 ms
p95 latency        446.16 ms      409.60 ms
Throughput         207.65 req/s   230.82 req/s
```

The parallel workload performed somewhat better, but the difference was smaller than expected.

This suggested that row-lock contention contributes to latency, but does not fully explain system behavior at higher concurrency.

---

# Runtime Bottleneck Investigation

Spring Boot Actuator and HikariCP metrics were introduced to investigate where time was being spent under load.

Relevant metrics include:

```text
hikaricp.connections.active
hikaricp.connections.idle
hikaricp.connections.pending
hikaricp.connections.max
hikaricp.connections.acquire
hikaricp.connections.usage
hikaricp.connections.timeout
```

The initial HikariCP pool size was:

```text
maximum connections = 10
```

A high-throughput sustained workload produced approximately:

```text
~41,250 requests
~4,119 requests/second
p95 latency: 57.01 ms
```

Connection metrics showed measurable waiting during connection acquisition.

This created a new hypothesis:

> Would increasing the database connection pool improve throughput?

---

# HikariCP Pool Size Experiment

The pool size was increased from:

```text
10 → 20
```

while keeping the workload equivalent.

Results:

```text
                         Pool 10        Pool 20
------------------------------------------------
Throughput               ~4119 req/s     ~3830 req/s
Average latency          24.15 ms        25.97 ms
p95 latency              57.01 ms        76.82 ms
Avg connection acquire   ~21.6 ms        ~18.65 ms
Timeouts                 0               0
```

Increasing the pool size reduced connection acquisition time slightly, but did not improve overall performance.

Throughput decreased and p95 latency increased during this run.

The conclusion is not that a pool size of 10 is universally optimal.

Instead, the experiment demonstrates something more important:

> Increasing the number of database connections does not automatically increase application throughput.

Performance decisions need to be measured rather than assumed.

---

# SQL Logging During Benchmarks

Hibernate SQL console logging was disabled during later benchmark runs:

```yaml
spring:
  jpa:
    show-sql: false
```

With the pool restored to 10, another high-throughput workload produced approximately:

```text
Throughput       : ~4198 req/s
Average latency  : 23.67 ms
p95 latency      : 60.16 ms
```

The difference from the previous pool-10 run was relatively small.

SQL logging therefore did not appear to be the dominant bottleneck in this local workload, but it remains disabled during performance testing to reduce benchmark noise.

---

# Prometheus & Grafana Observability

After the initial performance investigation, Prometheus and Grafana were introduced to make runtime behavior observable during load tests.

Spring Boot Actuator exposes application metrics through Micrometer, which are scraped by Prometheus and visualized through Grafana.

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

The Tickera observability dashboard currently tracks:

- HikariCP active connections
- HikariCP idle connections
- HikariCP pending connections
- HikariCP maximum connections
- HTTP request rate
- Booking API p95 latency
- JVM heap memory usage
- CPU usage

This makes it possible to correlate incoming traffic with application latency, JVM resource usage, and database connection-pool utilization.

---

## Controlled Sustained-Load Observation

A lower-rate workload was introduced specifically to observe the application over a longer period rather than maximize throughput.

The workload was configured at:

```text
10 requests/second
30 seconds
```

A representative run produced:

```text
Requests completed : 300
Request rate       : ~10 req/s
Failures           : 0
Average latency    : 15.83 ms
p95 latency        : 23.65 ms
Maximum latency    : 468.52 ms
```

All 300 booking requests completed successfully.

During the same workload, Grafana showed approximately:

```text
HikariCP max connections     : 10
HikariCP active connections  : 0–1
HikariCP idle connections    : 9–10
HikariCP pending connections : 0
```

The pool remained largely underutilized.

At this workload level, the application generally required no more than one database connection at a time and no requests were observed waiting for a connection.

Therefore:

> The HikariCP connection pool was not a bottleneck under the tested 10 req/s sustained booking workload.

This result does not contradict the earlier high-throughput HikariCP experiments.

The workloads answer different questions:

```text
High-throughput benchmark
        │
        └── How does the system behave near much heavier load?

Controlled sustained workload
        │
        └── What does normal runtime resource utilization look like
            at a fixed 10 req/s?
```

The experiments reinforce an important performance-engineering principle:

> Bottlenecks are workload-dependent and should be identified through measurement rather than assumption.

---

# Current Architecture

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
                             ▼
                    ┌─────────────────┐
                    │    HikariCP     │
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
                    │ SELECT ...      │
                    │ FOR UPDATE      │
                    └─────────────────┘
```

Observability runs alongside the application:

```text
Spring Boot Actuator
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

The core architecture remains intentionally simple.

Redis, Kafka, distributed coordination, and other infrastructure will be introduced only when a concrete system problem requires them.

---

# Current Features

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
- k6 load testing
- Hot-seat contention testing
- Parallel-seat testing
- Sustained load testing
- Spring Boot Actuator metrics
- Micrometer metrics
- HikariCP connection-pool monitoring
- Prometheus metrics scraping
- Grafana observability dashboard
- HTTP request-rate monitoring
- Booking API p95 latency monitoring
- JVM memory monitoring
- CPU monitoring
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
- HikariCP

## API

- REST
- OpenAPI / Swagger

## Performance & Observability

- k6
- Spring Boot Actuator
- Micrometer
- Prometheus
- Grafana

## Planned

- Testcontainers
- Redis
- Kafka
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
k6 Concurrent Load Testing
          │
          ▼
Hot-Seat Testing
          │
          ▼
Parallel Multi-Seat Testing
          │
          ▼
Sustained Load Testing
          │
          ▼
Runtime Metrics
          │
          ▼
HikariCP Investigation
          │
          ▼
Runtime / Database Bottleneck Analysis
          │
          ▼
Prometheus & Grafana Observability
          │
          ▼
Optimistic Concurrency                 ← NEXT
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
Integration & Concurrency Testing
          │
          ▼
Performance & Scalability Testing
```

---

# Current Findings

The experiments performed so far demonstrate several important properties of the system:

- Sequential correctness does not guarantee concurrent correctness.
- PostgreSQL pessimistic locking prevents the reproduced double-booking race condition.
- Expected booking conflicts must be distinguished from actual server failures.
- Hot-seat contention affects latency and throughput, but row-lock contention is not the only performance factor.
- Database connection acquisition can introduce measurable waiting under sufficiently heavy concurrency.
- Increasing HikariCP from 10 to 20 connections did not improve the tested high-throughput workload.
- Increasing connection-pool size does not automatically increase application throughput.
- Under a controlled 10 req/s sustained workload, HikariCP remained largely underutilized with no pending connection requests.
- The 10-connection pool was therefore not a bottleneck at that workload level.
- Prometheus and Grafana make it possible to correlate application traffic with HTTP latency, JVM utilization, and connection-pool behavior.
- Performance bottlenecks depend on workload characteristics.
- Performance changes should be validated with controlled experiments rather than assumptions.

---

# Next Milestone — Optimistic Concurrency

The current pessimistic-locking implementation provides a correctness baseline.

The next step is to implement optimistic concurrency and compare both strategies under equivalent workloads.

The comparison will focus on:

```text
                    Pessimistic       Optimistic
                    Locking           Concurrency
                         │                 │
                         ├──────┬──────────┤
                                │
                                ▼
                         Correctness
                                │
                                ▼
                            Latency
                                │
                                ▼
                           Throughput
                                │
                                ▼
                       Conflict Behavior
```

The goal is not simply to determine which strategy is "faster."

Instead, the experiment should identify the trade-offs between:

- Lock waiting
- Conflict frequency
- Retry behavior
- Latency
- Throughput
- Implementation complexity

under equivalent concurrency patterns.

---

# Project Philosophy

Tickera is intentionally not developed by adding technologies simply because they are commonly associated with distributed systems.

Each architectural decision should answer a concrete engineering problem.

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