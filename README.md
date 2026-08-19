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

Early high-throughput experiments showed measurable waiting during connection acquisition.

This raised an important question:

> Is the connection pool itself the bottleneck, or is pool saturation only a symptom of another bottleneck?

To investigate this properly, controlled sustained-load and capacity tests were introduced.

---

# SQL Logging During Benchmarks

Hibernate SQL console logging was disabled during later benchmark runs:

```yaml
spring:
  jpa:
    show-sql: false
```

With the pool restored to 10, an earlier high-throughput workload produced approximately:

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

This established an important baseline: the connection pool behaves normally under low sustained load.

---

# Capacity Testing

After establishing the low-load baseline, the workload was increased gradually to determine where the system begins to show saturation.

The capacity experiment used a `constant-arrival-rate` workload and fresh seat ranges for each run.

Representative results:

```text
Requested Rate      Observed Behavior
----------------------------------------------------
10 req/s            Stable
50 req/s            Stable
75 req/s            Stable
90 req/s            Stable
95 req/s            Stable
100 req/s           Stable on repeat run
150 req/s           Stable
200 req/s           Stable
250 req/s           Stable
400 req/s           Stable
800 req/s           Minor dropped iterations
1200 req/s          Increased latency and drops
1500 req/s          Further latency growth
2000 req/s          Clear saturation signals
```

The tests showed that the earlier failed 100 req/s run was not a reproducible system capacity boundary.

A repeated 100 req/s run completed successfully:

```text
Requests completed : 3000
Request rate       : ~100 req/s
Failures           : 0
Average latency    : 3.92 ms
p95 latency        : 8.37 ms
```

This reinforced another important performance-testing principle:

> A single anomalous benchmark run should not automatically be treated as a system limit. Results should be reproducible.

---

# 2000 req/s Capacity Observation

At 2000 requested iterations per second with the default HikariCP pool size of 10, the system produced:

```text
Requested rate       : 2000 req/s
Achieved throughput  : ~1981 req/s
Average latency      : 19.07 ms
p95 latency          : 93.11 ms
Maximum latency      : 757 ms
Dropped iterations   : 559
HTTP failures        : 0%
```

Although all executed HTTP requests succeeded, k6 could no longer maintain the requested arrival rate perfectly.

At peak load, Grafana showed:

```text
HikariCP max connections     : 10
HikariCP active connections  : 10
HikariCP idle connections    : 0
HikariCP pending connections : > 0
```

These metrics indicate that requests had begun waiting for database connections.

However, pool saturation alone does not prove that the pool is undersized.

This created the next hypothesis:

> Would increasing the HikariCP connection pool reduce waiting and improve throughput?

---

# HikariCP Pool Size Experiment

To test the hypothesis, the HikariCP maximum pool size was increased:

```text
10 → 20
```

The same 2000 req/s workload was repeated using a fresh set of seats.

Results:

```text
                         Pool 10          Pool 20
--------------------------------------------------
Requested rate           2000 req/s       2000 req/s
Achieved throughput      ~1981 req/s      ~1943 req/s
Average latency          19.07 ms         47.00 ms
p95 latency              93.11 ms         260.04 ms
Maximum latency          757 ms           1.39 s
Dropped iterations       559              1642
HTTP failures            0%               0%
```

During the pool-20 run, Grafana showed:

```text
HikariCP active connections  : 20 / 20
HikariCP idle connections    : 0 at peak
HikariCP pending connections : significant spike
```

k6 also reported:

```text
Insufficient VUs, reached 500 active VUs and cannot initialize more
```

The test generator therefore reached its configured `maxVUs` limit while attempting to maintain the requested arrival rate.

More importantly, increasing the database connection pool did not improve application performance.

Instead:

- Achieved throughput decreased
- Average latency increased
- p95 latency increased from `93.11 ms` to `260.04 ms`
- Maximum latency increased to `1.39 s`
- Dropped iterations increased from `559` to `1642`
- Connection-pool waiting remained visible

The simple hypothesis that the system only needed more database connections was therefore rejected.

> Connection-pool saturation can be a symptom of a deeper bottleneck. Increasing pool capacity does not automatically increase throughput.

The HikariCP pool size was restored to:

```text
maximum-pool-size: 10
```

This keeps the application on the original baseline configuration while the underlying bottleneck is investigated.

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
- Capacity testing
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
Prometheus & Grafana Observability
          │
          ▼
Capacity Testing
          │
          ▼
HikariCP Saturation Investigation
          │
          ▼
PostgreSQL Root-Cause Analysis              ← NEXT
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
- Under low sustained load, the HikariCP connection pool remains largely underutilized.
- Capacity testing must be repeated before treating a single anomalous result as a system limit.
- As the booking workload approaches system capacity, HikariCP saturation becomes visible through active, idle, and pending connection metrics.
- At 2000 requested req/s, the 10-connection pool reached full utilization and requests began waiting for database connections.
- Increasing HikariCP from 10 to 20 connections did not resolve the observed bottleneck.
- Under the same 2000 req/s workload, the larger pool increased latency and dropped iterations while reducing achieved throughput.
- Connection-pool saturation therefore appears to be a symptom rather than sufficient evidence that the pool itself is undersized.
- Prometheus and Grafana make it possible to correlate application traffic with HTTP latency, JVM utilization, and connection-pool behavior.
- Performance bottlenecks depend on workload characteristics.
- Performance changes should be validated with controlled experiments rather than assumptions.
- Further investigation is required at the PostgreSQL query and transaction level.

---

# Next Milestone — PostgreSQL Bottleneck Analysis

Capacity testing revealed that increasing the HikariCP connection pool does not resolve the performance degradation observed near system capacity.

The next investigation therefore moves below the connection-pool layer.

The goal is to determine where database time is actually being spent under high booking concurrency.

The investigation will focus on:

- PostgreSQL query execution
- Transaction duration
- Row-lock behavior
- Database wait events
- Concurrent `INSERT` / `UPDATE` behavior
- Connection utilization
- Query plans where relevant

Conceptually, the investigation moves one layer deeper:

```text
k6 Load
   │
   ▼
Spring Boot
   │
   ▼
HikariCP
   │
   ▼
PostgreSQL
   │
   ├── Query execution
   ├── Transactions
   ├── Row locks
   ├── Wait events
   └── Concurrent writes
```

The objective is to identify the underlying database bottleneck before introducing further architectural changes.

After establishing this baseline, optimistic concurrency will be implemented and compared against the current pessimistic-locking strategy.

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