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

The first concurrency strategy implemented in Tickera was database-level pessimistic locking.

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

Prometheus and Grafana were introduced to make runtime behavior observable during load tests.

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

The Tickera observability dashboard tracks:

- HikariCP active connections
- HikariCP idle connections
- HikariCP pending connections
- HikariCP maximum connections
- HTTP request rate
- Booking API p95 latency
- JVM heap memory usage
- CPU usage
- Tomcat busy threads
- Tomcat current threads
- Tomcat maximum threads
- Booking transaction duration
- Seat-lock duration
- Booking persistence duration

This makes it possible to correlate incoming traffic with application latency, JVM utilization, HTTP worker threads, transaction timing, and database connection-pool behavior.

---

# Controlled Sustained-Load Observation

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

During the same workload, Grafana showed approximately:

```text
HikariCP max connections     : 10
HikariCP active connections  : 0–1
HikariCP idle connections    : 9–10
HikariCP pending connections : 0
```

The pool remained largely underutilized.

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

The tests showed that an earlier failed 100 req/s run was not a reproducible system capacity boundary.

A repeated 100 req/s run completed successfully:

```text
Requests completed : 3000
Request rate       : ~100 req/s
Failures           : 0
Average latency    : 3.92 ms
p95 latency        : 8.37 ms
```

This reinforced an important performance-testing principle:

> A single anomalous benchmark run should not automatically be treated as a system limit. Results should be reproducible.

---

# 2000 req/s Capacity Observation

At 2000 requested iterations per second with the default HikariCP pool size of 10, a representative run produced:

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

These metrics indicated that requests had begun waiting for database connections.

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

Increasing the database connection pool did not improve application performance.

Instead:

- Achieved throughput decreased
- Average latency increased
- p95 latency increased
- Dropped iterations increased
- Connection-pool waiting remained visible

The simple hypothesis that the system only needed more database connections was therefore rejected.

> Connection-pool saturation can be a symptom of a deeper bottleneck. Increasing pool capacity does not automatically increase throughput.

The HikariCP pool size was restored to:

```text
maximum-pool-size: 10
```

---

# Deeper Bottleneck Investigation

Because increasing the connection pool did not resolve the degradation near system capacity, the investigation moved deeper into PostgreSQL, transaction timing, the HTTP server, and the load generator itself.

## PostgreSQL Query Analysis

`pg_stat_statements` was enabled to inspect database execution under the booking workload.

A representative high-load run showed:

```text
Query                                  Calls      Mean Execution
----------------------------------------------------------------
INSERT booking                         ~59k       ~0.082 ms
UPDATE seat                            ~59k       ~0.063 ms
SELECT seat FOR NO KEY UPDATE          ~59k       ~0.031 ms
```

Individual SQL statements were therefore very fast.

This suggested that raw SQL execution time alone was not sufficient to explain the much larger end-to-end HTTP latency observed near saturation.

---

## WAL and Synchronous Commit Experiment

Because every successful booking modifies persistent state, PostgreSQL Write-Ahead Logging (WAL) was investigated.

WAL I/O timing was enabled and WAL statistics were measured before and after a high-load run.

The experiment showed measurable WAL synchronization activity.

To test whether synchronous WAL commits were a dominant source of latency, the following setting was temporarily changed:

```text
synchronous_commit

ON → OFF
```

The same workload was then repeated.

Disabling synchronous commit reduced WAL synchronization cost, but did not produce a corresponding improvement in overall application throughput or p95 latency.

The setting was restored after the experiment.

Therefore:

> Synchronous WAL flushes contribute to database work, but they were not identified as the primary cause of the observed capacity degradation.

---

## Application-Level Timing

Custom Micrometer timers were added around important parts of the booking flow:

```text
booking_seat_lock_duration
booking_save_duration
booking_flush_duration
booking_transaction_duration
```

These metrics make it possible to compare time spent inside the booking transaction with end-to-end HTTP latency.

Together with `pg_stat_statements`, the measurements reinforced an important observation:

> Large end-to-end latency under saturation cannot be explained by individual SQL execution time alone.

---

# Tomcat Thread-Pool Investigation

Tomcat worker-thread metrics were exposed through Spring Boot Actuator and monitored in Grafana.

During one 2000 req/s run with the default configuration:

```text
Tomcat busy threads    : 200
Tomcat current threads : 200
Tomcat max threads     : 200
```

This initially suggested that the Tomcat worker-thread limit might be restricting throughput.

To test the hypothesis, the maximum thread count was temporarily increased:

```text
200 → 400
```

The same workload was repeated.

```text
                         200 threads      400 threads
------------------------------------------------------
Achieved throughput      ~1917 req/s       ~1918 req/s
Average latency          70.25 ms          67.06 ms
p95 latency              332.83 ms         293.94 ms
Dropped iterations       2440              1864
HTTP failures            0%                0%
```

With the larger pool, Tomcat thread saturation disappeared during the observed run, but throughput remained almost unchanged.

Therefore:

> The default 200-thread Tomcat limit was not identified as the primary throughput bottleneck.

The experimental thread configuration was reverted after the test.

---

# k6 VU-Limit Investigation

During several capacity tests, k6 reported:

```text
Insufficient VUs, reached 500 active VUs and cannot initialize more
```

This raised another hypothesis:

> Is the load generator's 500-VU limit preventing k6 from sustaining 2000 req/s?

To test this, k6 was temporarily changed from:

```text
preAllocatedVUs: 100
maxVUs: 500
```

to:

```text
preAllocatedVUs: 500
maxVUs: 1000
```

The 2000 req/s workload was repeated.

Results:

```text
                         maxVUs 500       maxVUs 1000
------------------------------------------------------
Achieved throughput      ~1917 req/s       ~1710 req/s
Average latency          ~70 ms            ~475 ms
p95 latency              ~333 ms           ~917 ms
Dropped iterations       2440              8016
HTTP failures            0%                0%
```

k6 eventually reached the new 1000-VU limit as well.

Providing additional concurrency therefore did not increase achieved throughput.

Instead:

```text
More concurrency
       │
       ▼
Longer request duration
       │
       ▼
More requests in flight
       │
       ▼
Higher latency
       │
       ▼
No additional throughput
```

The original 500-VU limit was therefore not identified as the underlying cause of the observed capacity boundary.

The result is consistent with system saturation: adding more concurrency after a certain point increases waiting rather than useful throughput.

---

# Capacity Investigation Conclusion

The performance investigation tested several plausible bottlenecks independently:

```text
HikariCP connections
10 → 20
      │
      └── No throughput improvement

PostgreSQL synchronous commit
ON → OFF
      │
      └── No material application-level improvement

Tomcat worker threads
200 → 400
      │
      └── Saturation disappeared,
          throughput remained similar

k6 maxVUs
500 → 1000
      │
      └── Throughput decreased,
          latency increased
```

No single configuration parameter tested so far fully explains the observed capacity boundary.

Instead, the experiments demonstrate that near 2000 req/s the local system enters a saturation regime where additional concurrency increases latency and waiting without increasing useful throughput.

These numbers should not be interpreted as production capacity measurements.

The complete benchmark environment runs locally and includes:

```text
k6
Spring Boot JVM
PostgreSQL
Docker
Prometheus
Grafana
```

All components therefore compete for resources on the same development machine.

The purpose of these experiments is not to claim a production throughput number, but to practice controlled performance investigation and understand how bottlenecks appear across application layers.

---

# Pessimistic vs Optimistic Locking

After establishing pessimistic locking as the baseline, optimistic locking was implemented using a version column.

The two strategies were then evaluated under equivalent 100-user workloads.

Two contention patterns were tested:

- **Hot seat:** 100 users compete for the same seat.
- **Parallel seats:** 100 users concurrently book 100 different seats.

Results:

| Workload | Strategy | Result | Avg Latency | p95 Latency | Throughput |
|---|---|---:|---:|---:|---:|
| Hot seat | Pessimistic | 1 created / 99 conflicts | 224.24 ms | 245.34 ms | ~359.76 req/s |
| Hot seat | Optimistic | 1 created / 99 conflicts | 317.55 ms | 324.06 ms | ~292.18 req/s |
| Parallel seats | Pessimistic | 100 created / 0 conflicts | 135.07 ms | 180.91 ms | ~467.76 req/s |
| Parallel seats | Optimistic | 100 created / 0 conflicts | 135.43 ms | 155.89 ms | ~531.19 req/s |

Both strategies preserved the core invariant:

> A seat was never successfully booked more than once.

Under extreme hot-seat contention, pessimistic locking performed better in the observed local run.

With independent parallel-seat bookings, average latency was almost identical, while optimistic locking showed better p95 latency and throughput in the observed run.

The experiment therefore does not establish one strategy as universally superior.

Instead, it demonstrates that concurrency-control decisions should be evaluated against the expected contention pattern:

```text
High contention
      │
      ▼
Pessimistic locking may avoid
wasted conflicting work

Low contention
      │
      ▼
Optimistic locking can avoid
unnecessary lock coordination
```

These measurements are experimental observations from a local environment and should not be interpreted as universal performance characteristics.

---

# Current Architecture

```text
                    ┌─────────────────┐
                    │     Client      │
                    │      / k6       │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │     Tomcat      │
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
                    │ Concurrency     │
                    │    Control      │
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
- Optimistic locking with version-based conflict detection
- Pessimistic vs optimistic concurrency benchmarking
- Protection against concurrent double booking
- Swagger / OpenAPI documentation
- Dockerized PostgreSQL
- k6 load testing
- Hot-seat contention testing
- Parallel-seat testing
- Sustained load testing
- Capacity testing
- Spring Boot Actuator metrics
- Micrometer custom timers
- HikariCP connection-pool monitoring
- Prometheus metrics scraping
- Grafana observability dashboard
- HTTP request-rate monitoring
- Booking API p95 latency monitoring
- Tomcat thread monitoring
- Booking transaction timing
- JVM memory monitoring
- CPU monitoring
- PostgreSQL `pg_stat_statements`
- PostgreSQL WAL investigation
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
- pg_stat_statements

## Infrastructure

- Docker
- Docker Compose
- HikariCP
- Tomcat

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
Prometheus & Grafana Observability
          │
          ▼
Capacity Testing
          │
          ▼
HikariCP Saturation Investigation
          │
          ▼
PostgreSQL Query & WAL Analysis
          │
          ▼
Application Timing Instrumentation
          │
          ▼
Tomcat Thread-Pool Investigation
          │
          ▼
Load-Generator Limit Investigation
          │
          ▼
Optimistic Concurrency
          │
          ▼
Compare Concurrency Strategies
          │
          ▼
Integration & Concurrency Testing       ← NEXT
          │
          ▼
Idempotency
          │
          ▼
Redis / Distributed Coordination
          │
          ▼
Multi-Instance Deployment
          │
          ▼
Kafka & Event-Driven Processing
          │
          ▼
Reliable Event Publishing
          │
          ▼
Failure Handling & Retry
          │
          ▼
Extended Observability
          │
          ▼
CI/CD & Testcontainers
          │
          ▼
Final Performance Comparison
```

---

# Current Findings

The experiments performed so far demonstrate several important properties of the system:

- Sequential correctness does not guarantee concurrent correctness.
- PostgreSQL pessimistic locking prevents the reproduced double-booking race condition.
- Version-based optimistic locking also preserves the single-booking invariant under concurrent access.
- Both pessimistic and optimistic strategies prevented double booking in the tested hot-seat workload.
- Expected booking conflicts must be distinguished from actual server failures.
- Under extreme hot-seat contention, pessimistic locking performed better in the observed local comparison.
- Under parallel independent-seat workload, optimistic locking showed better p95 latency and throughput in the observed run.
- No concurrency-control strategy should be considered universally superior based on a single workload.
- The expected contention pattern is an important factor when choosing a concurrency-control strategy.
- Hot-seat contention affects latency and throughput, but row-lock contention is not the only performance factor.
- Under low sustained load, the HikariCP connection pool remains largely underutilized.
- As the workload approaches local system capacity, connection waiting, latency, and dropped iterations become visible.
- Increasing HikariCP from 10 to 20 connections did not improve throughput and increased latency in the tested workload.
- `pg_stat_statements` showed that individual booking SQL statements execute very quickly, so raw query execution time alone does not explain end-to-end latency.
- Disabling synchronous commit reduced WAL synchronization cost but did not materially improve application-level performance.
- Increasing Tomcat's worker-thread limit from 200 to 400 removed the observed thread-pool saturation without materially increasing throughput.
- Increasing k6 `maxVUs` from 500 to 1000 did not increase achieved throughput. Instead, latency and dropped iterations increased significantly.
- The observed local capacity boundary is therefore not explained by a single connection-pool, Tomcat-thread, WAL, or k6 VU configuration limit.
- Additional concurrency can reduce performance once the system enters saturation.
- Performance bottlenecks emerge from interactions between multiple layers and should be investigated through controlled experiments rather than assumptions.
- Local benchmark results should not be interpreted as production capacity because the load generator, application, database, observability stack, and Docker runtime share the same machine.

---

# Next Milestone — Automated Concurrency Testing

The next milestone is to convert the manually verified concurrency invariant into repeatable integration tests using JUnit and Testcontainers.

The tests will verify that concurrent booking attempts cannot create more than one successful booking for the same seat.

Conceptually:

```text
JUnit
  │
  ▼
Start PostgreSQL with Testcontainers
  │
  ▼
Create fresh seat
  │
  ▼
Launch concurrent booking attempts
  │
  ▼
Wait for all requests
  │
  ▼
Verify invariant
  │
  ├── Exactly one successful booking
  │
  └── No duplicate booking
```

This will provide a regression safety net before introducing distributed coordination, idempotency, multi-instance deployment, and event-driven processing.

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