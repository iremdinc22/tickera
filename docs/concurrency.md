# Concurrency Control in Tickera

This document describes the concurrency problem investigated in Tickera and the strategies implemented to prevent double booking.

---

## The Problem

The main correctness invariant of Tickera is:

> A seat must never be successfully booked more than once.

A naive booking implementation may appear correct when requests are processed sequentially:

```text
Request
   │
   ▼
Read Seat
   │
   ▼
AVAILABLE?
   │
   ▼
Set BOOKED
   │
   ▼
Create Booking
```

The problem appears when multiple transactions execute concurrently.

```text
Transaction A                 Transaction B
      │                             │
      ▼                             ▼
READ AVAILABLE                READ AVAILABLE
      │                             │
      ▼                             ▼
UPDATE BOOKED                 UPDATE BOOKED
      │                             │
      ▼                             ▼
CREATE BOOKING                CREATE BOOKING
```

Both transactions can observe the seat as available before either transaction commits.

The result is a classic race condition:

```text
Seat A1
  │
  ├── Booking A
  │
  └── Booking B

DOUBLE BOOKING
```

---

## Reproducing the Race Condition

The race condition was reproduced by sending concurrent booking requests for the same seat.

Example:

```bash
curl -s -X POST http://localhost:8080/bookings \
  -H "Content-Type: application/json" \
  -d '{"seatId":4,"userId":"user-a"}' &

curl -s -X POST http://localhost:8080/bookings \
  -H "Content-Type: application/json" \
  -d '{"seatId":4,"userId":"user-b"}' &

wait
```

Without concurrency protection, both requests could create bookings.

This demonstrated an important property:

> Sequential correctness does not imply concurrent correctness.

---

# Pessimistic Locking

The first solution implemented was database-level pessimistic locking.

The repository retrieves the seat using:

```java
@Lock(LockModeType.PESSIMISTIC_WRITE)
@Query("SELECT s FROM Seat s WHERE s.id = :id")
Optional<Seat> findByIdForUpdate(@Param("id") Long id);
```

Conceptually, PostgreSQL performs:

```sql
SELECT *
FROM seats
WHERE id = ?
FOR UPDATE;
```

The selected seat row is locked until the transaction commits or rolls back.

---

## Behavior

Consider two concurrent transactions:

```text
Transaction A                 Transaction B
      │                             │
      ▼                             ▼
SELECT FOR UPDATE             SELECT FOR UPDATE
      │                             │
      ▼                             ▼
LOCK ACQUIRED                   WAIT
      │                             │
      ▼                             │
Set BOOKED                       │
      │                             │
      ▼                             │
Create Booking                  │
      │                             │
      ▼                             │
COMMIT                          │
      │                             ▼
      │                       LOCK ACQUIRED
      │                             │
      │                             ▼
      │                       Seat = BOOKED
      │                             │
      │                             ▼
      │                       409 Conflict
```

Only one transaction successfully creates a booking.

---

# Optimistic Locking

Tickera also implements optimistic concurrency control.

The `Seat` entity contains:

```java
@Version
@Column(nullable = false)
private Long version;
```

Hibernate includes this version when updating the entity.

Conceptually:

```sql
UPDATE seats
SET status = 'BOOKED',
    version = version + 1
WHERE id = ?
AND version = ?;
```

If another transaction has already modified the row, the expected version no longer matches.

Hibernate then detects the conflict.

---

## Pessimistic vs Optimistic Locking

The two strategies solve the same correctness problem differently.

### Pessimistic

```text
Conflict expected
      │
      ▼
Prevent concurrent modification
      │
      ▼
Lock first
```

### Optimistic

```text
Conflict assumed uncommon
      │
      ▼
Allow concurrent work
      │
      ▼
Detect conflict during update
```

---

## Experimental Results

Both strategies were tested with two workload patterns.

### Hot Seat

100 users compete for the same seat.

| Strategy | Result | Avg Latency | p95 | Throughput |
|---|---:|---:|---:|---:|
| Pessimistic | 1 created / 99 conflicts | 224.24 ms | 245.34 ms | ~359.76 req/s |
| Optimistic | 1 created / 99 conflicts | 317.55 ms | 324.06 ms | ~292.18 req/s |

In this experiment, pessimistic locking performed better under extreme contention.

### Parallel Seats

100 users book 100 independent seats.

| Strategy | Result | Avg Latency | p95 | Throughput |
|---|---:|---:|---:|---:|
| Pessimistic | 100 created | 135.07 ms | 180.91 ms | ~467.76 req/s |
| Optimistic | 100 created | 135.43 ms | 155.89 ms | ~531.19 req/s |

Optimistic locking performed better in p95 latency and throughput for independent seats.

These local measurements do not establish a universally superior strategy.

The contention pattern matters.

---

# Automated Concurrency Testing

The concurrency invariant is protected by automated integration tests.

Technologies:

```text
JUnit 5
   +
Testcontainers
   +
PostgreSQL 17
```

The test creates a real PostgreSQL container and launches 20 concurrent operations against the same seat.

```text
20 threads
     │
     ▼
Same Seat
     │
     ▼
BookingService
     │
     ▼
PostgreSQL
```

Expected result:

```text
1 successful booking
19 rejected attempts
1 booking in database
seat = BOOKED
```

Separate tests verify:

- Pessimistic locking
- Optimistic locking

This means a future architectural change that accidentally reintroduces double booking can be detected automatically.

---

# Test Infrastructure

Concurrency integration tests share a PostgreSQL Testcontainer through a common test configuration.

```text
PostgresIntegrationTest
          │
          ▼
   PostgreSQL 17
    Testcontainer
          │
     ┌────┴────┐
     ▼         ▼
 Booking    Idempotency
  Tests       Tests
```

This avoids starting an independent PostgreSQL container for every test class.

A representative complete test run:

```text
Tests run: 4
Failures: 0
Errors: 0
Skipped: 0

BUILD SUCCESS

Total time: 9.370 s
```

---

# Key Findings

The concurrency experiments demonstrated that:

- Sequential correctness is not sufficient for concurrent systems.
- Database transactions alone do not automatically prevent logical race conditions.
- Pessimistic locking prevents competing transactions from modifying the same seat simultaneously.
- Optimistic locking detects conflicting updates using entity versions.
- Both strategies preserve Tickera's single-booking invariant.
- Pessimistic locking performed better in the observed high-contention workload.
- Optimistic locking performed better in the observed independent-seat workload.
- Concurrency strategies should be selected based on workload characteristics rather than assumed to be universally superior.
- Correctness invariants should be protected by repeatable automated tests.