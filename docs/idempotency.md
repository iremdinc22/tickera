# Idempotency in Tickera

This document describes how Tickera handles duplicate booking requests and concurrent retries using idempotency keys.

---

## The Problem

Concurrency control protects seats from being booked multiple times.

However, another problem exists at the HTTP request level.

Consider the following scenario:

```text
Client
  │
  ├── POST /bookings ─────► Server
  │                           │
  │                           ▼
  │                      Booking created
  │                           │
  │       response lost       │
  │            ✕              │
  │
  └── retry ──────────────► Server
```

The client does not know whether the first request succeeded.

Retrying is therefore reasonable.

But the server must recognize that the second request represents the same logical operation.

This is the problem idempotency solves.

---

# Idempotency Key

Booking requests can include an idempotency key:

```http
POST /bookings
Idempotency-Key: test-key-001
Content-Type: application/json
```

Example request:

```json
{
  "seatId": 1003621,
  "userId": "irem"
}
```

The key identifies the logical booking operation.

---

# Idempotency Record

Tickera stores idempotency information in PostgreSQL.

Conceptually:

```text
id
idempotency_key
request_hash
status
booking_id
created_at
```

Example:

```text
id              : 1
idempotency_key : test-key-001
request_hash    : 4060ff56...
status          : COMPLETED
booking_id      : 791359
created_at      : 2026-08-20 ...
```

The `idempotency_key` has a database-level unique constraint.

---

# Why the Unique Constraint Matters

A naive implementation could perform:

```text
Does key exist?
      │
      ▼
     NO
      │
      ▼
Create operation
```

But two requests can execute concurrently:

```text
Request A                    Request B
    │                            │
    ▼                            ▼
Check key                  Check key
    │                            │
    ▼                            ▼
Not found                  Not found
    │                            │
    ▼                            ▼
INSERT                     INSERT
```

Application-level checking alone therefore does not guarantee ownership.

The database unique constraint guarantees that only one record can own a given idempotency key.

---

# Request Fingerprinting

An idempotency key should represent one specific request.

Consider:

```text
Idempotency-Key: abc123

seatId = 10
userId = irem
```

Later:

```text
Idempotency-Key: abc123

seatId = 45
userId = irem
```

These are not the same logical operation.

Tickera generates a SHA-256 fingerprint from the request.

Conceptually:

```text
seatId + userId
      │
      ▼
    SHA-256
      │
      ▼
 request_hash
```

The resulting behavior is:

```text
same key + same request
        │
        ▼
legitimate retry


same key + different request
        │
        ▼
409 Conflict
```

---

# Idempotency Lifecycle

An idempotency record uses two states:

```text
PROCESSING
COMPLETED
```

The lifecycle is:

```text
Request received
      │
      ▼
Claim idempotency key
      │
      ▼
PROCESSING
      │
      ▼
Create booking
      │
      ▼
Store booking ID
      │
      ▼
COMPLETED
```

A completed record connects one logical request to one result:

```text
Idempotency Key
      │
      ▼
Request Hash
      │
      ▼
Booking ID
```

---

# Concurrent Retries

Sequential retries are relatively straightforward.

The harder case occurs when multiple retries arrive simultaneously.

Example:

```text
20 requests
     +
same Idempotency-Key
     +
same request body
```

Without coordination:

```text
Request A                 Request B
    │                         │
    ▼                         ▼
Key missing               Key missing
    │                         │
    ▼                         ▼
Process                   Process
```

This creates another race condition.

---

# Ownership Model

Tickera uses the idempotency record as an ownership mechanism.

```text
Request A                    Request B
    │                            │
    ▼                            ▼
INSERT PROCESSING          INSERT PROCESSING
    │                            │
    ▼                            ▼
 SUCCESS                 UNIQUE CONFLICT
    │                            │
    ▼                            ▼
  OWNER                        RETRY
    │                            │
    ▼                            │
Create booking                  │
    │                            │
    ▼                            │
COMPLETED                       │
bookingId = 42                  │
    │                            │
    └────────────┬───────────────┘
                 ▼
             Booking #42
```

Only one request owns the operation.

Concurrent retries resolve to the same logical result.

---

# Locking vs Idempotency

These mechanisms solve different problems.

## Seat Locking

Protects a resource:

```text
Seat A1
   │
   ▼
Only one booking
can claim the seat
```

## Idempotency

Protects an operation:

```text
Logical Request X
       │
       ▼
Multiple HTTP retries
       │
       ▼
One logical result
```

Therefore:

> Preventing double booking does not automatically make an API idempotent.

Both mechanisms are required for different reasons.

---

# Automated Idempotency Test

Concurrent idempotency behavior is verified using JUnit and Testcontainers.

The test launches:

```text
20 concurrent requests
        │
        ├── same key
        └── same request
```

Expected invariant:

```text
20 retries
    │
    ▼
1 logical operation
    │
    ├── 1 Booking
    ├── 1 IdempotencyRecord
    └── same Booking ID
```

The test verifies:

- Exactly one booking exists.
- Exactly one idempotency record exists.
- Concurrent callers resolve to the same booking.
- The seat ends in `BOOKED`.
- Duplicate side effects are not created.

---

# Key Findings

The idempotency implementation demonstrated that:

- Client retries are a separate problem from database concurrency.
- Seat locking and request idempotency solve different correctness problems.
- `findByIdempotencyKey()` followed by `insert` is not concurrency-safe by itself.
- Database unique constraints can provide request ownership.
- Request fingerprints prevent accidental reuse of keys for different operations.
- Explicit `PROCESSING` and `COMPLETED` states make request lifecycle visible.
- Concurrent retries should converge on the same logical result.
- Idempotency behavior should be tested concurrently, not only sequentially.