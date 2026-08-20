# Redis Seat Holds

This document describes how Tickera uses Redis for temporary seat reservations, expiration, ownership enforcement, and coordination across multiple application instances.

---

## Problem

Database locking protects the final booking transaction.

However, a real ticketing flow may need to temporarily reserve a seat while a user completes the checkout process.

Without temporary reservation:

```text
User selects seat
      │
      ▼
Immediately BOOKED
```

A more realistic lifecycle is:

```text
AVAILABLE
    │
    ▼
HELD
    │
    ├── timeout ──► AVAILABLE
    │
    └── booking ─► BOOKED
```

The `HELD` state is temporary.

It should not require a long-running PostgreSQL transaction or database lock.

Tickera uses Redis for this short-lived reservation state.

---

## Why Redis?

Seat holds have several characteristics:

- They are temporary.
- They require expiration.
- They must be shared across application instances.
- Multiple users may compete for the same hold.
- Ownership acquisition must be atomic.
- Stale reservations should disappear automatically.

Redis provides useful primitives for this problem:

```text
SET NX
TTL
atomic commands
Lua scripts
shared distributed state
```

PostgreSQL remains the source of truth for permanent booking state.

---

## Hold Representation

A hold is stored as a Redis key:

```text
key   = seat:hold:{seatId}
value = userId
TTL   = configured hold duration
```

Example:

```text
seat:hold:1003619 = ahmet
```

The default hold duration is configured in `application.yml`:

```yaml
tickera:
  seat-hold:
    duration: 2m
```

This allows production and test configurations to use different durations.

For example, integration tests can override the duration:

```text
tickera.seat-hold.duration=1s
```

without waiting two minutes for every TTL test.

---

## Atomic Hold Acquisition

A seat hold must not use a naive flow such as:

```text
Does hold exist?
      │
      ▼
     NO
      │
      ▼
Create hold
```

Two concurrent requests could both observe that the hold does not yet exist.

Tickera instead uses Redis `SET NX`.

Through Spring Data Redis:

```java
redisTemplate
        .opsForValue()
        .setIfAbsent(
                key,
                userId,
                holdDuration
        );
```

Conceptually:

```text
SET seat:hold:1003619 ahmet NX EX 120
```

`NX` means:

> Create the key only if it does not already exist.

This operation is atomic.

---

## Concurrent Hold Example

Consider 20 users attempting to hold the same seat:

```text
User 1 ────┐
User 2 ────┤
User 3 ────┤
...        ├────► Redis SET NX
User 20 ───┘
                 │
                 ▼
             one winner
```

Expected invariant:

```text
20 concurrent hold attempts
          │
          ▼
1 successful hold
19 rejected attempts
```

This behavior is protected by an automated integration test.

---

## TTL Expiration

Every hold has a TTL.

Example:

```text
seat:hold:1003619 = irem
TTL = 120 seconds
```

Redis automatically reduces the TTL over time:

```text
120
119
118
...
3
2
1
0
```

When the TTL expires, the key disappears automatically.

```text
HELD
 │
 │ TTL expires
 ▼
AVAILABLE FOR HOLD AGAIN
```

No periodic cleanup job is required for normal hold expiration.

---

## Why PostgreSQL Locks Are Not Used for Holds

A pessimistic database lock is appropriate for a short transaction:

```text
BEGIN
  │
  ▼
SELECT FOR UPDATE
  │
  ▼
Create booking
  │
  ▼
COMMIT
```

It is not appropriate to keep a transaction open while a human completes checkout.

For example:

```text
BEGIN
  │
  ▼
SELECT FOR UPDATE
  │
  ▼
wait 2 minutes for user
  │
  ▼
COMMIT
```

would hold a database connection and row lock for far too long.

Tickera therefore separates responsibilities:

```text
Redis
→ user-facing temporary reservation
→ seconds / minutes

PostgreSQL
→ final booking transaction
→ milliseconds
```

---

## Hold Ownership

A user must not be able to book a seat temporarily held by another user.

Example:

```text
Redis

seat:hold:123 = irem
```

Then:

```text
Booking request
userId = ahmet
```

must be rejected.

The booking flow verifies:

```text
Redis owner
    ==
request user?
```

Behavior:

```text
owner = irem
request = irem
     │
     ▼
continue


owner = irem
request = ahmet
     │
     ▼
reject
```

---

## Owner-Aware Release

Only the hold owner should be able to release the hold.

The following implementation would be unsafe:

```text
GET key
  │
  ▼
compare value
  │
  ▼
DELETE key
```

because another operation could modify the key between `GET` and `DELETE`.

Tickera performs compare-and-delete atomically using a Redis Lua script:

```lua
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
else
    return 0
end
```

Behavior:

```text
seat:hold:10 = irem

irem releases
→ deleted


ahmet releases
→ rejected
→ key remains
```

---

## Hold-to-Booking Lifecycle

The final Redis-aware booking flow is:

```text
User selects seat
       │
       ▼
Acquire Redis hold
       │
       ▼
Verify hold ownership
       │
       ▼
PostgreSQL SELECT FOR UPDATE
       │
       ▼
Verify seat is AVAILABLE
       │
       ▼
Create Booking
       │
       ▼
Seat → BOOKED
       │
       ▼
Release Redis hold
```

Redis handles temporary coordination.

PostgreSQL still protects final persistent correctness.

---

## Redis Does Not Replace PostgreSQL Locking

Even if Redis guarantees that one user currently owns a temporary hold, the final booking transaction still uses PostgreSQL locking.

This provides defense at the source of truth.

```text
Redis
  │
  └── temporary ownership

PostgreSQL
  │
  └── final booking correctness
```

A Redis hold is not treated as proof that the persistent seat state cannot have changed.

The database is checked again during booking.

---

## Database Validation Before Hold Creation

An important issue was discovered during multi-instance testing.

A hold could initially be created for a seat ID that did not exist in PostgreSQL.

For example:

```text
Redis:
seat:hold:1003622 = irem
```

while:

```sql
SELECT *
FROM seats
WHERE id = 1003622;
```

returned no rows.

This created temporary Redis state for a resource that did not exist in the source-of-truth database.

The hold flow was therefore changed to validate the seat before writing Redis state.

Current flow:

```text
Hold request
     │
     ▼
Seat exists in PostgreSQL?
     │
 ┌───┴────┐
 │        │
NO       YES
 │        │
 ▼        ▼
reject   status AVAILABLE?
              │
          ┌───┴────┐
          │        │
         NO       YES
          │        │
          ▼        ▼
       reject   Redis SET NX
```

As a result:

```text
non-existing seat
→ no Redis hold

BOOKED seat
→ no Redis hold

AVAILABLE seat
→ hold may be acquired
```

---

## Multi-Instance Coordination

Redis becomes especially useful when multiple application instances are running.

The following setup was tested:

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

Both application instances shared:

```text
Redis      → localhost:6380
PostgreSQL → localhost:5436
```

---

## Multi-Instance Hold Test

Two hold requests were submitted concurrently through different instances:

```text
Tickera :8081
user = irem
       │
       ├──── same seat
       │
Tickera :8082
user = ahmet
```

Observed result:

```text
one request
→ 200 OK

other request
→ 409 Conflict
```

Redis contained only one owner:

```text
seat:hold:{seatId} = ahmet
```

This demonstrated that the hold was not tied to a local JVM.

Both Spring Boot processes were coordinating through the same Redis instance.

---

## Cross-Instance Booking Test

The winning hold was acquired through one application instance.

Example:

```text
Tickera :8082
        │
        ▼
hold acquired by ahmet
```

The booking was then submitted through the other instance:

```text
Tickera :8081
        │
        ▼
POST /bookings/held
userId = ahmet
```

The request returned:

```text
201 Created
```

PostgreSQL confirmed:

```text
seat status = BOOKED
booking count = 1
user = ahmet
```

The Redis hold was released after the successful booking.

This verified:

```text
Instance A creates hold
        │
        ▼
Redis shared state
        │
        ▼
Instance B reads hold
        │
        ▼
Booking succeeds
```

---

## Automated Redis Tests

Redis behavior is protected using Testcontainers.

### `SeatHoldIntegrationTest`

Verifies:

- Only one concurrent user acquires the same seat hold.
- Other concurrent attempts fail.
- Hold TTL expires automatically.
- The seat can be held again after expiration.
- Non-existing seats cannot create Redis state.
- Already booked seats cannot create Redis state.

### `HeldBookingIntegrationTest`

Verifies:

- A user with a valid hold can complete the booking.
- Another user cannot use someone else's hold.
- Booking cannot be created without an active hold.
- Successful booking changes PostgreSQL state to `BOOKED`.
- Exactly one booking is persisted.
- Successful booking removes the Redis hold.

---

## Redis and PostgreSQL Responsibilities

The current design intentionally separates temporary and permanent state.

```text
Redis
├── temporary seat ownership
├── expiration
├── short-lived coordination
└── multi-instance shared state


PostgreSQL
├── Event
├── Seat
├── Booking
├── persistent status
├── transaction integrity
├── pessimistic locking
└── final source of truth
```

The design principle is:

> Redis is used for coordination, not as the authoritative booking database.

---

## Failure Considerations

Redis and PostgreSQL are separate systems.

For example:

```text
PostgreSQL booking succeeds
        │
        ▼
Redis release fails
```

could temporarily leave a stale hold.

The hold TTL limits how long this state can survive.

The permanent booking remains correct because PostgreSQL is still the source of truth.

This trade-off is intentional for the current implementation.

Future distributed-system phases will continue exploring cross-system consistency and reliable event processing.

---

## Key Engineering Findings

The Redis phase demonstrated that:

- Temporary reservations and permanent bookings have different lifecycle requirements.
- Database row locks should not remain open while users complete checkout.
- Redis `SET NX` provides atomic temporary ownership acquisition.
- TTL provides automatic expiration for abandoned reservations.
- Hold release must enforce ownership.
- Compare-and-delete should be atomic.
- Redis state should not be created without validating the source-of-truth resource.
- Redis does not replace PostgreSQL transaction guarantees.
- Shared Redis state can coordinate multiple independent application instances.
- Correctness should be validated with both integration tests and multi-instance experiments.