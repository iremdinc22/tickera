# Kafka & Reliable Event Processing

This document describes how Tickera evolved from direct Kafka publishing to a reliable event-processing flow using the Transactional Outbox Pattern and an idempotent consumer.

The implementation was introduced only after reproducing the failure it is intended to solve.

---

## 1. Why Kafka?

A successful booking can trigger work that does not need to be performed synchronously inside the booking HTTP request.

Examples include:

- Notifications
- Analytics
- Audit processing
- Downstream service integration
- Other reactions to a successful booking

Tickera represents the completed booking as a `BookingCreatedEvent` and publishes it to:

```text
booking-created
```

Conceptually:

```text
Booking committed
       │
       ▼
BookingCreatedEvent
       │
       ▼
     Kafka
       │
       ▼
    Consumer
```

This decouples the booking transaction from asynchronous consumers.

---

## 2. Initial Producer / Consumer Flow

The first implementation published `BookingCreatedEvent` directly to Kafka after the database transaction committed.

```text
BookingService
     │
     ▼
PostgreSQL transaction
     │
     ▼
   COMMIT
     │
     ▼
afterCommit()
     │
     ▼
Kafka publish
```

At first this appears reasonable: the event is published only after the booking has been successfully committed.

However, it creates a failure window.

---

## 3. The Dual-Write Problem

PostgreSQL and Kafka are two independent systems.

A successful write to one does not automatically make a write to the other successful.

The problematic sequence is:

```text
1. Booking INSERT
2. Seat becomes BOOKED
3. PostgreSQL COMMIT          ✅
4. Kafka publish             ❌
```

The resulting system state is:

```text
PostgreSQL
Booking #42                  ✅

Kafka
BookingCreated #42           ❌
```

The booking exists permanently, but downstream systems never receive the corresponding event.

Restarting Kafka does not fix this automatically because the failed event was not stored anywhere durable for later publication.

---

## 4. Reproducing the Failure

The failure was intentionally reproduced by stopping Kafka before creating a new booking.

```bash
docker stop tickera-kafka
```

A booking request was then sent for an available seat using a fresh idempotency key.

The observed state was:

```text
Booking row exists           ✅
Seat is BOOKED               ✅
Kafka publish                ❌
```

This demonstrated the exact reliability problem that motivated the next architectural change.

The important lesson is:

> Publishing after commit prevents publishing an event for a rolled-back booking, but it does not guarantee that a committed booking will eventually produce an event.

---

## 5. Why `afterCommit()` Is Not Enough

`afterCommit()` correctly orders the operations:

```text
Database commit
      ↓
Kafka publish
```

but it does not make them atomic.

There is still a point between the two operations where:

- Kafka can be unavailable
- The application can crash
- The network can fail
- The process can be terminated

```text
COMMIT
  │
  │ failure window
  ▼
Kafka
```

The application therefore needs a durable representation of the event before the database transaction finishes.

---

## 6. Transactional Outbox Pattern

Tickera writes the booking changes and an outbox record inside the same PostgreSQL transaction.

```text
Booking request
      │
      ▼
┌─────────────────────────────┐
│   PostgreSQL Transaction    │
│                             │
│   Booking / Seat changes    │
│             +               │
│      OutboxEvent INSERT     │
│                             │
└──────────────┬──────────────┘
               │
             COMMIT
               │
               ▼
       outbox_events
           PENDING
```

This changes the guarantee.

If the transaction commits:

```text
Booking exists               ✅
Outbox event exists          ✅
```

If the transaction rolls back:

```text
Booking exists               ❌
Outbox event exists          ❌
```

The booking and the intent to publish its event therefore cannot diverge at this boundary.

---

## 7. `outbox_events`

An outbox record contains the information required to publish the event later.

The table contains fields such as:

```text
id
aggregate_type
aggregate_id
event_type
payload
status
created_at
published_at
```

Example:

```text
aggregate_type = BOOKING
event_type      = BOOKING_CREATED
status          = PENDING
```

The serialized event is stored in `payload`.

Basic lifecycle:

```text
PENDING
   │
   ▼
publish to Kafka
   │
   ▼
PUBLISHED
```

---

## 8. Outbox Publisher

`OutboxPublisher` periodically searches for events that still need to be delivered.

Conceptually:

```text
@Scheduled
    │
    ▼
find pending events
    │
    ▼
deserialize payload
    │
    ▼
publish to Kafka
    │
    ▼
Kafka ACK received?
    │
 ┌──┴───┐
 │      │
YES     NO
 │      │
 ▼      ▼
PUBLISHED
        │
        └── retry later
```

The Kafka publisher waits for the send result before the outbox event is considered successfully published.

Calling an asynchronous `send()` method alone is not proof that Kafka accepted the record.

---

## 9. Kafka Failure and Recovery

The outbox design was explicitly tested with Kafka unavailable.

While Kafka was down:

```text
Booking                         ✅
OutboxEvent                     ✅
Outbox event retained           ✅
Kafka delivery                  ❌
```

After Kafka was started again:

```bash
docker start tickera-kafka
```

the publisher could retry the durable event.

Observed lifecycle:

```text
PENDING
   │
   │ Kafka becomes available
   ▼
publish
   │
   ▼
PUBLISHED
```

`published_at` is populated after successful publication.

The important difference from direct publishing is that the event does not disappear during the outage.

---

## 10. Multi-Instance Publishing

A real deployment can run more than one Tickera instance.

```text
                PostgreSQL
                    │
              outbox_events
                    │
           ┌────────┴────────┐
           ▼                 ▼
      Tickera A          Tickera B
      Publisher          Publisher
           │                 │
           └────────┬────────┘
                    ▼
                   Kafka
```

This creates another concurrency problem:

> What happens if two publishers see the same pending event?

The outbox flow was tested with multiple application instances sharing PostgreSQL and Kafka.

For the tested booking, Kafka inspection showed one record for the target booking.

Example verification:

```bash
docker exec tickera-kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic booking-created \
  --from-beginning \
  --property print.key=true \
  --timeout-ms 3000 \
  2>/dev/null | grep -c BOOKING_ID
```

Expected result for the tested event:

```text
1
```

Multi-instance execution remains an important constraint for future outbox changes.

---

## 11. Why Duplicate Delivery Still Matters

Preventing two publishers from intentionally claiming the same event does not mean duplicate delivery can never happen.

Consider:

```text
OutboxPublisher
      │
      ▼
Kafka publish                 ✅
      │
      ▼
application crashes           💥
      │
      ▼
local success state not saved
      │
      ▼
event can be retried
```

Therefore:

> Reliable publishing does not imply that consumers can assume exactly one delivery.

Consumers must protect their own side effects.

---

## 12. Idempotent Consumer Pattern

Every `BookingCreatedEvent` has an event ID.

Tickera stores processed IDs in:

```text
processed_events
```

Current structure:

```text
event_id       UUID PRIMARY KEY
event_type
processed_at
```

The consumer uses this durable record to detect duplicate delivery.

```text
BookingCreatedEvent
        │
        ▼
event_id processed?
        │
   ┌────┴────┐
   │         │
  NO        YES
   │         │
   ▼         ▼
process     ignore
   │
   ▼
INSERT processed_events
```

Because `event_id` is the primary key, PostgreSQL provides a uniqueness boundary for processed events.

---

## 13. Duplicate Event Integration Test

The idempotent consumer is protected with an integration test.

The test performs:

```text
1. Create BookingCreatedEvent with eventId = X
2. Publish X to Kafka
3. Wait until the consumer processes X
4. Verify processed_events contains X
5. Publish exactly the same event again
6. Verify processed_events still contains only one record
```

Expected behavior:

```text
Delivery #1
    │
    ▼
PROCESS
    │
    ▼
processed_events: X

Delivery #2
    │
    ▼
X already processed
    │
    ▼
IGNORE
```

The logical event is therefore processed once even though it was delivered twice.

---

## 14. Kafka Integration Testing with Testcontainers

Kafka integration testing uses a real Kafka broker through Testcontainers.

```text
JUnit
  │
  ▼
Testcontainers
  │
  ▼
Apache Kafka container
  │
  ▼
Spring Kafka Producer
  │
  ▼
Spring Kafka Consumer
  │
  ▼
PostgreSQL
```

This approach replaced the embedded Kafka test after the embedded KRaft broker failed during test context startup.

The test dynamically supplies the container's bootstrap server to Spring Boot.

Run the integration test with:

```bash
./mvnw -Dtest=IdempotentConsumerIntegrationTest test
```

Expected result:

```text
Tests run: 1
Failures: 0
Errors: 0

BUILD SUCCESS
```

---

## 15. Kafka JSON Deserialization

`BookingCreatedEvent` is serialized as JSON.

The consumer explicitly trusts the package containing Tickera's event classes:

```yaml
spring:
  kafka:
    consumer:
      properties:
        spring.json.trusted.packages: com.iremdinc.tickera.event
```

This resolved the deserialization failure:

```text
BookingCreatedEvent is not in the trusted packages
```

Using the exact event package is preferable to trusting every package using:

```text
*
```

---

## 16. Producer and Consumer Reliability Solve Different Problems

The current architecture separates two concerns.

### Producer Side

Transactional Outbox protects:

```text
PostgreSQL business state
           │
           ▼
event eventually published
```

Its purpose is to avoid losing the event after the business transaction has committed.

### Consumer Side

Idempotent consumption protects:

```text
event delivered
      │
      ▼
logical side effect executed once
```

Its purpose is to tolerate duplicate event delivery.

Together:

```text
Booking Transaction
        │
        ▼
Transactional Outbox
        │
        ▼
      Kafka
        │
        ▼
Idempotent Consumer
```

The architecture does not assume failures and duplicates cannot happen.

Instead, it makes them recoverable.

---

## 17. Current Limitations

The core reliability model is implemented, but retry behavior is still intentionally simple.

The next production-oriented improvements include:

- Retry attempt count
- Retry backoff
- `nextRetryAt`
- `FAILED` status
- Last error storage
- Maximum retry policy
- Handling permanently invalid payloads
- Outbox backlog metrics
- Retry metrics
- Failure metrics
- Cleanup / retention strategy for published events

This will prevent permanently broken events from being retried forever every few seconds.

---

## 18. Engineering Findings

The Kafka phase produced several important findings:

- A successful PostgreSQL commit does not imply a successful Kafka publish.
- `afterCommit()` provides ordering, not atomicity across PostgreSQL and Kafka.
- A failed event must exist somewhere durable if it is expected to be retried later.
- The Transactional Outbox Pattern turns publication intent into database state.
- Kafka outages do not need to invalidate already committed booking state.
- An outbox event should not be marked as published before Kafka confirms the send.
- Multi-instance deployments introduce concurrency concerns for background publishers.
- Reliable messaging must account for possible duplicate delivery.
- Producer-side reliability and consumer-side idempotency solve different problems.
- Database uniqueness constraints can participate in duplicate protection.
- Integration tests are valuable for testing actual messaging behavior.
- Testcontainers allows Kafka integration behavior to be tested against a real broker.

---

## 19. Resulting Architecture

```text
                         Booking Request
                               │
                               ▼
                         BookingService
                               │
                               ▼
                 ┌────────────────────────┐
                 │ PostgreSQL Transaction │
                 │                        │
                 │ Booking / Seat         │
                 │ Idempotency            │
                 │ OutboxEvent            │
                 └───────────┬────────────┘
                             │
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
                     booking-created
                             │
                             ▼
                BookingCreatedEventConsumer
                             │
                             ▼
                      processed_events
                             │
                      duplicate guard
```

The important architectural boundary is:

> PostgreSQL stores both the business state and the durable intent to publish. Kafka handles asynchronous delivery. Consumers remain responsible for processing events idempotently.

---

## 20. Next Step

The next iteration will make outbox retry behavior production-oriented.

Target lifecycle:

```text
PENDING
   │
   ▼
PROCESSING
   │
   ├── success ─────────────► PUBLISHED
   │
   └── failure
          │
          ▼
     retryCount + 1
          │
          ▼
      backoff delay
          │
     ┌────┴─────┐
     │          │
 retry again   max retries
     │          │
     ▼          ▼
 PENDING      FAILED
```

This will add explicit failure handling instead of allowing problematic events to retry indefinitely.