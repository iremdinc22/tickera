# Changelog

All notable changes to Tickera are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Tickera is an engineering portfolio project. Version numbers describe the
state of the reference implementation, not a commercial release cadence.

---

## [1.0.0] — 2026-11-18

First tagged release of the reference implementation.

### Added

**Booking and concurrency control**

- Booking API with pessimistic and optimistic seat locking strategies and a
  documented comparison between them.
- Explicit seat and event model with a `Seat` version field for optimistic
  locking.
- Automated concurrency tests against real PostgreSQL through Testcontainers,
  covering both locking strategies.

**Idempotency**

- `Idempotency-Key` support on `POST /bookings`.
- Durable idempotency records with request fingerprinting (SHA-256) and a
  unique constraint that arbitrates concurrent retries.
- Replay semantics: the same key and payload return the existing booking; the
  same key with a different payload returns `409 Conflict`.

**Redis seat holds**

- Temporary seat reservations using `SET NX` with a TTL.
- Hold ownership validation and hold → booking → release lifecycle.
- Multi-instance coordination through shared Redis state.

**Kafka and reliable event processing**

- `BookingCreatedEvent` publication to the `booking-created` topic.
- Transactional outbox: booking state and event intent commit in the same
  PostgreSQL transaction.
- Claim-based `PENDING → PROCESSING → PUBLISHED` lifecycle so that Kafka
  acknowledgement is awaited outside the database transaction.
- Acknowledgement-based publishing with a configurable timeout
  (`tickera.outbox.publish-ack-timeout`, default `10s`).
- Explicit retry and backoff: `2s → 5s → 10s → 30s`, maximum 5 attempts,
  `FAILED` state with `lastError`.
- Stale `PROCESSING` recovery (2-minute threshold, 30-second job) for
  processes that crash mid-publish.
- Manual requeue endpoint for permanently failed events.
- Multi-instance outbox publishing with row-level claiming.
- At-least-once delivery semantics with an idempotent consumer backed by the
  `processed_events` table.
- Kafka integration testing with Testcontainers.

**Observability**

- Spring Boot Actuator and Micrometer metrics.
- Outbox metrics: published, retry, stale recovery, pending and failed.
- Provisioned Grafana dashboards:
  - `Tickera — Application Overview`
  - `Tickera — Outbox & Kafka Reliability`
- Prometheus alert rules for target availability, HTTP 5xx ratio, outbox
  backlog and failed events (development thresholds, no Alertmanager channel).

**Load testing**

- k6 suite with six maintained scripts: `hot-seat.js`,
  `concurrent-booking.js`, `parallel-booking.js`, `sustained-load.js`,
  `capacity-test.js` and `idempotency-replay.js`.
- Shared configuration helper (`load-tests/lib/config.js`) for base URL,
  validated parameters and unique idempotency keys.
- `load-tests/prepare-test-data.sh` for creating a fresh event with
  consecutive seats.
- `load-tests/README.md` with data sizing, smoke runs, full runs and
  troubleshooting.

**Continuous integration**

- GitHub Actions workflow that runs the full Maven verification on Java 21 for
  every push and pull request to `main`.

### Changed

- Maven project version set to `1.0.0`.
- k6 scripts use the current API contract, environment-configurable base URL
  and parameters, and unique `Idempotency-Key` values per independent attempt.
- README restructured as a technical overview: objectives, booking lifecycle,
  locking comparison, Redis holds, concurrent idempotency, outbox, retry and
  stale recovery, at-least-once delivery, observability, load testing, setup
  and limitations.

### Documentation

- `docs/kafka-event-processing.md` corrected to describe the implemented
  `PROCESSING` lifecycle, acknowledgement timeout, retry/backoff table, stale
  recovery, metric names and manual requeue.
- `docs/performance-testing.md` now marks all recorded measurements as a
  historical baseline collected before the current API and outbox work, and
  documents the current scripts.
- `docs/architecture/architecture-v1.md` marked as a historical snapshot.
- Removed outdated roadmap and "next step" statements that were already
  implemented.

### Engineering validation

- Full Maven verification: 35 tests passing on Java 21, including PostgreSQL,
  Redis and Kafka integration tests.
- All six k6 scripts validated with `k6 inspect`.
- Controlled smoke runs validated the scripts and API contract; results are
  functional checks, not capacity benchmarks.

### Notes

- Performance numbers in `docs/performance-testing.md` are historical local
  measurements from an earlier implementation. They are not measurements of
  this release and not production capacity claims.
- Known limitations are documented in the README: manual outbox requeue, no
  dead-letter topic, no outbox retention policy, no Alertmanager integration,
  process-local metric counters, and development-only Compose topology.

---

[1.0.0]: https://github.com/iremdinc22/tickera/releases/tag/v1.0.0
