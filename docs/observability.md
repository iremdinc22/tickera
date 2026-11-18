# Observability

Tickera uses application and infrastructure metrics to understand system behavior under concurrent load.

The observability stack is:

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

---

# Local Access

The observability stack runs through `docker-compose.yml`.

```text
Application        http://localhost:8080
Actuator health    http://localhost:8080/actuator/health
Prometheus scrape  http://localhost:8080/actuator/prometheus

Prometheus         http://localhost:9090
Prometheus alerts  http://localhost:9090/alerts
Prometheus rules   http://localhost:9090/rules

Grafana            http://localhost:3000
```

Prometheus and Grafana images are pinned in
`docker-compose.yml` so the local stack is reproducible.

Provisioning is file-based:

```text
prometheus.yml                          Scrape configuration
prometheus/rules/tickera-alerts.yml     Alert rules

grafana/provisioning/datasources/       Prometheus datasource (uid: prometheus)
grafana/provisioning/dashboards/        Dashboard provider
grafana/dashboards/                     Dashboard JSON files
```

The Prometheus datasource is marked as the default datasource.
Dashboards are provisioned into the `Tickera` folder and are
read-only from the UI (`allowUiUpdates: false`).

---

# Why Observability Was Added

Load-test results provide external measurements such as:

```text
Throughput
Latency
p95
Failures
Dropped iterations
```

But these metrics do not explain why the application behaves that way.

For example:

```text
p95 = 300 ms
```

does not tell us whether the time was spent:

- Waiting for a database connection
- Waiting for a seat lock
- Executing SQL
- Waiting for a Tomcat worker
- Performing application logic
- Running garbage collection

Observability was introduced to connect external load-test behavior with internal runtime behavior.

---

# Spring Boot Actuator

Spring Boot Actuator exposes runtime information about the application.

Metrics are exported through Micrometer.

Prometheus periodically scrapes these metrics.

Grafana is used for visualization.

---

# HikariCP Metrics

The database connection pool is monitored using:

```text
hikaricp.connections.active
hikaricp.connections.idle
hikaricp.connections.pending
hikaricp.connections.max
hikaricp.connections.acquire
hikaricp.connections.usage
hikaricp.connections.timeout
```

These metrics help answer:

> Are requests waiting for database connections?

Example low-load behavior:

```text
Maximum : 10
Active  : 0–1
Idle    : 9–10
Pending : 0
```

The connection pool is not constrained in this state.

At higher load:

```text
Maximum : 10
Active  : 10
Idle    : 0
Pending : > 0
```

Requests have begun waiting for connections.

This is a saturation signal, but not automatically proof that the pool should be enlarged.

---

# HTTP Metrics

The dashboard tracks:

- Request rate
- Booking endpoint latency
- p95 response time

These metrics are compared with k6 measurements to understand external API behavior.

---

# Tomcat Metrics

The embedded Tomcat server is monitored using:

```text
Busy threads
Current threads
Maximum threads
```

During one high-load experiment:

```text
Busy    : 200
Current : 200
Maximum : 200
```

The maximum was temporarily increased to 400.

Although thread saturation disappeared, throughput remained almost unchanged.

Observability therefore helped reject Tomcat worker count as the primary bottleneck.

---

# JVM Metrics

The dashboard also tracks:

```text
Heap memory
CPU usage
```

These metrics help determine whether the JVM itself is approaching resource limits during load tests.

---

# Custom Booking Metrics

Micrometer timers were added around important sections of the booking flow.

```text
booking.transaction.duration
booking.seat.lock.duration
booking.save.duration
booking.flush.duration
```

Conceptually:

```text
HTTP Request
     │
     ▼
Booking Transaction Timer
     │
     ├── Seat Lock Timer
     │
     ├── Save Timer
     │
     └── Flush Timer
     │
     ▼
HTTP Response
```

This allows end-to-end HTTP latency to be compared with internal booking-operation timing.

These timers are recorded on the plain `createBooking` and
`createBookingOptimistic` service paths. The HTTP endpoints that are
currently exposed (`POST /bookings` and `POST /bookings/held`) use the
idempotent and Redis-hold flows, so the `booking.*` timers are normally
absent during API traffic and during k6 runs.

For that reason the provisioned Grafana dashboards derive booking
throughput, conflict and latency panels from the standard
`http_server_requests_seconds_*` metrics filtered by:

```text
uri =~ "/bookings.*"
```

This avoids fabricating data for timers that the running endpoints do
not emit. The internal timers remain useful when the service methods are
exercised directly.

---

# Outbox and Kafka Metrics

The outbox publisher exposes custom Micrometer meters. Micrometer
translates the names to Prometheus conventions (dots become
underscores and counters receive a `_total` suffix).

```text
tickera_outbox_published_total
tickera_outbox_retry_total
tickera_outbox_stale_recovery_total
tickera_outbox_pending_events
tickera_outbox_failed_events
```

Semantics:

```text
tickera_outbox_published_total
└── Counter: events successfully published to Kafka
    and marked PUBLISHED

tickera_outbox_retry_total
├── Counter: retries scheduled after a publish failure
└── Counter: retries scheduled by stale recovery

tickera_outbox_stale_recovery_total
└── Counter: PROCESSING events recovered as stale

tickera_outbox_pending_events
└── Gauge: current number of PENDING outbox events

tickera_outbox_failed_events
└── Gauge: current number of FAILED outbox events
```

Labels are intentionally low-cardinality. No booking IDs, event IDs,
aggregate IDs or user IDs are used as label values.

## Transaction-boundary fix

Metrics must describe committed state, not attempted state.

The publish-failure retry counter was already incremented only after the
failure-state transaction completed.

Stale recovery previously incremented `tickera_outbox_stale_recovery_total`
and `tickera_outbox_retry_total` inside the recovery transaction. If that
transaction rolled back, the counters still increased even though no
state transition had been committed.

`OutboxPublisher.recoverStaleProcessingEvents()` now returns a
`RecoveryOutcome` from the transaction and increments the counters only
after a successful commit.

```text
Recovery transaction
├── lock stale PROCESSING rows
├── move them to PENDING or FAILED
└── commit
        │
        ▼
increment counters
```

On rollback, the counters remain unchanged. This behavior is covered by
`OutboxMetricsIntegrationTest`.

---

# PostgreSQL Observability

`pg_stat_statements` was enabled to inspect query execution behavior.

Representative measurements showed that individual booking queries executed very quickly:

```text
INSERT booking                ~0.082 ms
UPDATE seat                   ~0.063 ms
SELECT seat FOR UPDATE        ~0.031 ms
```

This was useful because end-to-end request latency was much higher near saturation.

Therefore:

> Slow HTTP responses were not explained by individual SQL execution time alone.

---

# Grafana Dashboards

Two provisioned dashboards are available.

## Tickera — Application Overview

`uid: tickera-application-overview`

```text
HTTP
├── Request rate by URI
├── 5xx error rate (%)
├── p95 / p99 latency
├── Booking API p95 latency
├── Booking throughput (2xx)
├── Booking conflicts (HTTP 409)
└── Booking success ratio

Runtime
├── JVM heap used / max
├── Process and system CPU usage
├── Hikari active / idle / pending / max
└── Tomcat busy / current / max threads
```

## Tickera — Outbox & Kafka Reliability

`uid: tickera-outbox-kafka`

```text
Rates
├── Published outbox events / sec
├── Outbox retry rate
└── Stale processing recoveries / sec

Current state
├── Pending outbox events
├── Failed outbox events
└── Published outbox events (total)
```

The pending and failed panels use development thresholds for visual
clarity (10/50 for pending, 1 for failed). They are not production SLAs.

All panels are backed by real Prometheus series:

```text
http_server_requests_seconds_count
http_server_requests_seconds_bucket
jvm_memory_used_bytes
jvm_memory_max_bytes
process_cpu_usage
system_cpu_usage
hikaricp_connections_*
tomcat_threads_*
tickera_outbox_*
```

The dashboard JSON files are the source of truth. They are picked up by
Grafana provisioning and do not require manual UI work.

```text
k6
 │
 │ traffic
 ▼
Tickera
 │
 │ metrics
 ▼
Prometheus
 │
 ▼
Grafana
```

This makes it possible to correlate load with internal system behavior.

---

# Prometheus Alert Rules

`prometheus/rules/tickera-alerts.yml` defines a small development rule
set. Thresholds are examples chosen for a local environment and are not
production SLAs.

```text
TickeraTargetDown
└── up{job="tickera"} == 0 for 1m
    severity: critical

TickeraOutboxFailedEvents
└── sum(tickera_outbox_failed_events) > 0 for 1m
    severity: warning

TickeraOutboxBacklog
└── sum(tickera_outbox_pending_events) > 50 for 5m
    severity: warning

TickeraHttp5xxRateHigh
└── non-Actuator 5xx ratio > 5% for 5m
    severity: warning
```

Actuator endpoints are excluded from the HTTP error-rate rule so that
health and scrape traffic do not distort the result.

Alertmanager and external notification services are intentionally out of
scope. Rules are evaluated by Prometheus and are visible in the Prometheus
UI (`/alerts` and `/rules`).

---

# Observability-Driven Investigation

One of the main lessons from the performance experiments was that a visible saturated resource is not necessarily the root cause.

For example:

```text
Hikari pending connections > 0
```

could suggest:

```text
Increase connection pool
```

But when the pool was increased:

```text
10 → 20
```

performance became worse.

Similarly:

```text
Tomcat busy threads = max threads
```

suggested possible HTTP worker saturation.

But increasing:

```text
200 → 400
```

did not materially improve throughput.

Observability therefore should not be used merely to identify high numbers.

It should be used to form hypotheses that are then tested experimentally.

---

# Runtime Verification

The stack was verified against a running application:

```text
/actuator/health                  UP
Prometheus target                 up{job="tickera"} = 1
Alert rule health                 ok for all 4 rules
Grafana provisioning              datasource and 2 dashboards provisioned
```

A controlled booking scenario produced real series:

```text
Create event
Hold seat (Redis)
Held booking                    201
Idempotent booking              201
Idempotent replay               201
Same key, different request     409
Already-booked seat             409
```

Metric responses:

```text
tickera_outbox_published_total   increased
http_server_requests_seconds_*   booking rate, 409 conflict rate and
                                 p95 latency became non-zero
```

A controlled Kafka interruption was also tested:

```text
1. Stop the Kafka broker
2. Create a booking
3. Publisher fails and schedules retries
   → tickera_outbox_retry_total increases
   → tickera_outbox_pending_events becomes 1
4. Start the Kafka broker
5. Publisher publishes the event
   → tickera_outbox_published_total increases
   → tickera_outbox_pending_events returns to 0
```

No volumes or existing data were removed.

---

# Known Limitations

- The `booking.*` timers are not emitted by the currently exposed
  endpoints, so booking panels use HTTP metrics instead.
- Latency panels show no data when no requests fall inside the selected
  rate window. This is expected Prometheus histogram behavior.
- Grafana provisioning and datasource state can be verified through
  container logs and the Grafana SQLite database. Browser/API login may
  be unavailable if the existing volume has an unknown admin password;
  this does not affect the provisioned datasource or dashboards.
- Alert rules have no notification channel because Alertmanager is out
  of scope.

---

# Key Findings

The observability work demonstrated that:

- External load-test metrics alone are not enough to diagnose bottlenecks.
- Connection-pool saturation can be a symptom rather than a root cause.
- Thread-pool saturation does not automatically mean more threads will improve throughput.
- Individual SQL execution time can be very small while end-to-end latency remains high.
- Application-level timers help separate database work from broader request latency.
- Metrics are most useful when used to form and test engineering hypotheses.