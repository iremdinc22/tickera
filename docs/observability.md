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

# Grafana Dashboard

The Tickera dashboard currently tracks:

```text
HTTP
├── Request rate
└── Booking p95 latency

Database
├── Hikari active
├── Hikari idle
├── Hikari pending
└── Hikari maximum

Application
├── Transaction duration
├── Seat-lock duration
├── Save duration
└── Flush duration

Tomcat
├── Busy threads
├── Current threads
└── Maximum threads

JVM
├── Heap memory
└── CPU usage
```

The dashboard is primarily used while k6 workloads are running.

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

# Key Findings

The observability work demonstrated that:

- External load-test metrics alone are not enough to diagnose bottlenecks.
- Connection-pool saturation can be a symptom rather than a root cause.
- Thread-pool saturation does not automatically mean more threads will improve throughput.
- Individual SQL execution time can be very small while end-to-end latency remains high.
- Application-level timers help separate database work from broader request latency.
- Metrics are most useful when used to form and test engineering hypotheses.