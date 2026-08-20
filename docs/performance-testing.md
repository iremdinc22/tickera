# Performance Testing

This document contains the performance and capacity experiments performed on Tickera.

The objective is not to claim production-scale capacity.

The objective is to understand how the system behaves as concurrency and request rate increase.

---

# Testing Tool

Tickera uses k6 for load testing.

k6 generates repeatable workloads and measures:

- Request latency
- p95 latency
- Throughput
- Booking outcomes
- Dropped iterations
- Unexpected responses

Custom metrics distinguish business conflicts from infrastructure failures.

```text
booking_created
booking_conflict
unexpected_responses
valid_booking_response
```

A `409 Conflict` during a contested-seat workload is expected behavior, not a system failure.

---

# Hot-Seat Test

Multiple users attempt to book the same seat.

Example:

```text
100 users
    │
    ▼
Same Seat
    │
    ▼
Booking API
```

Expected result:

```text
1 × Created
99 × Conflict
```

This workload measures both correctness and contention behavior.

---

# Parallel-Seat Test

Users concurrently book different seats.

```text
100 users
    │
    ▼
100 independent seats
```

Unlike the hot-seat test, there is minimal logical contention between transactions.

This workload helps distinguish general system overhead from same-row contention.

---

# Locking Strategy Comparison

Representative local results:

| Workload | Strategy | Avg Latency | p95 | Throughput |
|---|---|---:|---:|---:|
| Hot seat | Pessimistic | 224.24 ms | 245.34 ms | ~359.76 req/s |
| Hot seat | Optimistic | 317.55 ms | 324.06 ms | ~292.18 req/s |
| Parallel seats | Pessimistic | 135.07 ms | 180.91 ms | ~467.76 req/s |
| Parallel seats | Optimistic | 135.43 ms | 155.89 ms | ~531.19 req/s |

Observed behavior:

```text
High contention
      │
      ▼
Pessimistic performed better


Independent seats
      │
      ▼
Optimistic showed better
p95 and throughput
```

These measurements are workload-specific and local.

---

# Sustained Load Testing

A constant arrival rate was used to observe the system over longer periods.

Representative low-load configuration:

```text
10 requests/second
30 seconds
```

Result:

```text
Requests completed : 300
Request rate       : ~10 req/s
Failures           : 0
Average latency    : 15.83 ms
p95 latency        : 23.65 ms
Maximum latency    : 468.52 ms
```

At this level, HikariCP remained largely underutilized.

---

# Capacity Testing

The request rate was gradually increased.

Representative observations:

```text
10 req/s       Stable
50 req/s       Stable
75 req/s       Stable
90 req/s       Stable
100 req/s      Stable
150 req/s      Stable
200 req/s      Stable
250 req/s      Stable
400 req/s      Stable
800 req/s      Minor dropped iterations
1200 req/s     Increased latency and drops
1500 req/s     Further latency growth
2000 req/s     Clear saturation signals
```

The exact values should not be interpreted as production capacity.

All components were running on the same local development machine.

---

# 2000 req/s Experiment

With HikariCP configured with 10 maximum connections:

```text
Requested rate       : 2000 req/s
Achieved throughput  : ~1981 req/s
Average latency      : 19.07 ms
p95 latency          : 93.11 ms
Maximum latency      : 757 ms
Dropped iterations   : 559
HTTP failures        : 0%
```

At peak load:

```text
HikariCP max connections     : 10
HikariCP active connections  : 10
HikariCP idle connections    : 0
HikariCP pending connections : > 0
```

This showed connection waiting.

However, connection waiting does not automatically mean the pool is the root cause.

---

# HikariCP Experiment

The connection pool was increased:

```text
10 → 20
```

Comparison:

| Metric | Pool 10 | Pool 20 |
|---|---:|---:|
| Requested rate | 2000 req/s | 2000 req/s |
| Throughput | ~1981 req/s | ~1943 req/s |
| Average latency | 19.07 ms | 47.00 ms |
| p95 | 93.11 ms | 260.04 ms |
| Max latency | 757 ms | 1.39 s |
| Dropped iterations | 559 | 1642 |

Increasing the pool made performance worse.

Therefore:

> Connection-pool saturation was a symptom, not proof that the pool needed to be larger.

The pool size was restored to 10.

---

# PostgreSQL Investigation

`pg_stat_statements` was enabled to inspect query execution.

Representative measurements:

```text
Query                                Mean Execution
---------------------------------------------------
INSERT booking                       ~0.082 ms
UPDATE seat                          ~0.063 ms
SELECT seat FOR UPDATE               ~0.031 ms
```

Individual SQL statements were extremely fast compared with observed HTTP latency.

This suggested that end-to-end latency was caused by more than raw SQL execution.

---

# WAL Experiment

PostgreSQL synchronous commit behavior was temporarily changed:

```text
synchronous_commit

ON → OFF
```

The experiment reduced WAL synchronization work but did not materially improve overall application throughput.

The original setting was restored.

Conclusion:

> WAL synchronization contributed to database work but was not identified as the primary bottleneck.

---

# Tomcat Experiment

During a high-load run:

```text
Tomcat busy threads    : 200
Tomcat current threads : 200
Tomcat max threads     : 200
```

The maximum worker count was increased:

```text
200 → 400
```

Comparison:

| Metric | 200 Threads | 400 Threads |
|---|---:|---:|
| Throughput | ~1917 req/s | ~1918 req/s |
| Average latency | 70.25 ms | 67.06 ms |
| p95 | 332.83 ms | 293.94 ms |
| Dropped iterations | 2440 | 1864 |

Thread saturation disappeared.

Throughput remained almost unchanged.

Therefore:

> Tomcat's 200-thread limit was not the primary throughput bottleneck.

---

# k6 VU Experiment

k6 initially reported:

```text
Insufficient VUs, reached 500 active VUs
```

Configuration:

```text
maxVUs: 500 → 1000
```

Comparison:

| Metric | 500 VUs | 1000 VUs |
|---|---:|---:|
| Throughput | ~1917 req/s | ~1710 req/s |
| Average latency | ~70 ms | ~475 ms |
| p95 | ~333 ms | ~917 ms |
| Dropped iterations | 2440 | 8016 |

More concurrency did not produce more throughput.

Instead:

```text
More concurrent work
        │
        ▼
More waiting
        │
        ▼
Longer requests
        │
        ▼
Higher concurrency
        │
        ▼
Higher latency
```

---

# Capacity Conclusion

Several potential bottlenecks were tested:

```text
HikariCP
10 → 20
   │
   └── No improvement

PostgreSQL synchronous commit
ON → OFF
   │
   └── No material improvement

Tomcat
200 → 400 threads
   │
   └── No throughput improvement

k6
500 → 1000 VUs
   │
   └── Worse latency
```

No single parameter explained the entire saturation behavior.

The experiments instead demonstrated a broader principle:

> Once a system reaches its useful capacity, increasing concurrency can increase waiting without increasing throughput.

---

# Benchmark Limitations

All measurements were collected in a local development environment.

The same machine was running:

```text
k6
Spring Boot JVM
PostgreSQL
Docker
Prometheus
Grafana
```

Therefore these results are useful for:

- Comparing architectural changes
- Detecting regressions
- Understanding bottlenecks
- Learning system behavior

They are not production capacity claims.