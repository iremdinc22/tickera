# Architecture v1

> Historical note: this is the initial architecture before Redis seat holds,
> idempotency, the transactional outbox and Kafka. The current architecture
> is documented in the project README.

Client
  |
  v
Tickera Spring Boot API
  |
  v
PostgreSQL