package com.iremdinc.tickera.metrics;

import com.iremdinc.tickera.enums.OutboxStatus;
import com.iremdinc.tickera.repository.OutboxEventRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class OutboxMetrics {

    private final Counter publishedCounter;
    private final Counter retryCounter;
    private final Counter staleRecoveryCounter;

    public OutboxMetrics(
            MeterRegistry meterRegistry,
            OutboxEventRepository outboxEventRepository
    ) {

        this.publishedCounter =
                Counter.builder("tickera.outbox.published")
                        .description(
                                "Total number of successfully published outbox events"
                        )
                        .register(meterRegistry);

        this.retryCounter =
                Counter.builder("tickera.outbox.retry")
                        .description(
                                "Total number of outbox publish retries"
                        )
                        .register(meterRegistry);

        this.staleRecoveryCounter =
                Counter.builder("tickera.outbox.stale.recovery")
                        .description(
                                "Total number of stale PROCESSING outbox events recovered"
                        )
                        .register(meterRegistry);

        Gauge.builder(
                        "tickera.outbox.pending.events",
                        outboxEventRepository,
                        repository ->
                                repository.countByStatus(
                                        OutboxStatus.PENDING
                                )
                )
                .description(
                        "Current number of PENDING outbox events"
                )
                .register(meterRegistry);

        Gauge.builder(
                        "tickera.outbox.failed.events",
                        outboxEventRepository,
                        repository ->
                                repository.countByStatus(
                                        OutboxStatus.FAILED
                                )
                )
                .description(
                        "Current number of FAILED outbox events"
                )
                .register(meterRegistry);
    }

    public void incrementPublished() {
        publishedCounter.increment();
    }

    public void incrementRetry() {
        retryCounter.increment();
    }

    public void incrementRetry(int count) {
        retryCounter.increment(count);
    }

    public void incrementStaleRecovery() {
        staleRecoveryCounter.increment();
    }

    public void incrementStaleRecovery(int count) {
        staleRecoveryCounter.increment(count);
    }
}
