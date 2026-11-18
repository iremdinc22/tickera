package com.iremdinc.tickera.metrics;

import com.iremdinc.tickera.entity.OutboxEvent;
import com.iremdinc.tickera.enums.OutboxStatus;
import com.iremdinc.tickera.repository.OutboxEventRepository;
import com.iremdinc.tickera.service.OutboxPublisher;
import com.iremdinc.tickera.support.PostgresIntegrationTest;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;

/**
 * Stale recovery metric'lerinin yalnızca PostgreSQL
 * transaction'ı commit edildikten sonra artırıldığını
 * doğrular.
 */
@SpringBootTest
class OutboxMetricsIntegrationTest
        extends PostgresIntegrationTest {

    private static final String STALE_RECOVERY_METRIC =
            "tickera.outbox.stale.recovery";

    private static final String RETRY_METRIC =
            "tickera.outbox.retry";

    @Autowired
    private OutboxPublisher outboxPublisher;

    @Autowired
    private MeterRegistry meterRegistry;

    @MockitoSpyBean
    private OutboxEventRepository outboxEventRepository;

    @BeforeEach
    void setUp() {

        outboxEventRepository.deleteAll();
    }

    @Test
    void shouldIncrementMetricsForCommittedStaleRecovery() {

        outboxEventRepository.saveAndFlush(
                staleEvent()
        );

        double staleRecoveryBefore =
                counterValue(
                        STALE_RECOVERY_METRIC
                );

        double retryBefore =
                counterValue(
                        RETRY_METRIC
                );

        outboxPublisher
                .recoverStaleProcessingEvents();

        assertEquals(
                staleRecoveryBefore + 1,
                counterValue(
                        STALE_RECOVERY_METRIC
                ),
                "Stale recovery counter should increase after commit"
        );

        assertEquals(
                retryBefore + 1,
                counterValue(
                        RETRY_METRIC
                ),
                "Retry counter should increase when recovery schedules a retry"
        );
    }

    @Test
    void shouldNotIncrementMetricsWhenRecoveryTransactionRollsBack() {

        UUID eventId =
                UUID.randomUUID();

        outboxEventRepository.saveAndFlush(
                staleEvent(
                        eventId
                )
        );

        double staleRecoveryBefore =
                counterValue(
                        STALE_RECOVERY_METRIC
                );

        double retryBefore =
                counterValue(
                        RETRY_METRIC
                );

        doThrow(
                new IllegalStateException(
                        "Simulated persistence failure"
                )
        ).when(
                outboxEventRepository
        ).saveAllAndFlush(
                any()
        );

        assertThrows(
                IllegalStateException.class,
                outboxPublisher::recoverStaleProcessingEvents
        );

        assertEquals(
                staleRecoveryBefore,
                counterValue(
                        STALE_RECOVERY_METRIC
                ),
                "Rolled back recovery must not increment the stale recovery counter"
        );

        assertEquals(
                retryBefore,
                counterValue(
                        RETRY_METRIC
                ),
                "Rolled back recovery must not increment the retry counter"
        );

        assertEquals(
                OutboxStatus.PROCESSING,
                outboxEventRepository
                        .findById(
                                eventId
                        )
                        .orElseThrow()
                        .getStatus(),
                "Rolled back transaction must leave the event unchanged"
        );
    }

    private OutboxEvent staleEvent() {

        return staleEvent(
                UUID.randomUUID()
        );
    }

    private OutboxEvent staleEvent(
            UUID eventId
    ) {

        return OutboxEvent.builder()
                .id(eventId)
                .aggregateType("BOOKING")
                .aggregateId("999900")
                .eventType("BOOKING_CREATED")
                .payload("{}")
                .status(
                        OutboxStatus.PROCESSING
                )
                .retryCount(
                        0
                )
                .processingStartedAt(
                        LocalDateTime.now()
                                .minusMinutes(
                                        5
                                )
                                .truncatedTo(
                                        ChronoUnit.MICROS
                                )
                )
                .createdAt(
                        LocalDateTime.now()
                                .minusMinutes(
                                        5
                                )
                )
                .build();
    }

    private double counterValue(
            String metricName
    ) {

        return meterRegistry
                .get(
                        metricName
                )
                .counter()
                .count();
    }
}
