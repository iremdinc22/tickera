package com.iremdinc.tickera.service;

import com.iremdinc.tickera.entity.OutboxEvent;
import com.iremdinc.tickera.enums.OutboxStatus;
import com.iremdinc.tickera.event.BookingCreatedEvent;
import com.iremdinc.tickera.event.BookingEventPublisher;
import com.iremdinc.tickera.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class OutboxPublisher {

    private static final int BATCH_SIZE = 100;

    private static final int MAX_RETRY_ATTEMPTS = 5;

    /*
     * Bir event PROCESSING state'inde bu süreden
     * daha uzun kalırsa stale kabul edilir.
     */
    private static final int PROCESSING_TIMEOUT_MINUTES = 2;

    private final OutboxEventRepository outboxEventRepository;
    private final BookingEventPublisher bookingEventPublisher;
    private final JsonMapper jsonMapper;
    private final PlatformTransactionManager transactionManager;

    /**
     * Normal outbox publishing job.
     *
     * Retry zamanı gelmiş PENDING event'leri claim eder
     * ve Kafka'ya publish etmeye çalışır.
     */
    @Scheduled(fixedDelay = 2000)
    public void publishPendingEvents() {

        List<OutboxEvent> claimedEvents =
                claimRetryableEvents();

        for (OutboxEvent outboxEvent : claimedEvents) {

            try {

                BookingCreatedEvent event =
                        deserializeEvent(
                                outboxEvent
                        );

                bookingEventPublisher.publish(
                        event
                );

                markAsPublished(
                        outboxEvent
                );

                log.info(
                        "Outbox event published successfully: eventId={}, aggregateId={}, type={}",
                        outboxEvent.getId(),
                        outboxEvent.getAggregateId(),
                        outboxEvent.getEventType()
                );

            } catch (Exception exception) {

                handlePublishFailure(
                        outboxEvent,
                        exception
                );
            }
        }
    }

    /**
     * Stale PROCESSING recovery job.
     *
     * Örneğin:
     *
     * PENDING
     *   ↓
     * PROCESSING
     *   ↓
     * application crash
     *
     * durumunda event PROCESSING state'inde
     * sonsuza kadar kalmasın diye çalışır.
     *
     * Her 30 saniyede bir kontrol ediyoruz.
     */
    @Scheduled(fixedDelay = 30000)
    public void recoverStaleProcessingEvents() {

        TransactionTemplate transactionTemplate =
                new TransactionTemplate(
                        transactionManager
                );

        transactionTemplate.executeWithoutResult(
                status -> {

                    LocalDateTime now =
                            LocalDateTime.now();

                    LocalDateTime cutoff =
                            now.minusMinutes(
                                    PROCESSING_TIMEOUT_MINUTES
                            );

                    List<OutboxEvent> staleEvents =
                            outboxEventRepository
                                    .findStaleProcessingEventsForUpdate(
                                            OutboxStatus.PROCESSING,
                                            cutoff,
                                            PageRequest.of(
                                                    0,
                                                    BATCH_SIZE
                                            )
                                    );

                    if (staleEvents.isEmpty()) {
                        return;
                    }

                    for (OutboxEvent event : staleEvents) {

                        int currentRetryCount =
                                event.getRetryCount() == null
                                        ? 0
                                        : event.getRetryCount();

                        int newRetryCount =
                                currentRetryCount + 1;

                        event.setRetryCount(
                                newRetryCount
                        );

                        event.setProcessingStartedAt(
                                null
                        );

                        event.setPublishedAt(
                                null
                        );

                        event.setLastError(
                                "Recovered stale PROCESSING event"
                        );

                        /*
                         * Çok fazla stale recovery yaşandıysa
                         * event artık otomatik işlenmesin.
                         */
                        if (newRetryCount
                                >= MAX_RETRY_ATTEMPTS) {

                            event.setStatus(
                                    OutboxStatus.FAILED
                            );

                            event.setNextRetryAt(
                                    null
                            );

                            log.error(
                                    "Stale outbox event moved to FAILED: eventId={}, aggregateId={}, retryCount={}",
                                    event.getId(),
                                    event.getAggregateId(),
                                    newRetryCount
                            );

                            continue;
                        }

                        /*
                         * Event tekrar publish edilebilsin.
                         *
                         * Backoff uyguluyoruz; hemen Kafka'ya
                         * saldırmak yerine kısa süre bekleyecek.
                         */
                        event.setStatus(
                                OutboxStatus.PENDING
                        );

                        long backoffSeconds =
                                calculateBackoffSeconds(
                                        newRetryCount
                                );

                        LocalDateTime nextRetryAt =
                                now.plusSeconds(
                                        backoffSeconds
                                );

                        event.setNextRetryAt(
                                nextRetryAt
                        );

                        log.warn(
                                "Recovered stale PROCESSING outbox event: eventId={}, aggregateId={}, retryCount={}, nextRetryAt={}",
                                event.getId(),
                                event.getAggregateId(),
                                newRetryCount,
                                nextRetryAt
                        );
                    }

                    outboxEventRepository
                            .saveAllAndFlush(
                                    staleEvents
                            );
                }
        );
    }

    /**
     * Retry zamanı gelmiş PENDING event'leri
     * PostgreSQL transaction içerisinde claim eder.
     *
     * PENDING -> PROCESSING
     */
    private List<OutboxEvent> claimRetryableEvents() {

        TransactionTemplate transactionTemplate =
                new TransactionTemplate(
                        transactionManager
                );

        List<OutboxEvent> claimedEvents =
                transactionTemplate.execute(
                        status -> {

                            LocalDateTime now =
                                    LocalDateTime.now();

                            List<OutboxEvent> retryableEvents =
                                    outboxEventRepository
                                            .findRetryableEventsForUpdate(
                                                    OutboxStatus.PENDING,
                                                    now,
                                                    PageRequest.of(
                                                            0,
                                                            BATCH_SIZE
                                                    )
                                            );

                            if (retryableEvents.isEmpty()) {
                                return Collections.emptyList();
                            }

                            for (OutboxEvent event : retryableEvents) {

                                event.setStatus(
                                        OutboxStatus.PROCESSING
                                );

                                event.setProcessingStartedAt(
                                        now
                                );

                                event.setNextRetryAt(
                                        null
                                );
                            }

                            outboxEventRepository
                                    .saveAllAndFlush(
                                            retryableEvents
                                    );

                            /*
                             * Commit sonrası row lock bırakılır.
                             *
                             * Kafka publish DB transaction dışında
                             * yapılır.
                             */
                            return retryableEvents;
                        }
                );

        if (claimedEvents == null) {
            return Collections.emptyList();
        }

        return claimedEvents;
    }

    /**
     * JSON payload -> BookingCreatedEvent
     */
    private BookingCreatedEvent deserializeEvent(
            OutboxEvent outboxEvent
    ) {

        try {

            return jsonMapper.readValue(
                    outboxEvent.getPayload(),
                    BookingCreatedEvent.class
            );

        } catch (JacksonException exception) {

            throw new IllegalStateException(
                    "Failed to deserialize outbox event",
                    exception
            );
        }
    }

    /**
     * Kafka publish başarılı:
     *
     * PROCESSING -> PUBLISHED
     */
    private void markAsPublished(
            OutboxEvent outboxEvent
    ) {

        TransactionTemplate transactionTemplate =
                new TransactionTemplate(
                        transactionManager
                );

        transactionTemplate.executeWithoutResult(
                status -> {

                    OutboxEvent event =
                            findOutboxEvent(
                                    outboxEvent.getId()
                            );

                    event.setStatus(
                            OutboxStatus.PUBLISHED
                    );

                    event.setPublishedAt(
                            LocalDateTime.now()
                    );

                    event.setProcessingStartedAt(
                            null
                    );

                    event.setNextRetryAt(
                            null
                    );

                    event.setLastError(
                            null
                    );

                    outboxEventRepository
                            .saveAndFlush(
                                    event
                            );
                }
        );
    }

    /**
     * Kafka publish başarısız.
     */
    private void handlePublishFailure(
            OutboxEvent outboxEvent,
            Exception exception
    ) {

        TransactionTemplate transactionTemplate =
                new TransactionTemplate(
                        transactionManager
                );

        transactionTemplate.executeWithoutResult(
                status -> {

                    OutboxEvent event =
                            findOutboxEvent(
                                    outboxEvent.getId()
                            );

                    int currentRetryCount =
                            event.getRetryCount() == null
                                    ? 0
                                    : event.getRetryCount();

                    int newRetryCount =
                            currentRetryCount + 1;

                    event.setRetryCount(
                            newRetryCount
                    );

                    event.setLastError(
                            buildErrorMessage(
                                    exception
                            )
                    );

                    event.setPublishedAt(
                            null
                    );

                    event.setProcessingStartedAt(
                            null
                    );

                    /*
                     * Maksimum retry sayısına ulaşıldı.
                     */
                    if (newRetryCount
                            >= MAX_RETRY_ATTEMPTS) {

                        event.setStatus(
                                OutboxStatus.FAILED
                        );

                        event.setNextRetryAt(
                                null
                        );

                        outboxEventRepository
                                .saveAndFlush(
                                        event
                                );

                        log.error(
                                "Outbox event permanently failed after {} attempts: eventId={}, aggregateId={}, type={}, error={}",
                                newRetryCount,
                                event.getId(),
                                event.getAggregateId(),
                                event.getEventType(),
                                event.getLastError()
                        );

                        return;
                    }

                    /*
                     * PROCESSING -> PENDING
                     */
                    event.setStatus(
                            OutboxStatus.PENDING
                    );

                    long backoffSeconds =
                            calculateBackoffSeconds(
                                    newRetryCount
                            );

                    LocalDateTime nextRetryAt =
                            LocalDateTime.now()
                                    .plusSeconds(
                                            backoffSeconds
                                    );

                    event.setNextRetryAt(
                            nextRetryAt
                    );

                    outboxEventRepository
                            .saveAndFlush(
                                    event
                            );

                    log.warn(
                            "Outbox event publish failed. Retry scheduled: eventId={}, aggregateId={}, attempt={}, nextRetryAt={}, error={}",
                            event.getId(),
                            event.getAggregateId(),
                            newRetryCount,
                            nextRetryAt,
                            event.getLastError()
                    );
                }
        );
    }

    /**
     * Retry backoff süreleri.
     *
     * 1. failure -> 2 saniye
     * 2. failure -> 5 saniye
     * 3. failure -> 10 saniye
     * 4. failure -> 30 saniye
     * 5. failure -> FAILED
     */
    private long calculateBackoffSeconds(
            int retryCount
    ) {

        return switch (retryCount) {
            case 1 -> 2;
            case 2 -> 5;
            case 3 -> 10;
            case 4 -> 30;
            default -> 30;
        };
    }

    private OutboxEvent findOutboxEvent(
            UUID eventId
    ) {

        return outboxEventRepository
                .findById(
                        eventId
                )
                .orElseThrow(() ->
                        new IllegalStateException(
                                "Outbox event not found: "
                                        + eventId
                        )
                );
    }

    private String buildErrorMessage(
            Exception exception
    ) {

        String message =
                exception.getMessage();

        if (message == null
                || message.isBlank()) {

            return exception
                    .getClass()
                    .getSimpleName();
        }

        return exception
                .getClass()
                .getSimpleName()
                + ": "
                + message;
    }
}