package com.iremdinc.tickera.service;

import com.iremdinc.tickera.entity.OutboxEvent;
import com.iremdinc.tickera.enums.OutboxStatus;
import com.iremdinc.tickera.event.BookingCreatedEvent;
import com.iremdinc.tickera.event.BookingEventPublisher;
import com.iremdinc.tickera.metrics.OutboxMetrics;
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
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
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
     *
     * Bu süre, tek bir publish denemesinin üst
     * sınırından (Kafka ack timeout + DB işlemleri)
     * belirgin şekilde büyük olmalıdır. Aksi halde
     * stale recovery, hâlâ aktif olan bir denemeyi
     * geri alabilir.
     */
    private static final int PROCESSING_TIMEOUT_MINUTES = 2;

    private final OutboxEventRepository outboxEventRepository;
    private final BookingEventPublisher bookingEventPublisher;
    private final JsonMapper jsonMapper;
    private final PlatformTransactionManager transactionManager;
    private final OutboxMetrics outboxMetrics;

    /**
     * Normal outbox publishing job.
     *
     * Retry zamanı gelmiş PENDING event'leri tek tek
     * claim eder, Kafka'ya publish eder ve ACK bekler.
     *
     * Event'ler tek tek claim edilir. Böylece bir event
     * için beklenen Kafka ACK süresi, batch'teki diğer
     * event'lerin PROCESSING süresini uzatmaz ve stale
     * recovery yanlışlıkla aktif bir event'i geri almaz.
     */
    @Scheduled(
            fixedDelayString = "${tickera.outbox.publish-interval-ms:2000}",
            initialDelayString = "${tickera.outbox.publish-initial-delay-ms:0}"
    )
    public void publishPendingEvents() {

        for (int processed = 0;
             processed < BATCH_SIZE;
             processed++) {

            OutboxEvent claimedEvent;

            try {

                claimedEvent =
                        claimNextRetryableEvent();

            } catch (Exception exception) {

                /*
                 * Claim sırasında DB hatası varsa aynı
                 * hatayı 100 kez tekrarlamak anlamsız.
                 * Bir sonraki scheduled çalışmada tekrar
                 * denenir.
                 */
                log.error(
                        "Failed to claim outbox event for publishing",
                        exception
                );

                return;
            }

            if (claimedEvent == null) {
                return;
            }

            /*
             * Bir event'in hatası diğer event'lerin
             * işlenmesini engellemez.
             */
            publishSingleEvent(
                    claimedEvent
            );
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
     * Kafka publish bekleyen job'dan bağımsız
     * çalışabilmesi için scheduler pool size
     * en az 2 olmalıdır (spring.task.scheduling.pool.size).
     */
    @Scheduled(
            fixedDelayString = "${tickera.outbox.stale-recovery-interval-ms:30000}",
            initialDelayString = "${tickera.outbox.stale-recovery-initial-delay-ms:0}"
    )
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
                         * Bu event'in stale PROCESSING
                         * durumundan kurtarıldığını metric'e yaz.
                         */
                        outboxMetrics.incrementStaleRecovery();

                        /*
                         * Çok fazla stale recovery yaşandıysa
                         * artık otomatik retry yapılmaz.
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

                        /*
                         * Yeni bir retry gerçekten schedule edildi.
                         */
                        outboxMetrics.incrementRetry();

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
     * Tek bir event'i publish eder.
     *
     * Publish hatası event'in kendi state'ine yazılır;
     * çağıran döngü bir sonraki event ile devam eder.
     */
    private void publishSingleEvent(
            OutboxEvent claimedEvent
    ) {

        try {

            BookingCreatedEvent event =
                    deserializeEvent(
                            claimedEvent
                    );

            /*
             * Bu çağrı Kafka broker ACK'ini bekler.
             * DB transaction'ı ve row lock bu noktada
             * tutulmaz.
             */
            bookingEventPublisher.publish(
                    event
            );

            boolean transitioned =
                    markAsPublished(
                            claimedEvent
                    );

            if (!transitioned) {

                /*
                 * Event artık bu denemeye ait değil
                 * (stale recovery veya manuel müdahale).
                 * Yeni state'in üzerine yazılmadı.
                 */
                return;
            }

            outboxMetrics.incrementPublished();

            log.info(
                    "Outbox event published successfully: eventId={}, aggregateId={}, type={}",
                    claimedEvent.getId(),
                    claimedEvent.getAggregateId(),
                    claimedEvent.getEventType()
            );

        } catch (Exception exception) {

            try {

                FailureOutcome outcome =
                        handlePublishFailure(
                                claimedEvent,
                                exception
                        );

                if (outcome == FailureOutcome.IGNORED) {

                    log.warn(
                            "Outbox publish failure ignored because attempt is no longer current: eventId={}",
                            claimedEvent.getId()
                    );
                }

            } catch (Exception handlerException) {

                /*
                 * Hata state'i yazılamadıysa event
                 * PROCESSING olarak kalır. Stale recovery
                 * job'u bu event'i daha sonra kurtarır.
                 */
                log.error(
                        "Failed to persist outbox publish failure; event will be recovered as stale: eventId={}",
                        claimedEvent.getId(),
                        handlerException
                );
            }
        }
    }

    /**
     * Retry zamanı gelmiş ilk PENDING event'i
     * PostgreSQL transaction içerisinde claim eder.
     *
     * PENDING -> PROCESSING
     *
     * Row lock yalnızca claim sırasında tutulur.
     * Kafka publish DB transaction dışında yapılır.
     */
    private OutboxEvent claimNextRetryableEvent() {

        TransactionTemplate transactionTemplate =
                new TransactionTemplate(
                        transactionManager
                );

        List<OutboxEvent> claimedEvents =
                transactionTemplate.execute(
                        status -> {

                            /*
                             * PostgreSQL timestamp precision
                             * mikrosaniyedir. Claim anındaki
                             * değeri de mikrosaniyeye yuvarlayarak
                             * attempt karşılaştırmasını deterministik
                             * yapıyoruz.
                             */
                            LocalDateTime now =
                                    LocalDateTime.now()
                                            .truncatedTo(
                                                    ChronoUnit.MICROS
                                            );

                            List<OutboxEvent> retryableEvents =
                                    outboxEventRepository
                                            .findRetryableEventsForUpdate(
                                                    OutboxStatus.PENDING,
                                                    now,
                                                    PageRequest.of(
                                                            0,
                                                            1
                                                    )
                                            );

                            if (retryableEvents.isEmpty()) {
                                return Collections.emptyList();
                            }

                            OutboxEvent event =
                                    retryableEvents.get(0);

                            event.setStatus(
                                    OutboxStatus.PROCESSING
                            );

                            event.setProcessingStartedAt(
                                    now
                            );

                            event.setNextRetryAt(
                                    null
                            );

                            outboxEventRepository
                                    .saveAndFlush(
                                            event
                                    );

                            return List.of(event);
                        }
                );

        if (claimedEvents == null
                || claimedEvents.isEmpty()) {

            return null;
        }

        return claimedEvents.get(0);
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
     *
     * Yalnızca bu publish denemesi hâlâ güncel ise
     * (status PROCESSING ve processingStartedAt aynı)
     * state değiştirilir.
     *
     * @return state gerçekten değiştirildiyse true
     */
    private boolean markAsPublished(
            OutboxEvent claimedEvent
    ) {

        TransactionTemplate transactionTemplate =
                new TransactionTemplate(
                        transactionManager
                );

        Boolean transitioned =
                transactionTemplate.execute(
                        status -> {

                            OutboxEvent event =
                                    findCurrentAttempt(
                                            claimedEvent
                                    );

                            if (event == null) {
                                return false;
                            }

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

                            return true;
                        }
                );

        return Boolean.TRUE.equals(
                transitioned
        );
    }

    /**
     * Kafka publish başarısız.
     *
     * Yalnızca bu publish denemesi hâlâ güncel ise
     * retry/FAILED state'i yazılır. Aksi halde daha
     * yeni bir state'in üzerine yazılmaz.
     */
    private FailureOutcome handlePublishFailure(
            OutboxEvent claimedEvent,
            Exception exception
    ) {

        TransactionTemplate transactionTemplate =
                new TransactionTemplate(
                        transactionManager
                );

        FailureOutcome outcome =
                transactionTemplate.execute(
                        status -> {

                            OutboxEvent event =
                                    findCurrentAttempt(
                                            claimedEvent
                                    );

                            if (event == null) {
                                return FailureOutcome.IGNORED;
                            }

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

                                return FailureOutcome.PERMANENTLY_FAILED;
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

                            return FailureOutcome.RETRY_SCHEDULED;
                        }
                );

        if (outcome == FailureOutcome.RETRY_SCHEDULED) {

            /*
             * Retry gerçekten schedule edildiği
             * için counter artırılır.
             */
            outboxMetrics.incrementRetry();
        }

        return outcome == null
                ? FailureOutcome.IGNORED
                : outcome;
    }

    /**
     * Event'in hâlâ claim edildiği denemeye ait olup
     * olmadığını kontrol eder.
     *
     * Stale recovery veya manuel bir müdahale event'i
     * başka bir state'e taşıdıysa eski publisher
     * denemesi bu state'i ezmemelidir.
     */
    private OutboxEvent findCurrentAttempt(
            OutboxEvent claimedEvent
    ) {

        OutboxEvent event =
                outboxEventRepository
                        .findByIdForUpdate(
                                claimedEvent.getId()
                        )
                        .orElse(null);

        if (event == null) {

            log.warn(
                    "Outbox event disappeared while publishing: eventId={}",
                    claimedEvent.getId()
            );

            return null;
        }

        boolean sameAttempt =
                event.getStatus()
                        == OutboxStatus.PROCESSING
                        && Objects.equals(
                        event.getProcessingStartedAt(),
                        claimedEvent.getProcessingStartedAt()
                );

        if (!sameAttempt) {

            log.warn(
                    "Outbox attempt is no longer current; state change skipped: eventId={}, expectedStatus={}, actualStatus={}, expectedProcessingStartedAt={}, actualProcessingStartedAt={}",
                    claimedEvent.getId(),
                    OutboxStatus.PROCESSING,
                    event.getStatus(),
                    claimedEvent.getProcessingStartedAt(),
                    event.getProcessingStartedAt()
            );

            return null;
        }

        return event;
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

    private enum FailureOutcome {
        IGNORED,
        RETRY_SCHEDULED,
        PERMANENTLY_FAILED
    }
}
