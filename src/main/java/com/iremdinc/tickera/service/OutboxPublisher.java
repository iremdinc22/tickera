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

@Service
@RequiredArgsConstructor
@Slf4j
public class OutboxPublisher {

    private static final int BATCH_SIZE = 100;

    private final OutboxEventRepository outboxEventRepository;
    private final BookingEventPublisher bookingEventPublisher;
    private final JsonMapper jsonMapper;
    private final PlatformTransactionManager transactionManager;

    @Scheduled(fixedDelay = 2000)
    public void publishPendingEvents() {

        /*
         * İlk aşamada PENDING event'leri DB transaction
         * içerisinde claim ediyoruz.
         *
         * Claim edilen event:
         *
         * PENDING -> PROCESSING
         */
        List<OutboxEvent> claimedEvents =
                claimPendingEvents();

        for (OutboxEvent outboxEvent : claimedEvents) {

            try {

                BookingCreatedEvent event =
                        deserializeEvent(
                                outboxEvent
                        );

                /*
                 * KafkaTemplate.send(...).join()
                 * kullanıldığı için Kafka ACK gelene
                 * kadar bu method tamamlanmaz.
                 */
                bookingEventPublisher.publish(
                        event
                );

                /*
                 * Kafka gerçekten başarılıysa:
                 *
                 * PROCESSING -> PUBLISHED
                 */
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

                /*
                 * Serialization veya Kafka publish
                 * başarısız oldu.
                 *
                 * Event tekrar retry edilebilsin diye:
                 *
                 * PROCESSING -> PENDING
                 */
                markAsPending(
                        outboxEvent
                );

                log.warn(
                        "Failed to publish outbox event: eventId={}, aggregateId={}, type={}",
                        outboxEvent.getId(),
                        outboxEvent.getAggregateId(),
                        outboxEvent.getEventType(),
                        exception
                );
            }
        }
    }

    /**
     * PENDING event'leri ayrı bir PostgreSQL
     * transaction içerisinde claim eder.
     *
     * PESSIMISTIC_WRITE lock sayesinde concurrent
     * publisher'ların aynı kayıtları aynı anda
     * claim etmesini engellemeye çalışıyoruz.
     */
    private List<OutboxEvent> claimPendingEvents() {

        TransactionTemplate transactionTemplate =
                new TransactionTemplate(
                        transactionManager
                );

        List<OutboxEvent> claimedEvents =
                transactionTemplate.execute(
                        status -> {

                            List<OutboxEvent> pendingEvents =
                                    outboxEventRepository
                                            .findPendingEventsForUpdate(
                                                    OutboxStatus.PENDING,
                                                    PageRequest.of(
                                                            0,
                                                            BATCH_SIZE
                                                    )
                                            );

                            if (pendingEvents.isEmpty()) {

                                return Collections.emptyList();
                            }

                            for (OutboxEvent event : pendingEvents) {

                                event.setStatus(
                                        OutboxStatus.PROCESSING
                                );
                            }

                            /*
                             * PROCESSING state DB'ye yazılır.
                             *
                             * Transaction tamamlandığında
                             * row lock bırakılır.
                             */
                            outboxEventRepository
                                    .saveAllAndFlush(
                                            pendingEvents
                                    );

                            return pendingEvents;
                        }
                );

        if (claimedEvents == null) {
            return Collections.emptyList();
        }

        return claimedEvents;
    }

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
     * Kafka publish başarılı olduğunda event
     * tamamlanmış olarak işaretlenir.
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
                            outboxEventRepository
                                    .findById(
                                            outboxEvent.getId()
                                    )
                                    .orElseThrow(() ->
                                            new IllegalStateException(
                                                    "Outbox event not found: "
                                                            + outboxEvent.getId()
                                            )
                                    );

                    event.setStatus(
                            OutboxStatus.PUBLISHED
                    );

                    event.setPublishedAt(
                            LocalDateTime.now()
                    );

                    outboxEventRepository.saveAndFlush(
                            event
                    );
                }
        );
    }

    /**
     * Kafka publish başarısız olduğunda event
     * yeniden retry edilebilir duruma getirilir.
     */
    private void markAsPending(
            OutboxEvent outboxEvent
    ) {

        TransactionTemplate transactionTemplate =
                new TransactionTemplate(
                        transactionManager
                );

        transactionTemplate.executeWithoutResult(
                status -> {

                    OutboxEvent event =
                            outboxEventRepository
                                    .findById(
                                            outboxEvent.getId()
                                    )
                                    .orElseThrow(() ->
                                            new IllegalStateException(
                                                    "Outbox event not found: "
                                                            + outboxEvent.getId()
                                            )
                                    );

                    event.setStatus(
                            OutboxStatus.PENDING
                    );

                    event.setPublishedAt(
                            null
                    );

                    outboxEventRepository.saveAndFlush(
                            event
                    );
                }
        );
    }
}