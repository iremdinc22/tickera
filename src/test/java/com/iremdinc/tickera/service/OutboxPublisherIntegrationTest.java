package com.iremdinc.tickera.service;

import com.iremdinc.tickera.entity.OutboxEvent;
import com.iremdinc.tickera.enums.OutboxStatus;
import com.iremdinc.tickera.event.BookingCreatedEvent;
import com.iremdinc.tickera.event.BookingEventPublisher;
import com.iremdinc.tickera.repository.OutboxEventRepository;
import com.iremdinc.tickera.support.PostgresIntegrationTest;
import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import tools.jackson.databind.json.JsonMapper;

import java.time.LocalDateTime;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Outbox state geçişlerini gerçek PostgreSQL üzerinde,
 * Kafka publisher'ı mock'layarak deterministik şekilde
 * doğrular.
 *
 * Scheduler, test süresince çalışmayacak şekilde
 * ertelenir; böylece testler yarış koşullarına bağlı
 * kalmaz.
 */
@SpringBootTest
class OutboxPublisherIntegrationTest
        extends PostgresIntegrationTest {

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private OutboxPublisher outboxPublisher;

    @Autowired
    private OutboxAdminService outboxAdminService;

    @Autowired
    private JsonMapper jsonMapper;

    @MockitoBean
    private BookingEventPublisher bookingEventPublisher;

    @BeforeEach
    void setUp() {

        outboxEventRepository.deleteAll();
    }

    @Test
    void shouldMarkEventPublishedOnlyAfterAcknowledgement() {

        UUID eventId =
                UUID.randomUUID();

        saveEvent(
                newEvent(
                        eventId,
                        1001L,
                        OutboxStatus.PENDING,
                        0,
                        null
                )
        );

        outboxPublisher
                .publishPendingEvents();

        OutboxEvent publishedEvent =
                reload(
                        eventId
                );

        assertEquals(
                OutboxStatus.PUBLISHED,
                publishedEvent.getStatus()
        );

        assertNotNull(
                publishedEvent.getPublishedAt()
        );

        assertNull(
                publishedEvent.getProcessingStartedAt()
        );

        assertNull(
                publishedEvent.getNextRetryAt()
        );

        assertNull(
                publishedEvent.getLastError()
        );

        assertEquals(
                0,
                publishedEvent.getRetryCount()
        );

        verify(
                bookingEventPublisher,
                times(1)
        ).publish(
                any(BookingCreatedEvent.class)
        );
    }

    @Test
    void shouldScheduleRetryWhenPublishFails() {

        UUID eventId =
                UUID.randomUUID();

        saveEvent(
                newEvent(
                        eventId,
                        1002L,
                        OutboxStatus.PENDING,
                        0,
                        null
                )
        );

        doThrow(
                new KafkaException(
                        "broker unavailable"
                )
        ).when(
                bookingEventPublisher
        ).publish(
                any(BookingCreatedEvent.class)
        );

        outboxPublisher
                .publishPendingEvents();

        OutboxEvent failedEvent =
                reload(
                        eventId
                );

        assertEquals(
                OutboxStatus.PENDING,
                failedEvent.getStatus(),
                "Failed publish must not transition to PUBLISHED"
        );

        assertEquals(
                1,
                failedEvent.getRetryCount()
        );

        assertNotNull(
                failedEvent.getNextRetryAt()
        );

        assertTrue(
                failedEvent.getNextRetryAt()
                        .isAfter(
                                LocalDateTime.now()
                                        .minusSeconds(1)
                        )
        );

        assertTrue(
                failedEvent.getNextRetryAt()
                        .isBefore(
                                LocalDateTime.now()
                                        .plusSeconds(5)
                        ),
                "First failure should use the 2 second backoff"
        );

        assertNull(
                failedEvent.getPublishedAt()
        );

        assertNull(
                failedEvent.getProcessingStartedAt()
        );

        assertTrue(
                failedEvent.getLastError()
                        .contains(
                                "KafkaException"
                        )
        );

        assertTrue(
                failedEvent.getLastError()
                        .contains(
                                "broker unavailable"
                        )
        );

        /*
         * Backoff dolmadan event tekrar publish
         * edilmemelidir.
         */
        outboxPublisher
                .publishPendingEvents();

        verify(
                bookingEventPublisher,
                times(1)
        ).publish(
                any(BookingCreatedEvent.class)
        );

        assertEquals(
                1,
                reload(
                        eventId
                ).getRetryCount()
        );
    }

    @Test
    void shouldPublishAfterRetryBecomesEligible() {

        UUID eventId =
                UUID.randomUUID();

        saveEvent(
                newEvent(
                        eventId,
                        1003L,
                        OutboxStatus.PENDING,
                        1,
                        LocalDateTime.now()
                                .minusSeconds(1)
                )
        );

        outboxPublisher
                .publishPendingEvents();

        OutboxEvent publishedEvent =
                reload(
                        eventId
                );

        assertEquals(
                OutboxStatus.PUBLISHED,
                publishedEvent.getStatus()
        );

        assertEquals(
                1,
                publishedEvent.getRetryCount(),
                "Retry history should be preserved"
        );

        assertNotNull(
                publishedEvent.getPublishedAt()
        );

        assertNull(
                publishedEvent.getLastError()
        );
    }

    @Test
    void shouldMoveEventToFailedAfterMaximumRetries() {

        UUID eventId =
                UUID.randomUUID();

        saveEvent(
                newEvent(
                        eventId,
                        1004L,
                        OutboxStatus.PENDING,
                        4,
                        null
                )
        );

        doThrow(
                new KafkaException(
                        "broker unavailable"
                )
        ).when(
                bookingEventPublisher
        ).publish(
                any(BookingCreatedEvent.class)
        );

        outboxPublisher
                .publishPendingEvents();

        OutboxEvent failedEvent =
                reload(
                        eventId
                );

        assertEquals(
                OutboxStatus.FAILED,
                failedEvent.getStatus()
        );

        assertEquals(
                5,
                failedEvent.getRetryCount()
        );

        assertNull(
                failedEvent.getNextRetryAt()
        );

        assertNull(
                failedEvent.getPublishedAt()
        );

        assertNull(
                failedEvent.getProcessingStartedAt()
        );

        assertNotNull(
                failedEvent.getLastError()
        );

        /*
         * FAILED event otomatik olarak
         * tekrar denenmemelidir.
         */
        outboxPublisher
                .publishPendingEvents();

        verify(
                bookingEventPublisher,
                times(1)
        ).publish(
                any(BookingCreatedEvent.class)
        );
    }

    @Test
    void shouldRequeueFailedEventAndPublishAgain() {

        UUID eventId =
                UUID.randomUUID();

        OutboxEvent failedEvent =
                newEvent(
                        eventId,
                        1005L,
                        OutboxStatus.FAILED,
                        5,
                        null
                );

        failedEvent.setLastError(
                "KafkaException: broker unavailable"
        );

        saveEvent(
                failedEvent
        );

        OutboxEvent requeuedEvent =
                outboxAdminService
                        .requeueFailedEvent(
                                eventId
                        );

        assertEquals(
                OutboxStatus.PENDING,
                requeuedEvent.getStatus()
        );

        assertEquals(
                0,
                requeuedEvent.getRetryCount()
        );

        assertNull(
                requeuedEvent.getNextRetryAt()
        );

        assertNull(
                requeuedEvent.getLastError()
        );

        assertNull(
                requeuedEvent.getPublishedAt()
        );

        assertNull(
                requeuedEvent.getProcessingStartedAt()
        );

        outboxPublisher
                .publishPendingEvents();

        assertEquals(
                OutboxStatus.PUBLISHED,
                reload(
                        eventId
                ).getStatus()
        );
    }

    @Test
    void shouldRejectRequeueWhenEventIsNotFailed() {

        UUID eventId =
                UUID.randomUUID();

        saveEvent(
                newEvent(
                        eventId,
                        1006L,
                        OutboxStatus.PENDING,
                        0,
                        null
                )
        );

        assertThrows(
                IllegalStateException.class,
                () -> outboxAdminService
                        .requeueFailedEvent(
                                eventId
                        )
        );
    }

    @Test
    void shouldIsolateFailureWithinBatch() {

        UUID failingEventId =
                UUID.randomUUID();

        UUID succeedingEventId =
                UUID.randomUUID();

        saveEvent(
                newEvent(
                        failingEventId,
                        1007L,
                        OutboxStatus.PENDING,
                        0,
                        null,
                        LocalDateTime.now()
                                .minusSeconds(2)
                )
        );

        saveEvent(
                newEvent(
                        succeedingEventId,
                        1008L,
                        OutboxStatus.PENDING,
                        0,
                        null,
                        LocalDateTime.now()
                                .minusSeconds(1)
                )
        );

        doAnswer(
                invocation -> {

                    BookingCreatedEvent event =
                            invocation.getArgument(
                                    0,
                                    BookingCreatedEvent.class
                            );

                    if (event.bookingId() == 1007L) {

                        throw new KafkaException(
                                "first event failed"
                        );
                    }

                    return null;
                }
        ).when(
                bookingEventPublisher
        ).publish(
                any(BookingCreatedEvent.class)
        );

        outboxPublisher
                .publishPendingEvents();

        OutboxEvent failedEvent =
                reload(
                        failingEventId
                );

        OutboxEvent publishedEvent =
                reload(
                        succeedingEventId
                );

        assertEquals(
                OutboxStatus.PENDING,
                failedEvent.getStatus()
        );

        assertEquals(
                1,
                failedEvent.getRetryCount()
        );

        assertEquals(
                OutboxStatus.PUBLISHED,
                publishedEvent.getStatus(),
                "A failure in one event must not block the rest of the batch"
        );

        assertNotNull(
                publishedEvent.getPublishedAt()
        );

        verify(
                bookingEventPublisher,
                times(2)
        ).publish(
                any(BookingCreatedEvent.class)
        );
    }

    @Test
    void shouldNotOverwriteNewerStateWhenAttemptIsStale() {

        UUID eventId =
                UUID.randomUUID();

        saveEvent(
                newEvent(
                        eventId,
                        1009L,
                        OutboxStatus.PENDING,
                        0,
                        null
                )
        );

        /*
         * Kafka ACK beklenirken event'in stale recovery
         * veya manuel müdahale ile PENDING'e alındığını
         * simüle ediyoruz.
         */
        doAnswer(
                invocation -> {

                    OutboxEvent current =
                            outboxEventRepository
                                    .findById(
                                            eventId
                                    )
                                    .orElseThrow();

                    current.setStatus(
                            OutboxStatus.PENDING
                    );

                    current.setProcessingStartedAt(
                            null
                    );

                    current.setNextRetryAt(
                            LocalDateTime.now()
                                    .plusMinutes(
                                            5
                                    )
                    );

                    outboxEventRepository
                            .saveAndFlush(
                                    current
                            );

                    return null;
                }
        ).when(
                bookingEventPublisher
        ).publish(
                any(BookingCreatedEvent.class)
        );

        outboxPublisher
                .publishPendingEvents();

        OutboxEvent current =
                reload(
                        eventId
                );

        assertEquals(
                OutboxStatus.PENDING,
                current.getStatus(),
                "Outdated publish attempt must not overwrite newer state"
        );

        assertNull(
                current.getPublishedAt()
        );

        assertEquals(
                0,
                current.getRetryCount()
        );

        assertNotNull(
                current.getNextRetryAt()
        );
    }

    private OutboxEvent newEvent(
            UUID eventId,
            long bookingId,
            OutboxStatus status,
            int retryCount,
            LocalDateTime nextRetryAt
    ) {

        return newEvent(
                eventId,
                bookingId,
                status,
                retryCount,
                nextRetryAt,
                LocalDateTime.now()
        );
    }

    private OutboxEvent newEvent(
            UUID eventId,
            long bookingId,
            OutboxStatus status,
            int retryCount,
            LocalDateTime nextRetryAt,
            LocalDateTime createdAt
    ) {

        BookingCreatedEvent event =
                new BookingCreatedEvent(
                        eventId,
                        bookingId,
                        bookingId + 10_000,
                        "TEST-" + bookingId,
                        "irem",
                        "PENDING",
                        LocalDateTime.now()
                );

        return OutboxEvent.builder()
                .id(
                        eventId
                )
                .aggregateType(
                        "BOOKING"
                )
                .aggregateId(
                        String.valueOf(
                                bookingId
                        )
                )
                .eventType(
                        "BOOKING_CREATED"
                )
                .payload(
                        jsonMapper.writeValueAsString(
                                event
                        )
                )
                .status(
                        status
                )
                .retryCount(
                        retryCount
                )
                .nextRetryAt(
                        nextRetryAt
                )
                .createdAt(
                        createdAt
                )
                .build();
    }

    private OutboxEvent saveEvent(
            OutboxEvent event
    ) {

        return outboxEventRepository
                .saveAndFlush(
                        event
                );
    }

    private OutboxEvent reload(
            UUID eventId
    ) {

        return outboxEventRepository
                .findById(
                        eventId
                )
                .orElseThrow();
    }
}
