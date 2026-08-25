package com.iremdinc.tickera.service;

import com.iremdinc.tickera.entity.OutboxEvent;
import com.iremdinc.tickera.enums.OutboxStatus;
import com.iremdinc.tickera.repository.OutboxEventRepository;
import com.iremdinc.tickera.support.PostgresIntegrationTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.LocalDateTime;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class OutboxStaleRecoveryIntegrationTest
        extends PostgresIntegrationTest {

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private OutboxPublisher outboxPublisher;

    @BeforeEach
    void setUp() {
        outboxEventRepository.deleteAll();
    }

    @Test
    void shouldRecoverStaleProcessingEvent() {

        UUID eventId =
                UUID.randomUUID();

        OutboxEvent staleEvent =
                OutboxEvent.builder()
                        .id(eventId)
                        .aggregateType("BOOKING")
                        .aggregateId("999001")
                        .eventType("BOOKING_CREATED")
                        .payload("""
                                {
                                  "eventId": "%s",
                                  "bookingId": 999001,
                                  "seatId": 999002,
                                  "seatNumber": "TEST-A1",
                                  "userId": "irem",
                                  "bookingStatus": "PENDING",
                                  "createdAt": "%s"
                                }
                                """.formatted(
                                eventId,
                                LocalDateTime.now()
                        ))
                        .status(
                                OutboxStatus.PROCESSING
                        )
                        .retryCount(
                                0
                        )
                        .processingStartedAt(
                                LocalDateTime.now()
                                        .minusMinutes(5)
                        )
                        .createdAt(
                                LocalDateTime.now()
                                        .minusMinutes(5)
                        )
                        .build();

        outboxEventRepository.saveAndFlush(
                staleEvent
        );

        /*
         * Stale PROCESSING recovery job'unu
         * doğrudan tetikliyoruz.
         */
        outboxPublisher
                .recoverStaleProcessingEvents();

        OutboxEvent recoveredEvent =
                outboxEventRepository
                        .findById(eventId)
                        .orElseThrow();

        assertEquals(
                OutboxStatus.PENDING,
                recoveredEvent.getStatus(),
                "Stale PROCESSING event should return to PENDING"
        );

        assertEquals(
                1,
                recoveredEvent.getRetryCount(),
                "Stale recovery should increment retry count"
        );

        assertNull(
                recoveredEvent.getProcessingStartedAt(),
                "processingStartedAt should be cleared after recovery"
        );

        assertNotNull(
                recoveredEvent.getNextRetryAt(),
                "Recovered event should have a next retry time"
        );

        assertEquals(
                "Recovered stale PROCESSING event",
                recoveredEvent.getLastError()
        );

        assertNull(
                recoveredEvent.getPublishedAt()
        );
    }

    @Test
    void shouldNotRecoverRecentlyProcessingEvent() {

        UUID eventId =
                UUID.randomUUID();

        LocalDateTime processingStartedAt =
                LocalDateTime.now()
                        .minusSeconds(30);

        OutboxEvent activeEvent =
                OutboxEvent.builder()
                        .id(eventId)
                        .aggregateType("BOOKING")
                        .aggregateId("999002")
                        .eventType("BOOKING_CREATED")
                        .payload("{}")
                        .status(
                                OutboxStatus.PROCESSING
                        )
                        .retryCount(
                                0
                        )
                        .processingStartedAt(
                                processingStartedAt
                        )
                        .createdAt(
                                LocalDateTime.now()
                        )
                        .build();

        outboxEventRepository.saveAndFlush(
                activeEvent
        );

        outboxPublisher
                .recoverStaleProcessingEvents();

        OutboxEvent result =
                outboxEventRepository
                        .findById(eventId)
                        .orElseThrow();

        assertEquals(
                OutboxStatus.PROCESSING,
                result.getStatus(),
                "Recently claimed event must not be recovered"
        );

        assertEquals(
                0,
                result.getRetryCount()
        );

        assertEquals(
                processingStartedAt,
                result.getProcessingStartedAt()
        );

        assertNull(
                result.getNextRetryAt()
        );
    }

    @Test
    void shouldMoveRepeatedlyStaleEventToFailed() {

        UUID eventId =
                UUID.randomUUID();

        OutboxEvent staleEvent =
                OutboxEvent.builder()
                        .id(eventId)
                        .aggregateType("BOOKING")
                        .aggregateId("999003")
                        .eventType("BOOKING_CREATED")
                        .payload("{}")
                        .status(
                                OutboxStatus.PROCESSING
                        )
                        .retryCount(
                                4
                        )
                        .processingStartedAt(
                                LocalDateTime.now()
                                        .minusMinutes(5)
                        )
                        .createdAt(
                                LocalDateTime.now()
                                        .minusMinutes(5)
                        )
                        .build();

        outboxEventRepository.saveAndFlush(
                staleEvent
        );

        outboxPublisher
                .recoverStaleProcessingEvents();

        OutboxEvent failedEvent =
                outboxEventRepository
                        .findById(eventId)
                        .orElseThrow();

        assertEquals(
                OutboxStatus.FAILED,
                failedEvent.getStatus(),
                "Fifth stale recovery should move event to FAILED"
        );

        assertEquals(
                5,
                failedEvent.getRetryCount()
        );

        assertNull(
                failedEvent.getProcessingStartedAt()
        );

        assertNull(
                failedEvent.getNextRetryAt()
        );

        assertEquals(
                "Recovered stale PROCESSING event",
                failedEvent.getLastError()
        );
    }
}