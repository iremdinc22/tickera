package com.iremdinc.tickera.service;

import com.iremdinc.tickera.event.BookingCreatedEvent;
import com.iremdinc.tickera.event.BookingEventPublisher;
import com.iremdinc.tickera.repository.ProcessedEventRepository;
import com.iremdinc.tickera.support.KafkaIntegrationTest;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Kafka consumer idempotency davranışını izole bir Kafka
 * Testcontainer'ı üzerinde doğrular.
 *
 * Test, listener'ın partition assignment'ını beklemeden
 * mesaj göndermez. Duplicate kontrolü sabit bir sleep
 * yerine, aynı partition'a gönderilen bir sentinel event'in
 * işlenmesiyle senkronize edilir.
 */
@SpringBootTest
class IdempotentConsumerIntegrationTest
        extends KafkaIntegrationTest {

    private static final long TIMEOUT_SECONDS = 30;

    @DynamicPropertySource
    static void enableKafkaListener(
            DynamicPropertyRegistry registry
    ) {

        /*
         * Destek sınıfı listener'ı varsayılan olarak
         * kapatır. Bu test gerçek consumer davranışını
         * doğruladığı için burada açıkça etkinleştirilir.
         */
        registry.add(
                "spring.kafka.listener.auto-startup",
                () -> "true"
        );
    }

    @Autowired
    private BookingEventPublisher bookingEventPublisher;

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @Autowired
    private KafkaListenerEndpointRegistry listenerEndpointRegistry;

    @BeforeEach
    void setUp() {

        processedEventRepository.deleteAll();

        awaitListenerAssignment();
    }

    @Test
    void shouldProcessDuplicateBookingCreatedEventOnlyOnce()
            throws InterruptedException {

        UUID eventId =
                UUID.randomUUID();

        BookingCreatedEvent event =
                new BookingCreatedEvent(
                        eventId,
                        999001L,
                        999002L,
                        "TEST-A1",
                        "irem",
                        "PENDING",
                        LocalDateTime.now()
                );

        /*
         * First delivery.
         */
        bookingEventPublisher.publish(
                event
        );

        waitUntilProcessed(
                eventId
        );

        assertTrue(
                processedEventRepository
                        .existsById(eventId),
                "First delivery should be processed"
        );

        assertEquals(
                1,
                processedEventRepository.count(),
                "Exactly one processed event should exist"
        );

        /*
         * Duplicate delivery with exactly the same eventId.
         */
        bookingEventPublisher.publish(
                event
        );

        /*
         * Duplicate'in işlendiğinden emin olmak için aynı
         * partition'a bir sentinel event gönderiyoruz.
         * Sentinel işlendiğinde duplicate de consumer
         * tarafından görülmüş olur.
         */
        UUID sentinelEventId =
                UUID.randomUUID();

        BookingCreatedEvent sentinelEvent =
                new BookingCreatedEvent(
                        sentinelEventId,
                        999003L,
                        999004L,
                        "TEST-A2",
                        "irem",
                        "PENDING",
                        LocalDateTime.now()
                );

        bookingEventPublisher.publish(
                sentinelEvent
        );

        waitUntilProcessed(
                sentinelEventId
        );

        assertEquals(
                2,
                processedEventRepository.count(),
                "Duplicate delivery must not create an extra processed event"
        );

        assertTrue(
                processedEventRepository
                        .existsById(eventId),
                "Original processed event record must remain"
        );

        assertTrue(
                processedEventRepository
                        .existsById(sentinelEventId),
                "Sentinel event must be processed"
        );
    }

    private void awaitListenerAssignment() {

        long deadline =
                System.nanoTime()
                        + TimeUnit.SECONDS.toNanos(
                                TIMEOUT_SECONDS
                        );

        while (System.nanoTime() < deadline) {

            boolean assigned =
                    listenerEndpointRegistry
                            .getListenerContainers()
                            .stream()
                            .filter(
                                    MessageListenerContainer::isRunning
                            )
                            .map(
                                    MessageListenerContainer::getAssignedPartitions
                            )
                            .anyMatch(
                                    partitions ->
                                            partitions != null
                                                    && !partitions.isEmpty()
                            );

            if (assigned) {
                return;
            }

            try {

                TimeUnit.MILLISECONDS.sleep(
                        100
                );

            } catch (InterruptedException exception) {

                Thread.currentThread()
                        .interrupt();

                fail(
                        "Interrupted while waiting for Kafka listener assignment"
                );

                return;
            }
        }

        fail(
                "Kafka listener was not assigned within "
                        + TIMEOUT_SECONDS
                        + " seconds"
        );
    }

    private void waitUntilProcessed(
            UUID eventId
    ) throws InterruptedException {

        long deadline =
                System.nanoTime()
                        + TimeUnit.SECONDS.toNanos(
                                TIMEOUT_SECONDS
                        );

        while (System.nanoTime() < deadline) {

            if (processedEventRepository
                    .existsById(eventId)) {

                return;
            }

            TimeUnit.MILLISECONDS.sleep(
                    100
            );
        }

        fail(
                "Kafka event was not processed within "
                        + TIMEOUT_SECONDS
                        + " seconds"
        );
    }
}
