package com.iremdinc.tickera.service;

import com.iremdinc.tickera.event.BookingCreatedEvent;
import com.iremdinc.tickera.event.BookingEventPublisher;
import com.iremdinc.tickera.repository.ProcessedEventRepository;
import com.iremdinc.tickera.support.PostgresIntegrationTest;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import org.testcontainers.kafka.KafkaContainer;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@SpringBootTest
class IdempotentConsumerIntegrationTest
        extends PostgresIntegrationTest {

    private static final long TIMEOUT_SECONDS = 10;

    static final KafkaContainer kafka =
            new KafkaContainer(
                    "apache/kafka:4.1.1"
            );

    static {
        kafka.start();
    }

    @DynamicPropertySource
    static void configureKafka(
            DynamicPropertyRegistry registry
    ) {

        registry.add(
                "spring.kafka.bootstrap-servers",
                kafka::getBootstrapServers
        );
    }

    @Autowired
    private BookingEventPublisher bookingEventPublisher;

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @BeforeEach
    void setUp() {

        processedEventRepository.deleteAll();
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
         * Duplicate delivery with exactly
         * the same eventId.
         */
        bookingEventPublisher.publish(
                event
        );

        TimeUnit.MILLISECONDS.sleep(
                500
        );

        /*
         * Duplicate message must not create
         * another processed_events record.
         */
        assertEquals(
                1,
                processedEventRepository.count(),
                "Duplicate delivery must not be processed twice"
        );

        assertTrue(
                processedEventRepository
                        .existsById(eventId),
                "Original processed event record must remain"
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