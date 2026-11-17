package com.iremdinc.tickera.service;

import com.iremdinc.tickera.entity.OutboxEvent;
import com.iremdinc.tickera.enums.OutboxStatus;
import com.iremdinc.tickera.event.BookingCreatedEvent;
import com.iremdinc.tickera.repository.OutboxEventRepository;
import com.iremdinc.tickera.support.KafkaIntegrationTest;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Outbox -> gerçek Kafka broker -> ACK -> PUBLISHED
 * akışını izole bir Kafka Testcontainer'ı üzerinde doğrular.
 */
@SpringBootTest
class OutboxKafkaPublishIntegrationTest
        extends KafkaIntegrationTest {

    private static final long WAIT_SECONDS = 30;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private OutboxPublisher outboxPublisher;

    @Autowired
    private JsonMapper jsonMapper;

    @BeforeEach
    void setUp() {

        outboxEventRepository.deleteAll();
    }

    @Test
    void shouldPublishOutboxEventToKafkaAndMarkAsPublished() {

        UUID eventId =
                UUID.randomUUID();

        long bookingId =
                2001L;

        BookingCreatedEvent event =
                new BookingCreatedEvent(
                        eventId,
                        bookingId,
                        bookingId + 10_000,
                        "TEST-KAFKA-1",
                        "irem",
                        "PENDING",
                        LocalDateTime.now()
                );

        outboxEventRepository.saveAndFlush(
                OutboxEvent.builder()
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
                                OutboxStatus.PENDING
                        )
                        .build()
        );

        outboxPublisher
                .publishPendingEvents();

        OutboxEvent publishedEvent =
                outboxEventRepository
                        .findById(
                                eventId
                        )
                        .orElseThrow();

        assertEquals(
                OutboxStatus.PUBLISHED,
                publishedEvent.getStatus()
        );

        assertNotNull(
                publishedEvent.getPublishedAt()
        );

        assertNull(
                publishedEvent.getLastError()
        );

        assertNull(
                publishedEvent.getNextRetryAt()
        );

        assertEquals(
                0,
                publishedEvent.getRetryCount()
        );

        ConsumerRecord<String, String> record =
                awaitRecord(
                        String.valueOf(
                                bookingId
                        )
                );

        assertEquals(
                String.valueOf(
                        bookingId
                ),
                record.key()
        );

        assertTrue(
                record.value()
                        .contains(
                                eventId.toString()
                        ),
                "Published payload must preserve the outbox event ID"
        );
    }

    private ConsumerRecord<String, String> awaitRecord(
            String expectedKey
    ) {

        Properties properties =
                new Properties();

        properties.put(
                "bootstrap.servers",
                bootstrapServers()
        );

        properties.put(
                "key.deserializer",
                StringDeserializer.class
                        .getName()
        );

        properties.put(
                "value.deserializer",
                StringDeserializer.class
                        .getName()
        );

        properties.put(
                "group.id",
                "outbox-publish-test-" + UUID.randomUUID()
        );

        properties.put(
                "auto.offset.reset",
                "earliest"
        );

        properties.put(
                "enable.auto.commit",
                "false"
        );

        try (
                KafkaConsumer<String, String> consumer =
                        new KafkaConsumer<>(
                                properties
                        )
        ) {

            consumer.subscribe(
                    List.of(
                            BOOKING_CREATED_TOPIC
                    )
            );

            long deadline =
                    System.nanoTime()
                            + TimeUnit.SECONDS.toNanos(
                                    WAIT_SECONDS
                            );

            while (System.nanoTime() < deadline) {

                ConsumerRecords<String, String> records =
                        consumer.poll(
                                Duration.ofMillis(
                                        500
                                )
                        );

                for (ConsumerRecord<String, String> record
                        : records) {

                    if (expectedKey.equals(
                            record.key()
                    )) {

                        return record;
                    }
                }
            }

            fail(
                    "Published Kafka record was not found within "
                            + WAIT_SECONDS
                            + " seconds"
            );

            return null;

        }
    }
}
