package com.iremdinc.tickera.event;

import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * KafkaTemplate seviyesinde ACK davranışını deterministik
 * olarak doğrular. Gerçek broker gerektirmez.
 */
class BookingEventPublisherTest {

    private static final String TOPIC =
            "booking-created";

    private static final Duration ACK_TIMEOUT =
            Duration.ofMillis(
                    200
            );

    private KafkaTemplate<String, BookingCreatedEvent> kafkaTemplate;

    private BookingEventPublisher publisher;

    private BookingCreatedEvent event;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {

        kafkaTemplate =
                mock(
                        KafkaTemplate.class
                );

        publisher =
                new BookingEventPublisher(
                        kafkaTemplate,
                        ACK_TIMEOUT
                );

        event =
                new BookingCreatedEvent(
                        UUID.randomUUID(),
                        1001L,
                        2001L,
                        "TEST-A1",
                        "irem",
                        "PENDING",
                        LocalDateTime.now()
                );
    }

    @Test
    void shouldReturnAfterKafkaAcknowledgement() {

        SendResult<String, BookingCreatedEvent> sendResult =
                successfulSendResult();

        when(
                kafkaTemplate.send(
                        eq(TOPIC),
                        eq("1001"),
                        eq(event)
                )
        ).thenReturn(
                CompletableFuture.completedFuture(
                        sendResult
                )
        );

        assertDoesNotThrow(
                () -> publisher.publish(
                        event
                )
        );

        verify(
                kafkaTemplate
        ).send(
                TOPIC,
                "1001",
                event
        );
    }

    @Test
    void shouldPropagateAsynchronousPublishFailure() {

        CompletableFuture<SendResult<String, BookingCreatedEvent>>
                failedFuture =
                new CompletableFuture<>();

        failedFuture.completeExceptionally(
                new RuntimeException(
                        "broker is unavailable"
                )
        );

        when(
                kafkaTemplate.send(
                        eq(TOPIC),
                        eq("1001"),
                        eq(event)
                )
        ).thenReturn(
                failedFuture
        );

        IllegalStateException exception =
                assertThrows(
                        IllegalStateException.class,
                        () -> publisher.publish(
                                event
                        )
                );

        assertTrue(
                exception.getMessage()
                        .contains(
                                "Kafka publish failed"
                        )
        );

        assertTrue(
                exception.getMessage()
                        .contains(
                                "broker is unavailable"
                        )
        );
    }

    @Test
    void shouldPropagateSynchronousPublishFailure() {

        when(
                kafkaTemplate.send(
                        anyString(),
                        anyString(),
                        eq(event)
                )
        ).thenThrow(
                new KafkaException(
                        "failed to obtain metadata"
                )
        );

        KafkaException exception =
                assertThrows(
                        KafkaException.class,
                        () -> publisher.publish(
                                event
                        )
                );

        assertTrue(
                exception.getMessage()
                        .contains(
                                "failed to obtain metadata"
                        )
        );
    }

    @Test
    void shouldFailWhenAcknowledgementTimesOut() {

        when(
                kafkaTemplate.send(
                        eq(TOPIC),
                        eq("1001"),
                        eq(event)
                )
        ).thenReturn(
                new CompletableFuture<>()
        );

        long startedAt =
                System.nanoTime();

        IllegalStateException exception =
                assertThrows(
                        IllegalStateException.class,
                        () -> publisher.publish(
                                event
                        )
                );

        long elapsedMillis =
                TimeUnit.NANOSECONDS.toMillis(
                        System.nanoTime()
                                - startedAt
                );

        assertTrue(
                exception.getMessage()
                        .contains(
                                "timed out"
                        )
        );

        /*
         * Bounded timeout: testin makul sürede
         * tamamlandığını doğrular.
         */
        assertTrue(
                elapsedMillis < 5_000,
                "ACK timeout must be bounded, elapsed="
                        + elapsedMillis
                        + "ms"
        );
    }

    @Test
    void shouldRestoreInterruptFlagWhenInterrupted() {

        when(
                kafkaTemplate.send(
                        eq(TOPIC),
                        eq("1001"),
                        eq(event)
                )
        ).thenReturn(
                new CompletableFuture<>()
        );

        Thread.currentThread()
                .interrupt();

        try {

            IllegalStateException exception =
                    assertThrows(
                            IllegalStateException.class,
                            () -> publisher.publish(
                                    event
                            )
                    );

            assertTrue(
                    exception.getMessage()
                            .contains(
                                    "Interrupted"
                            )
            );

            assertTrue(
                    Thread.currentThread()
                            .isInterrupted(),
                    "Interrupt flag must be preserved"
            );

        } finally {

            Thread.interrupted();
        }
    }

    @SuppressWarnings("unchecked")
    private SendResult<String, BookingCreatedEvent>
    successfulSendResult() {

        SendResult<String, BookingCreatedEvent> sendResult =
                mock(
                        SendResult.class
                );

        when(
                sendResult.getRecordMetadata()
        ).thenReturn(
                mock(
                        org.apache.kafka.clients.producer.RecordMetadata.class
                )
        );

        return sendResult;
    }
}
