package com.iremdinc.tickera.event;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
@Slf4j
public class BookingEventPublisher {

    private static final String TOPIC =
            "booking-created";

    private final KafkaTemplate<String, BookingCreatedEvent> kafkaTemplate;

    /*
     * Kafka broker'ından acknowledgement gelmesi için
     * beklenecek üst süre. Sınırsız beklemeyi engeller.
     *
     * Bu süre producer'ın delivery.timeout.ms değerinden
     * küçük tutulmalıdır; aksi halde uygulama, producer
     * kendi timeout'una ulaşmadan önce vazgeçmiş olur.
     */
    private final Duration publishAckTimeout;

    public BookingEventPublisher(
            KafkaTemplate<String, BookingCreatedEvent> kafkaTemplate,
            @Value("${tickera.outbox.publish-ack-timeout:10s}")
            Duration publishAckTimeout
    ) {

        this.kafkaTemplate = kafkaTemplate;
        this.publishAckTimeout = publishAckTimeout;
    }

    /**
     * Event'i Kafka'ya yayınlar ve broker acknowledgement'ını
     * bekler.
     *
     * Bu metot yalnızca Kafka send işlemi başarıyla
     * acknowledgement edildiğinde normal şekilde döner.
     *
     * Synchronous hatalar (örneğin metadata alınamaması)
     * KafkaException olarak, asynchronous hatalar ve
     * timeout ise IllegalStateException olarak çağıran
     * tarafa iletilir.
     */
    public void publish(
            BookingCreatedEvent event
    ) {

        SendResult<String, BookingCreatedEvent> sendResult;

        try {

            sendResult =
                    kafkaTemplate
                            .send(
                                    TOPIC,
                                    event.bookingId().toString(),
                                    event
                            )
                            .get(
                                    publishAckTimeout.toMillis(),
                                    TimeUnit.MILLISECONDS
                            );

        } catch (InterruptedException exception) {

            /*
             * Interrupt flag'ini korumak zorunludur.
             * Aksi halde üst katman interrupt sinyalini
             * kaybeder.
             */
            Thread.currentThread()
                    .interrupt();

            throw new IllegalStateException(
                    "Interrupted while waiting for Kafka acknowledgement: eventId="
                            + event.eventId(),
                    exception
            );

        } catch (TimeoutException exception) {

            /*
             * Broker acknowledgement'ı zamanında gelmedi.
             *
             * Producer arka planda gönderimi tamamlayabilir;
             * bu yüzden retry duplicate delivery üretebilir.
             * Consumer idempotent olduğu için bu kabul edilebilir
             * (at-least-once).
             */
            throw new IllegalStateException(
                    "Kafka acknowledgement timed out after "
                            + publishAckTimeout
                            + ": eventId="
                            + event.eventId(),
                    exception
            );

        } catch (ExecutionException exception) {

            Throwable cause =
                    exception.getCause() == null
                            ? exception
                            : exception.getCause();

            throw new IllegalStateException(
                    "Kafka publish failed: eventId="
                            + event.eventId()
                            + ", cause="
                            + cause.getMessage(),
                    cause
            );
        }

        if (sendResult == null
                || sendResult.getRecordMetadata() == null) {

            throw new IllegalStateException(
                    "Kafka publish completed without acknowledgement metadata: eventId="
                            + event.eventId()
            );
        }

        log.debug(
                "Kafka acknowledged booking event: eventId={}, topic={}, partition={}, offset={}",
                event.eventId(),
                sendResult.getRecordMetadata().topic(),
                sendResult.getRecordMetadata().partition(),
                sendResult.getRecordMetadata().offset()
        );
    }
}
