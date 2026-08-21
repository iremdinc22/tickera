package com.iremdinc.tickera.event;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class BookingEventPublisher {

    private static final String TOPIC =
            "booking-created";

    private final KafkaTemplate<String, BookingCreatedEvent> kafkaTemplate;

    public void publish(
            BookingCreatedEvent event
    ) {

        kafkaTemplate.send(
                TOPIC,
                event.bookingId().toString(),
                event
        );
    }
}