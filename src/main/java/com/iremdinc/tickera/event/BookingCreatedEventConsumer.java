package com.iremdinc.tickera.event;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class BookingCreatedEventConsumer {

    @KafkaListener(
            topics = "booking-created",
            groupId = "tickera-booking-consumers"
    )
    public void consume(
            BookingCreatedEvent event
    ) {

        log.info(
                "BookingCreatedEvent received: eventId={}, bookingId={}, seatId={}, userId={}, status={}",
                event.eventId(),
                event.bookingId(),
                event.seatId(),
                event.userId(),
                event.bookingStatus()
        );
    }
}