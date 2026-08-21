package com.iremdinc.tickera.service;

import com.iremdinc.tickera.entity.Booking;
import com.iremdinc.tickera.entity.OutboxEvent;
import com.iremdinc.tickera.enums.OutboxStatus;
import com.iremdinc.tickera.event.BookingCreatedEvent;
import com.iremdinc.tickera.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class OutboxService {

    private final OutboxEventRepository outboxEventRepository;
    private final JsonMapper jsonMapper;

    public void saveBookingCreatedEvent(
            Booking booking
    ) {

        BookingCreatedEvent event =
                new BookingCreatedEvent(
                        UUID.randomUUID(),
                        booking.getId(),
                        booking.getSeat().getId(),
                        booking.getSeat().getSeatNumber(),
                        booking.getUserId(),
                        booking.getStatus().name(),
                        LocalDateTime.now()
                );

        String payload;

        try {

            payload =
                    jsonMapper.writeValueAsString(
                            event
                    );

        } catch (JacksonException exception) {

            throw new IllegalStateException(
                    "Failed to serialize BookingCreatedEvent",
                    exception
            );
        }

        OutboxEvent outboxEvent =
                OutboxEvent.builder()
                        .id(event.eventId())
                        .aggregateType("BOOKING")
                        .aggregateId(
                                booking.getId().toString()
                        )
                        .eventType("BOOKING_CREATED")
                        .payload(payload)
                        .status(OutboxStatus.PENDING)
                        .createdAt(LocalDateTime.now())
                        .build();

        outboxEventRepository.save(
                outboxEvent
        );
    }
}