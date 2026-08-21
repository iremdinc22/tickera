package com.iremdinc.tickera.event;

import java.time.LocalDateTime;
import java.util.UUID;

public record BookingCreatedEvent(
        UUID eventId,
        Long bookingId,
        Long seatId,
        String seatNumber,
        String userId,
        String bookingStatus,
        LocalDateTime occurredAt
) {
}