package com.iremdinc.tickera.dto;

import com.iremdinc.tickera.enums.BookingStatus;

import java.time.LocalDateTime;

public record BookingResponse(
        Long id,
        Long seatId,
        String seatNumber,
        String userId,
        BookingStatus status,
        LocalDateTime createdAt
) {
}