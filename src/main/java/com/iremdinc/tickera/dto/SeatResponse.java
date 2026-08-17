package com.iremdinc.tickera.dto;

import com.iremdinc.tickera.enums.SeatStatus;

public record SeatResponse(
        Long id,
        String seatNumber,
        SeatStatus status
) {
}