package com.iremdinc.tickera.dto;

import java.time.LocalDateTime;
import java.util.List;

public record EventResponse(
        Long id,
        String name,
        String venue,
        LocalDateTime eventDate,
        List<SeatResponse> seats
) {
}