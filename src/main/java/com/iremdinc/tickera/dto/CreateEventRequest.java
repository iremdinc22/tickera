package com.iremdinc.tickera.dto;

import jakarta.validation.constraints.Future;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.LocalDateTime;
import java.util.List;

public record CreateEventRequest(

        @NotBlank
        String name,

        @NotBlank
        String venue,

        @NotNull
        @Future
        LocalDateTime eventDate,

        @NotNull
        List<String> seatNumbers
) {
}