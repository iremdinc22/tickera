package com.iremdinc.tickera.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record CreateBookingRequest(

        @NotNull
        Long seatId,

        @NotBlank
        String userId
) {
}