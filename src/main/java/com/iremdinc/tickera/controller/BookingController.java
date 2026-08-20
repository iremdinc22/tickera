package com.iremdinc.tickera.controller;

import com.iremdinc.tickera.dto.BookingResponse;
import com.iremdinc.tickera.dto.CreateBookingRequest;
import com.iremdinc.tickera.service.BookingService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/bookings")
@RequiredArgsConstructor
public class BookingController {

    private final BookingService bookingService;

    /*
     * Idempotent booking flow.
     *
     * Same Idempotency-Key + same request:
     * returns the existing booking.
     *
     * Same Idempotency-Key + different request:
     * returns 409 Conflict.
     */
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public BookingResponse createBooking(
            @RequestHeader("Idempotency-Key") String idempotencyKey,
            @RequestBody CreateBookingRequest request
    ) {

        return bookingService.createBookingIdempotent(
                idempotencyKey,
                request
        );
    }

    /*
     * Redis seat-hold based booking flow.
     *
     * User must already own an active Redis hold
     * for the requested seat.
     */
    @PostMapping("/held")
    @ResponseStatus(HttpStatus.CREATED)
    public BookingResponse createHeldBooking(
            @RequestBody CreateBookingRequest request
    ) {

        return bookingService.createHeldBooking(
                request
        );
    }
}