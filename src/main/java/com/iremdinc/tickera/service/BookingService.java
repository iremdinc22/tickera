package com.iremdinc.tickera.service;

import com.iremdinc.tickera.dto.BookingResponse;
import com.iremdinc.tickera.dto.CreateBookingRequest;
import com.iremdinc.tickera.entity.Booking;
import com.iremdinc.tickera.entity.Seat;
import com.iremdinc.tickera.enums.SeatStatus;
import com.iremdinc.tickera.exception.SeatNotAvailableException;
import com.iremdinc.tickera.repository.BookingRepository;
import com.iremdinc.tickera.repository.SeatRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class BookingService {

    private final BookingRepository bookingRepository;
    private final SeatRepository seatRepository;

    @Transactional
    public BookingResponse createBooking(CreateBookingRequest request) {

        Seat seat = seatRepository.findByIdForUpdate(request.seatId())
                .orElseThrow(() -> new RuntimeException("Seat not found"));

        if (seat.getStatus() != SeatStatus.AVAILABLE) {
            throw new SeatNotAvailableException("Seat is not available");
        }

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        seat.setStatus(SeatStatus.BOOKED);

        Booking booking = Booking.builder()
                .seat(seat)
                .userId(request.userId())
                .build();

        Booking savedBooking = bookingRepository.save(booking);

        return new BookingResponse(
                savedBooking.getId(),
                savedBooking.getSeat().getId(),
                savedBooking.getSeat().getSeatNumber(),
                savedBooking.getUserId(),
                savedBooking.getStatus(),
                savedBooking.getCreatedAt()
        );
    }
}