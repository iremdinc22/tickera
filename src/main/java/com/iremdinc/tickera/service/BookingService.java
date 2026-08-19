package com.iremdinc.tickera.service;

import com.iremdinc.tickera.dto.BookingResponse;
import com.iremdinc.tickera.dto.CreateBookingRequest;
import com.iremdinc.tickera.entity.Booking;
import com.iremdinc.tickera.entity.Seat;
import com.iremdinc.tickera.enums.SeatStatus;
import com.iremdinc.tickera.exception.SeatNotAvailableException;
import com.iremdinc.tickera.repository.BookingRepository;
import com.iremdinc.tickera.repository.SeatRepository;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@Service
@RequiredArgsConstructor
public class BookingService {

    private final BookingRepository bookingRepository;
    private final SeatRepository seatRepository;
    private final MeterRegistry meterRegistry;

    @Transactional
    public BookingResponse createBooking(CreateBookingRequest request) {

        Timer.Sample transactionSample = Timer.start(meterRegistry);

        TransactionSynchronizationManager.registerSynchronization(
                new TransactionSynchronization() {
                    @Override
                    public void afterCompletion(int status) {
                        transactionSample.stop(
                                meterRegistry.timer("booking.transaction.duration")
                        );
                    }
                }
        );

        Timer.Sample lockSample = Timer.start(meterRegistry);

        Seat seat = seatRepository.findByIdForUpdate(request.seatId())
                .orElseThrow(() -> new RuntimeException("Seat not found"));

        lockSample.stop(
                meterRegistry.timer("booking.seat.lock.duration")
        );

        if (seat.getStatus() != SeatStatus.AVAILABLE) {
            throw new SeatNotAvailableException("Seat is not available");
        }

        seat.setStatus(SeatStatus.BOOKED);

        Booking booking = Booking.builder()
                .seat(seat)
                .userId(request.userId())
                .build();

        Timer.Sample saveSample = Timer.start(meterRegistry);

        Booking savedBooking = bookingRepository.save(booking);

        saveSample.stop(
                meterRegistry.timer("booking.save.duration")
        );

        Timer.Sample flushSample = Timer.start(meterRegistry);

        bookingRepository.flush();

        flushSample.stop(
                meterRegistry.timer("booking.flush.duration")
        );

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