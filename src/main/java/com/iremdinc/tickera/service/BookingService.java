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

        return toBookingResponse(savedBooking);
    }

    @Transactional
    public BookingResponse createBookingOptimistic(
            CreateBookingRequest request
    ) {

        Seat seat = seatRepository.findById(request.seatId())
                .orElseThrow(() -> new RuntimeException("Seat not found"));

        if (seat.getStatus() != SeatStatus.AVAILABLE) {
            throw new SeatNotAvailableException(
                    "Seat is not available"
            );
        }

        seat.setStatus(SeatStatus.BOOKED);

        Booking booking = Booking.builder()
                .seat(seat)
                .userId(request.userId())
                .build();

        Booking savedBooking = bookingRepository.save(booking);

        /*
         * Flush'u transaction sonuna bırakmıyoruz.
         *
         * Böylece Seat entity'sindeki @Version kontrolü
         * burada DB'ye gönderilir ve optimistic locking
         * conflict'i metodun içinde ortaya çıkar.
         */
        bookingRepository.flush();

        return toBookingResponse(savedBooking);
    }

    private BookingResponse toBookingResponse(Booking booking) {

        return new BookingResponse(
                booking.getId(),
                booking.getSeat().getId(),
                booking.getSeat().getSeatNumber(),
                booking.getUserId(),
                booking.getStatus(),
                booking.getCreatedAt()
        );
    }
}