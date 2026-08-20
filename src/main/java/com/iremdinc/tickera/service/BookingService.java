package com.iremdinc.tickera.service;

import com.iremdinc.tickera.dto.BookingResponse;
import com.iremdinc.tickera.dto.CreateBookingRequest;
import com.iremdinc.tickera.entity.Booking;
import com.iremdinc.tickera.entity.IdempotencyRecord;
import com.iremdinc.tickera.entity.Seat;
import com.iremdinc.tickera.enums.IdempotencyStatus;
import com.iremdinc.tickera.enums.SeatStatus;
import com.iremdinc.tickera.exception.IdempotencyConflictException;
import com.iremdinc.tickera.exception.SeatNotAvailableException;
import com.iremdinc.tickera.repository.BookingRepository;
import com.iremdinc.tickera.repository.IdempotencyRecordRepository;
import com.iremdinc.tickera.repository.SeatRepository;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

@Service
@RequiredArgsConstructor
public class BookingService {

    private static final int IDEMPOTENCY_MAX_ATTEMPTS = 200;
    private static final long IDEMPOTENCY_RETRY_DELAY_MS = 25;

    private final BookingRepository bookingRepository;
    private final SeatRepository seatRepository;
    private final IdempotencyRecordRepository idempotencyRecordRepository;
    private final MeterRegistry meterRegistry;
    private final PlatformTransactionManager transactionManager;

    @Transactional
    public BookingResponse createBooking(
            CreateBookingRequest request
    ) {

        Timer.Sample transactionSample =
                Timer.start(meterRegistry);

        TransactionSynchronizationManager.registerSynchronization(
                new TransactionSynchronization() {
                    @Override
                    public void afterCompletion(int status) {
                        transactionSample.stop(
                                meterRegistry.timer(
                                        "booking.transaction.duration"
                                )
                        );
                    }
                }
        );

        Timer.Sample lockSample =
                Timer.start(meterRegistry);

        Seat seat = seatRepository
                .findByIdForUpdate(request.seatId())
                .orElseThrow(() ->
                        new RuntimeException("Seat not found")
                );

        lockSample.stop(
                meterRegistry.timer(
                        "booking.seat.lock.duration"
                )
        );

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

        Timer.Sample saveSample =
                Timer.start(meterRegistry);

        Booking savedBooking =
                bookingRepository.save(booking);

        saveSample.stop(
                meterRegistry.timer(
                        "booking.save.duration"
                )
        );

        Timer.Sample flushSample =
                Timer.start(meterRegistry);

        bookingRepository.flush();

        flushSample.stop(
                meterRegistry.timer(
                        "booking.flush.duration"
                )
        );

        return toBookingResponse(savedBooking);
    }

    @Transactional
    public BookingResponse createBookingOptimistic(
            CreateBookingRequest request
    ) {

        Seat seat = seatRepository
                .findById(request.seatId())
                .orElseThrow(() ->
                        new RuntimeException("Seat not found")
                );

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

        Booking savedBooking =
                bookingRepository.save(booking);

        bookingRepository.flush();

        return toBookingResponse(savedBooking);
    }

    public BookingResponse createBookingIdempotent(
            String idempotencyKey,
            CreateBookingRequest request
    ) {

        String requestHash =
                generateRequestHash(request);

        for (int attempt = 0;
             attempt < IDEMPOTENCY_MAX_ATTEMPTS;
             attempt++) {

            IdempotencySnapshot snapshot =
                    findIdempotencySnapshot(
                            idempotencyKey
                    );

            /*
             * Key zaten varsa önce request'in aynı
             * logical operation olup olmadığını kontrol ediyoruz.
             */
            if (snapshot != null) {

                validateRequestHash(
                        snapshot.requestHash(),
                        requestHash
                );

                /*
                 * İşlem tamamlanmışsa retry yapan client'a
                 * daha önce oluşturulan booking'i döndürüyoruz.
                 */
                if (snapshot.status()
                        == IdempotencyStatus.COMPLETED) {

                    return findExistingBooking(
                            snapshot.bookingId()
                    );
                }

                /*
                 * PROCESSING ise başka bir request şu anda
                 * bu logical operation'ın owner'ıdır.
                 *
                 * Yeni booking oluşturmuyoruz.
                 * İşlemin tamamlanmasını bekliyoruz.
                 */
                waitBeforeRetry();
                continue;
            }

            /*
             * Henüz record yok.
             *
             * Bu request idempotency key'in owner'ı
             * olmaya çalışıyor.
             */
            boolean claimed =
                    tryClaimIdempotencyKey(
                            idempotencyKey,
                            requestHash
                    );

            /*
             * Başka thread bizden önce key'i claim etmiş
             * olabilir. O durumda tekrar state kontrolüne
             * dönüyoruz.
             */
            if (!claimed) {
                continue;
            }

            /*
             * Bu request artık owner.
             *
             * Booking'i gerçekleştirip aynı transaction
             * içerisinde idempotency kaydını COMPLETED
             * durumuna geçiriyoruz.
             */
            try {
                return processOwnedBooking(
                        idempotencyKey,
                        request
                );
            } catch (RuntimeException exception) {

                /*
                 * Booking başarısız olursa PROCESSING
                 * kaydını sonsuza kadar bırakmıyoruz.
                 */
                releaseIdempotencyClaim(
                        idempotencyKey
                );

                throw exception;
            }
        }

        throw new IllegalStateException(
                "Timed out waiting for idempotent request to complete"
        );
    }

    /*
     * Idempotency key üzerinde ownership almaya çalışır.
     *
     * Bu işlem ayrı transaction'da çalışır.
     * Böylece PROCESSING kaydı hemen commit edilir ve
     * diğer concurrent request'ler tarafından görülebilir.
     */
    private boolean tryClaimIdempotencyKey(
            String idempotencyKey,
            String requestHash
    ) {

        TransactionTemplate transactionTemplate =
                requiresNewTransactionTemplate();

        try {

            Boolean claimed =
                    transactionTemplate.execute(status -> {

                        IdempotencyRecord record =
                                IdempotencyRecord.builder()
                                        .idempotencyKey(
                                                idempotencyKey
                                        )
                                        .requestHash(
                                                requestHash
                                        )
                                        .status(
                                                IdempotencyStatus.PROCESSING
                                        )
                                        .build();

                        idempotencyRecordRepository
                                .saveAndFlush(record);

                        return true;
                    });

            return Boolean.TRUE.equals(claimed);

        } catch (DataIntegrityViolationException exception) {

            /*
             * UNIQUE(idempotency_key) constraint:
             *
             * başka bir thread bu key'i bizden önce
             * claim etmiş demektir.
             */
            return false;
        }
    }

    /*
     * Owner request booking işlemini gerçekleştirir.
     *
     * Booking oluşturma ve PROCESSING -> COMPLETED
     * geçişi aynı transaction içinde yapılır.
     */
    private BookingResponse processOwnedBooking(
            String idempotencyKey,
            CreateBookingRequest request
    ) {

        TransactionTemplate transactionTemplate =
                new TransactionTemplate(
                        transactionManager
                );

        return transactionTemplate.execute(status -> {

            Seat seat = seatRepository
                    .findByIdForUpdate(request.seatId())
                    .orElseThrow(() ->
                            new RuntimeException(
                                    "Seat not found"
                            )
                    );

            if (seat.getStatus()
                    != SeatStatus.AVAILABLE) {

                throw new SeatNotAvailableException(
                        "Seat is not available"
                );
            }

            seat.setStatus(
                    SeatStatus.BOOKED
            );

            Booking booking =
                    Booking.builder()
                            .seat(seat)
                            .userId(request.userId())
                            .build();

            Booking savedBooking =
                    bookingRepository
                            .saveAndFlush(booking);

            IdempotencyRecord record =
                    idempotencyRecordRepository
                            .findByIdempotencyKey(
                                    idempotencyKey
                            )
                            .orElseThrow(() ->
                                    new IllegalStateException(
                                            "Idempotency record not found"
                                    )
                            );

            record.setBookingId(
                    savedBooking.getId()
            );

            record.setStatus(
                    IdempotencyStatus.COMPLETED
            );

            idempotencyRecordRepository
                    .saveAndFlush(record);

            return toBookingResponse(
                    savedBooking
            );
        });
    }

    /*
     * Idempotency kaydını kısa ve bağımsız bir
     * transaction içerisinde okuyoruz.
     *
     * Böylece her retry en güncel committed state'i görür.
     */
    private IdempotencySnapshot findIdempotencySnapshot(
            String idempotencyKey
    ) {

        TransactionTemplate transactionTemplate =
                requiresNewTransactionTemplate();

        return transactionTemplate.execute(status ->
                idempotencyRecordRepository
                        .findByIdempotencyKey(
                                idempotencyKey
                        )
                        .map(record ->
                                new IdempotencySnapshot(
                                        record.getRequestHash(),
                                        record.getStatus(),
                                        record.getBookingId()
                                )
                        )
                        .orElse(null)
        );
    }

    private BookingResponse findExistingBooking(
            Long bookingId
    ) {

        if (bookingId == null) {
            throw new IllegalStateException(
                    "Completed idempotency record has no booking"
            );
        }

        TransactionTemplate transactionTemplate =
                requiresNewTransactionTemplate();

        return transactionTemplate.execute(status -> {

            Booking booking =
                    bookingRepository
                            .findById(bookingId)
                            .orElseThrow(() ->
                                    new IllegalStateException(
                                            "Booking linked to idempotency record not found"
                                    )
                            );

            return toBookingResponse(
                    booking
            );
        });
    }

    /*
     * Owner işlemi başarısız olursa PROCESSING kaydını
     * temizliyoruz.
     *
     * Böylece daha sonraki retry tekrar ownership
     * alabilir.
     */
    private void releaseIdempotencyClaim(
            String idempotencyKey
    ) {

        TransactionTemplate transactionTemplate =
                requiresNewTransactionTemplate();

        transactionTemplate.executeWithoutResult(status -> {

            idempotencyRecordRepository
                    .findByIdempotencyKey(
                            idempotencyKey
                    )
                    .filter(record ->
                            record.getStatus()
                                    == IdempotencyStatus.PROCESSING
                    )
                    .ifPresent(
                            idempotencyRecordRepository::delete
                    );
        });
    }

    private void validateRequestHash(
            String existingHash,
            String incomingHash
    ) {

        if (!existingHash.equals(incomingHash)) {

            throw new IdempotencyConflictException(
                    "Idempotency key was already used with a different request"
            );
        }
    }

    private void waitBeforeRetry() {

        try {

            Thread.sleep(
                    IDEMPOTENCY_RETRY_DELAY_MS
            );

        } catch (InterruptedException exception) {

            Thread.currentThread().interrupt();

            throw new IllegalStateException(
                    "Interrupted while waiting for idempotent request",
                    exception
            );
        }
    }

    private TransactionTemplate
    requiresNewTransactionTemplate() {

        TransactionTemplate transactionTemplate =
                new TransactionTemplate(
                        transactionManager
                );

        transactionTemplate.setPropagationBehavior(
                TransactionDefinition
                        .PROPAGATION_REQUIRES_NEW
        );

        return transactionTemplate;
    }

    private String generateRequestHash(
            CreateBookingRequest request
    ) {

        String requestData =
                request.seatId()
                        + ":"
                        + request.userId();

        try {

            MessageDigest digest =
                    MessageDigest.getInstance(
                            "SHA-256"
                    );

            byte[] hash =
                    digest.digest(
                            requestData.getBytes(
                                    StandardCharsets.UTF_8
                            )
                    );

            return HexFormat
                    .of()
                    .formatHex(hash);

        } catch (NoSuchAlgorithmException exception) {

            throw new IllegalStateException(
                    "SHA-256 algorithm is not available",
                    exception
            );
        }
    }

    private BookingResponse toBookingResponse(
            Booking booking
    ) {

        return new BookingResponse(
                booking.getId(),
                booking.getSeat().getId(),
                booking.getSeat().getSeatNumber(),
                booking.getUserId(),
                booking.getStatus(),
                booking.getCreatedAt()
        );
    }

    private record IdempotencySnapshot(
            String requestHash,
            IdempotencyStatus status,
            Long bookingId
    ) {
    }
}
