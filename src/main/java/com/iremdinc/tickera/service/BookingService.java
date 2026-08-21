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
    private final SeatHoldService seatHoldService;
    private final OutboxService outboxService;

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
                        new RuntimeException(
                                "Seat not found"
                        )
                );

        lockSample.stop(
                meterRegistry.timer(
                        "booking.seat.lock.duration"
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

        Timer.Sample saveSample =
                Timer.start(meterRegistry);

        Booking savedBooking =
                bookingRepository.save(
                        booking
                );

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

        /*
         * Kafka'ya doğrudan publish etmiyoruz.
         *
         * Booking ve OutboxEvent aynı PostgreSQL
         * transaction içerisinde yazılır.
         */
        outboxService.saveBookingCreatedEvent(
                savedBooking
        );

        return toBookingResponse(
                savedBooking
        );
    }

    @Transactional
    public BookingResponse createBookingOptimistic(
            CreateBookingRequest request
    ) {

        Seat seat = seatRepository
                .findById(request.seatId())
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
                bookingRepository.save(
                        booking
                );

        /*
         * @Version conflict'inin transaction
         * tamamlanmadan ortaya çıkmasını sağlıyoruz.
         */
        bookingRepository.flush();

        /*
         * Booking ve event bilgisi aynı
         * PostgreSQL transaction'ında tutulur.
         */
        outboxService.saveBookingCreatedEvent(
                savedBooking
        );

        return toBookingResponse(
                savedBooking
        );
    }

    /**
     * Redis seat-hold mekanizmasını kullanan booking flow.
     *
     * Kullanıcının önce Redis'te ilgili seat'in
     * aktif hold sahibi olması gerekir.
     */
    @Transactional
    public BookingResponse createHeldBooking(
            CreateBookingRequest request
    ) {

        /*
         * Redis'teki hold gerçekten bu kullanıcıya
         * ait mi kontrol ediyoruz.
         */
        if (!seatHoldService.isHeldByUser(
                request.seatId(),
                request.userId()
        )) {

            throw new SeatNotAvailableException(
                    "Seat is not held by this user"
            );
        }

        /*
         * Redis temporary reservation state'ini yönetir.
         *
         * Final booking correctness PostgreSQL
         * pessimistic locking ile korunmaya devam eder.
         */
        Seat seat = seatRepository
                .findByIdForUpdate(
                        request.seatId()
                )
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
                        .saveAndFlush(
                                booking
                        );

        /*
         * Booking oluşturulduğu için corresponding
         * integration event'i Outbox'a kaydediyoruz.
         */
        outboxService.saveBookingCreatedEvent(
                savedBooking
        );

        /*
         * Booking başarılı olduktan sonra Redis'teki
         * geçici hold artık gerekli değildir.
         */
        seatHoldService.releaseSeat(
                request.seatId(),
                request.userId()
        );

        return toBookingResponse(
                savedBooking
        );
    }

    /**
     * Idempotent booking flow.
     *
     * Aynı Idempotency-Key + aynı request:
     * mevcut booking sonucunu döndürür.
     *
     * Aynı key + farklı request:
     * conflict üretir.
     */
    public BookingResponse createBookingIdempotent(
            String idempotencyKey,
            CreateBookingRequest request
    ) {

        String requestHash =
                generateRequestHash(
                        request
                );

        for (
                int attempt = 0;
                attempt < IDEMPOTENCY_MAX_ATTEMPTS;
                attempt++
        ) {

            IdempotencySnapshot snapshot =
                    findIdempotencySnapshot(
                            idempotencyKey
                    );

            /*
             * Key zaten varsa gelen request'in
             * aynı logical operation olup olmadığını
             * kontrol ediyoruz.
             */
            if (snapshot != null) {

                validateRequestHash(
                        snapshot.requestHash(),
                        requestHash
                );

                /*
                 * İşlem daha önce tamamlanmışsa
                 * yeni Booking veya OutboxEvent üretmiyoruz.
                 *
                 * Mevcut sonucu döndürüyoruz.
                 */
                if (snapshot.status()
                        == IdempotencyStatus.COMPLETED) {

                    return findExistingBooking(
                            snapshot.bookingId()
                    );
                }

                /*
                 * PROCESSING ise başka request
                 * operation owner'ıdır.
                 */
                waitBeforeRetry();

                continue;
            }

            /*
             * Record yoksa bu request idempotency
             * key'in ownership'ini almaya çalışır.
             */
            boolean claimed =
                    tryClaimIdempotencyKey(
                            idempotencyKey,
                            requestHash
                    );

            if (!claimed) {
                continue;
            }

            try {

                return processOwnedBooking(
                        idempotencyKey,
                        request
                );

            } catch (RuntimeException exception) {

                /*
                 * Booking başarısız olursa PROCESSING
                 * claim sonsuza kadar tutulmaz.
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

    /**
     * Idempotency key ownership claim.
     *
     * REQUIRES_NEW sayesinde PROCESSING kaydı
     * bağımsız olarak commit edilir.
     */
    private boolean tryClaimIdempotencyKey(
            String idempotencyKey,
            String requestHash
    ) {

        TransactionTemplate transactionTemplate =
                requiresNewTransactionTemplate();

        try {

            Boolean claimed =
                    transactionTemplate.execute(
                            status -> {

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
                                        .saveAndFlush(
                                                record
                                        );

                                return true;
                            }
                    );

            return Boolean.TRUE.equals(
                    claimed
            );

        } catch (
                DataIntegrityViolationException exception
        ) {

            /*
             * UNIQUE(idempotency_key)
             *
             * Başka bir request ownership'i
             * bizden önce aldı.
             */
            return false;
        }
    }

    /**
     * Gerçek idempotency owner booking işlemini
     * burada gerçekleştirir.
     *
     * Booking + Idempotency COMPLETED + OutboxEvent
     * aynı PostgreSQL transaction içerisinde yazılır.
     */
    private BookingResponse processOwnedBooking(
            String idempotencyKey,
            CreateBookingRequest request
    ) {

        TransactionTemplate transactionTemplate =
                new TransactionTemplate(
                        transactionManager
                );

        return transactionTemplate.execute(
                status -> {

                    Seat seat = seatRepository
                            .findByIdForUpdate(
                                    request.seatId()
                            )
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
                                    .userId(
                                            request.userId()
                                    )
                                    .build();

                    Booking savedBooking =
                            bookingRepository
                                    .saveAndFlush(
                                            booking
                                    );

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
                            .saveAndFlush(
                                    record
                            );

                    /*
                     * Kafka publish artık burada
                     * yapılmıyor.
                     *
                     * Event aynı DB transaction'ında
                     * Outbox'a yazılıyor.
                     */
                    outboxService.saveBookingCreatedEvent(
                            savedBooking
                    );

                    return toBookingResponse(
                            savedBooking
                    );
                }
        );
    }

    /**
     * Idempotency record'u kısa ve bağımsız bir
     * transaction içerisinde okur.
     */
    private IdempotencySnapshot
    findIdempotencySnapshot(
            String idempotencyKey
    ) {

        TransactionTemplate transactionTemplate =
                requiresNewTransactionTemplate();

        return transactionTemplate.execute(
                status ->
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

    /**
     * Daha önce tamamlanan idempotent operation'ın
     * mevcut booking sonucunu döndürür.
     */
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

        return transactionTemplate.execute(
                status -> {

                    Booking booking =
                            bookingRepository
                                    .findById(
                                            bookingId
                                    )
                                    .orElseThrow(() ->
                                            new IllegalStateException(
                                                    "Booking linked to idempotency record not found"
                                            )
                                    );

                    return toBookingResponse(
                            booking
                    );
                }
        );
    }

    /**
     * Owner operation başarısız olursa
     * PROCESSING claim'i temizler.
     */
    private void releaseIdempotencyClaim(
            String idempotencyKey
    ) {

        TransactionTemplate transactionTemplate =
                requiresNewTransactionTemplate();

        transactionTemplate.executeWithoutResult(
                status -> {

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
                }
        );
    }

    private void validateRequestHash(
            String existingHash,
            String incomingHash
    ) {

        if (!existingHash.equals(
                incomingHash
        )) {

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

        } catch (
                InterruptedException exception
        ) {

            Thread.currentThread()
                    .interrupt();

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

        transactionTemplate
                .setPropagationBehavior(
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
                    .formatHex(
                            hash
                    );

        } catch (
                NoSuchAlgorithmException exception
        ) {

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