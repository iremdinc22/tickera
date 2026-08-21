package com.iremdinc.tickera.service;

import com.iremdinc.tickera.dto.BookingResponse;
import com.iremdinc.tickera.dto.CreateBookingRequest;
import com.iremdinc.tickera.entity.Booking;
import com.iremdinc.tickera.entity.IdempotencyRecord;
import com.iremdinc.tickera.entity.Seat;
import com.iremdinc.tickera.enums.IdempotencyStatus;
import com.iremdinc.tickera.enums.SeatStatus;
import com.iremdinc.tickera.event.BookingCreatedEvent;
import com.iremdinc.tickera.event.BookingEventPublisher;
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
import java.time.LocalDateTime;
import java.util.HexFormat;
import java.util.UUID;

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
    private final BookingEventPublisher bookingEventPublisher;

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
         * Event yalnızca PostgreSQL transaction
         * başarılı şekilde commit edildikten sonra
         * Kafka'ya gönderilecek.
         */
        publishBookingCreatedAfterCommit(
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
         * bitmeden ortaya çıkmasını sağlıyoruz.
         */
        bookingRepository.flush();

        publishBookingCreatedAfterCommit(
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
         * Redis geçici reservation state'ini yönetiyor.
         *
         * Final booking correctness için PostgreSQL
         * pessimistic lock kullanmaya devam ediyoruz.
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
         * Booking başarıyla oluşturuldu.
         *
         * Redis hold artık gerekli değil.
         *
         * releaseSeat owner-aware ve atomic çalışır.
         */
        seatHoldService.releaseSeat(
                request.seatId(),
                request.userId()
        );

        publishBookingCreatedAfterCommit(
                savedBooking
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
             * Key zaten varsa incoming request'in
             * aynı logical operation olup olmadığını
             * doğruluyoruz.
             */
            if (snapshot != null) {

                validateRequestHash(
                        snapshot.requestHash(),
                        requestHash
                );

                /*
                 * İşlem daha önce tamamlanmışsa
                 * yeni booking ve yeni Kafka event'i
                 * üretmiyoruz.
                 *
                 * Existing sonucu döndürüyoruz.
                 */
                if (snapshot.status()
                        == IdempotencyStatus.COMPLETED) {

                    return findExistingBooking(
                            snapshot.bookingId()
                    );
                }

                /*
                 * PROCESSING ise başka request
                 * bu logical operation'ın owner'ı.
                 */
                waitBeforeRetry();

                continue;
            }

            /*
             * Henüz record yok.
             *
             * Bu request idempotency key'in
             * owner'ı olmaya çalışır.
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
                 * Booking başarısızsa PROCESSING
                 * claim sonsuza kadar bırakılmaz.
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
     * REQUIRES_NEW kullanıldığı için PROCESSING
     * record hemen commit edilir ve concurrent
     * request'ler tarafından görülebilir.
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
             * Başka bir request key'i bizden
             * önce claim etti.
             */
            return false;
        }
    }

    /**
     * Idempotency owner request gerçek booking'i
     * burada gerçekleştirir.
     *
     * Booking + COMPLETED state aynı PostgreSQL
     * transaction içerisinde commit edilir.
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
                     * Sadece gerçek owner request
                     * BookingCreatedEvent üretir.
                     *
                     * Aynı idempotency key ile gelen
                     * retry'lar tekrar event üretmez.
                     */
                    publishBookingCreatedAfterCommit(
                            savedBooking
                    );

                    return toBookingResponse(
                            savedBooking
                    );
                }
        );
    }

    /**
     * Idempotency record'u kısa ve bağımsız
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
     * Daha önce tamamlanmış idempotent operation'ın
     * booking sonucunu döndürür.
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
     * Owner operation başarısız olduğunda
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

    /**
     * BookingCreatedEvent'i yalnızca PostgreSQL
     * transaction commit başarılı olduktan sonra
     * Kafka'ya gönderir.
     */
    private void publishBookingCreatedAfterCommit(
            Booking booking
    ) {

        BookingCreatedEvent event =
                new BookingCreatedEvent(
                        UUID.randomUUID(),
                        booking.getId(),
                        booking.getSeat().getId(),
                        booking.getSeat().getSeatNumber(),
                        booking.getUserId(),
                        booking.getStatus().name(),
                        LocalDateTime.now()
                );

        /*
         * Aktif transaction varsa event'i hemen
         * göndermiyoruz.
         */
        if (TransactionSynchronizationManager
                .isSynchronizationActive()) {

            TransactionSynchronizationManager
                    .registerSynchronization(
                            new TransactionSynchronization() {

                                @Override
                                public void afterCommit() {

                                    bookingEventPublisher
                                            .publish(
                                                    event
                                            );
                                }
                            }
                    );

            return;
        }

        /*
         * Transaction dışında çağrılırsa
         * doğrudan publish edilir.
         */
        bookingEventPublisher.publish(
                event
        );
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