package com.iremdinc.tickera.service;

import com.iremdinc.tickera.dto.BookingResponse;
import com.iremdinc.tickera.dto.CreateBookingRequest;
import com.iremdinc.tickera.entity.Event;
import com.iremdinc.tickera.entity.Seat;
import com.iremdinc.tickera.enums.SeatStatus;
import com.iremdinc.tickera.repository.BookingRepository;
import com.iremdinc.tickera.repository.EventRepository;
import com.iremdinc.tickera.repository.IdempotencyRecordRepository;
import com.iremdinc.tickera.repository.SeatRepository;
import com.iremdinc.tickera.support.PostgresIntegrationTest;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@SpringBootTest
class IdempotencyConcurrencyIntegrationTest
        extends PostgresIntegrationTest {

    private static final int THREAD_COUNT = 20;

    private static final String IDEMPOTENCY_KEY =
            "concurrent-idempotency-key";

    @Autowired
    private BookingService bookingService;

    @Autowired
    private BookingRepository bookingRepository;

    @Autowired
    private IdempotencyRecordRepository idempotencyRecordRepository;

    @Autowired
    private SeatRepository seatRepository;

    @Autowired
    private EventRepository eventRepository;

    private Long seatId;

    @BeforeEach
    void setUp() {

        idempotencyRecordRepository.deleteAll();
        bookingRepository.deleteAll();
        seatRepository.deleteAll();
        eventRepository.deleteAll();

        Event event = Event.builder()
                .name("Idempotency Concurrency Test")
                .venue("Test Venue")
                .eventDate(LocalDateTime.now().plusDays(1))
                .build();

        event = eventRepository.saveAndFlush(event);

        Seat seat = Seat.builder()
                .event(event)
                .seatNumber("IDEM-A1")
                .status(SeatStatus.AVAILABLE)
                .build();

        seat = seatRepository.saveAndFlush(seat);

        seatId = seat.getId();
    }

    @Test
    void shouldProcessConcurrentRequestsWithSameIdempotencyKeyOnlyOnce()
            throws InterruptedException {

        ExecutorService executor =
                Executors.newFixedThreadPool(THREAD_COUNT);

        try {

            CountDownLatch readyLatch =
                    new CountDownLatch(THREAD_COUNT);

            CountDownLatch startLatch =
                    new CountDownLatch(1);

            List<Future<BookingResponse>> futures =
                    new ArrayList<>();

            CreateBookingRequest request =
                    new CreateBookingRequest(
                            seatId,
                            "irem"
                    );

            for (int i = 0; i < THREAD_COUNT; i++) {

                futures.add(
                        executor.submit(() -> {

                            readyLatch.countDown();

                            startLatch.await();

                            return bookingService
                                    .createBookingIdempotent(
                                            IDEMPOTENCY_KEY,
                                            request
                                    );
                        })
                );
            }

            readyLatch.await();
            startLatch.countDown();

            Set<Long> returnedBookingIds =
                    new HashSet<>();

            for (Future<BookingResponse> future : futures) {

                try {

                    BookingResponse response =
                            future.get();

                    returnedBookingIds.add(
                            response.id()
                    );

                } catch (ExecutionException exception) {

                    fail(
                            "Concurrent idempotent request failed: "
                                    + exception.getCause()
                    );
                }
            }

            assertEquals(
                    1,
                    returnedBookingIds.size(),
                    "All requests should return the same booking"
            );

            assertEquals(
                    1,
                    bookingRepository.count(),
                    "Only one booking must exist"
            );

            assertEquals(
                    1,
                    idempotencyRecordRepository.count(),
                    "Only one idempotency record must exist"
            );

            Seat seat = seatRepository.findById(seatId)
                    .orElseThrow();

            assertEquals(
                    SeatStatus.BOOKED,
                    seat.getStatus(),
                    "Seat must end in BOOKED state"
            );

        } finally {

            shutdownExecutor(executor);
        }
    }

    private void shutdownExecutor(
            ExecutorService executor
    ) throws InterruptedException {

        executor.shutdown();

        if (!executor.awaitTermination(
                5,
                TimeUnit.SECONDS
        )) {

            executor.shutdownNow();

            if (!executor.awaitTermination(
                    5,
                    TimeUnit.SECONDS
            )) {
                throw new IllegalStateException(
                        "Executor did not terminate"
                );
            }
        }
    }
}