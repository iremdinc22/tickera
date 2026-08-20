package com.iremdinc.tickera.service;

import com.iremdinc.tickera.dto.CreateBookingRequest;
import com.iremdinc.tickera.entity.Event;
import com.iremdinc.tickera.entity.Seat;
import com.iremdinc.tickera.enums.SeatStatus;
import com.iremdinc.tickera.exception.SeatNotAvailableException;
import com.iremdinc.tickera.repository.BookingRepository;
import com.iremdinc.tickera.repository.EventRepository;
import com.iremdinc.tickera.repository.SeatRepository;
import com.iremdinc.tickera.support.PostgresIntegrationTest;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.orm.ObjectOptimisticLockingFailureException;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@SpringBootTest
class BookingConcurrencyIntegrationTest
        extends PostgresIntegrationTest {

    private static final int THREAD_COUNT = 20;

    @Autowired
    private BookingService bookingService;

    @Autowired
    private BookingRepository bookingRepository;

    @Autowired
    private SeatRepository seatRepository;

    @Autowired
    private EventRepository eventRepository;

    private Long seatId;

    @BeforeEach
    void setUp() {

        bookingRepository.deleteAll();
        seatRepository.deleteAll();
        eventRepository.deleteAll();

        Event event = Event.builder()
                .name("Concurrency Test Event")
                .venue("Test Venue")
                .eventDate(LocalDateTime.now().plusDays(1))
                .build();

        event = eventRepository.saveAndFlush(event);

        Seat seat = Seat.builder()
                .event(event)
                .seatNumber("A1")
                .status(SeatStatus.AVAILABLE)
                .build();

        seat = seatRepository.saveAndFlush(seat);

        seatId = seat.getId();
    }

    @Test
    void shouldAllowOnlyOneBookingWithPessimisticLock()
            throws InterruptedException {

        ExecutorService executor =
                Executors.newFixedThreadPool(THREAD_COUNT);

        try {

            CountDownLatch readyLatch =
                    new CountDownLatch(THREAD_COUNT);

            CountDownLatch startLatch =
                    new CountDownLatch(1);

            List<Future<Boolean>> futures =
                    new ArrayList<>();

            for (int i = 0; i < THREAD_COUNT; i++) {

                String userId = "user-" + i;

                futures.add(
                        executor.submit(() -> {

                            readyLatch.countDown();

                            startLatch.await();

                            try {

                                bookingService.createBooking(
                                        new CreateBookingRequest(
                                                seatId,
                                                userId
                                        )
                                );

                                return true;

                            } catch (SeatNotAvailableException exception) {

                                return false;
                            }
                        })
                );
            }

            readyLatch.await();
            startLatch.countDown();

            int successCount = 0;
            int failureCount = 0;

            for (Future<Boolean> future : futures) {

                try {

                    boolean successful =
                            future.get();

                    if (successful) {
                        successCount++;
                    } else {
                        failureCount++;
                    }

                } catch (ExecutionException exception) {

                    fail(
                            "Unexpected exception: "
                                    + exception.getCause()
                    );
                }
            }

            assertEquals(
                    1,
                    successCount,
                    "Exactly one pessimistic booking should succeed"
            );

            assertEquals(
                    THREAD_COUNT - 1,
                    failureCount,
                    "All other pessimistic booking attempts should fail"
            );

            assertEquals(
                    1,
                    bookingRepository.count(),
                    "Database must contain exactly one booking"
            );

            Seat seat = seatRepository.findById(seatId)
                    .orElseThrow();

            assertEquals(
                    SeatStatus.BOOKED,
                    seat.getStatus(),
                    "Seat must be BOOKED"
            );

        } finally {

            shutdownExecutor(executor);
        }
    }

    @Test
    void shouldAllowOnlyOneBookingWithOptimisticLock()
            throws InterruptedException {

        ExecutorService executor =
                Executors.newFixedThreadPool(THREAD_COUNT);

        try {

            CountDownLatch readyLatch =
                    new CountDownLatch(THREAD_COUNT);

            CountDownLatch startLatch =
                    new CountDownLatch(1);

            List<Future<Boolean>> futures =
                    new ArrayList<>();

            for (int i = 0; i < THREAD_COUNT; i++) {

                String userId =
                        "optimistic-user-" + i;

                futures.add(
                        executor.submit(() -> {

                            readyLatch.countDown();

                            startLatch.await();

                            try {

                                bookingService
                                        .createBookingOptimistic(
                                                new CreateBookingRequest(
                                                        seatId,
                                                        userId
                                                )
                                        );

                                return true;

                            } catch (
                                    SeatNotAvailableException |
                                    ObjectOptimisticLockingFailureException exception
                            ) {

                                return false;
                            }
                        })
                );
            }

            readyLatch.await();
            startLatch.countDown();

            int successCount = 0;
            int failureCount = 0;

            for (Future<Boolean> future : futures) {

                try {

                    boolean successful =
                            future.get();

                    if (successful) {
                        successCount++;
                    } else {
                        failureCount++;
                    }

                } catch (ExecutionException exception) {

                    fail(
                            "Unexpected exception: "
                                    + exception.getCause()
                    );
                }
            }

            assertEquals(
                    1,
                    successCount,
                    "Exactly one optimistic booking should succeed"
            );

            assertEquals(
                    THREAD_COUNT - 1,
                    failureCount,
                    "All other optimistic booking attempts should fail"
            );

            assertEquals(
                    1,
                    bookingRepository.count(),
                    "Database must contain exactly one booking"
            );

            Seat seat = seatRepository.findById(seatId)
                    .orElseThrow();

            assertEquals(
                    SeatStatus.BOOKED,
                    seat.getStatus(),
                    "Seat must be BOOKED"
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