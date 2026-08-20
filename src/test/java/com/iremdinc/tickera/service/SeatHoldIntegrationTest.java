package com.iremdinc.tickera.service;

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
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@SpringBootTest(
        properties = "tickera.seat-hold.duration=1s"
)
class SeatHoldIntegrationTest
        extends PostgresIntegrationTest {

    private static final int THREAD_COUNT = 20;

    private static final Long NON_EXISTING_SEAT_ID =
            Long.MAX_VALUE;

    @SuppressWarnings("resource")
    static final GenericContainer<?> redis =
            new GenericContainer<>(
                    DockerImageName.parse(
                            "redis:7.4-alpine"
                    )
            )
                    .withExposedPorts(6379);

    static {
        redis.start();
    }

    @DynamicPropertySource
    static void configureRedis(
            DynamicPropertyRegistry registry
    ) {

        registry.add(
                "spring.data.redis.host",
                redis::getHost
        );

        registry.add(
                "spring.data.redis.port",
                () -> redis.getMappedPort(6379)
        );
    }

    @Autowired
    private SeatHoldService seatHoldService;

    @Autowired
    private SeatRepository seatRepository;

    @Autowired
    private EventRepository eventRepository;

    @Autowired
    private BookingRepository bookingRepository;

    private Long availableSeatId;
    private Long expiringSeatId;
    private Long bookedSeatId;

    @BeforeEach
    void setUp() {

        bookingRepository.deleteAll();
        seatRepository.deleteAll();
        eventRepository.deleteAll();

        Event event = Event.builder()
                .name("Redis Seat Hold Test Event")
                .venue("Test Venue")
                .eventDate(
                        LocalDateTime.now().plusDays(1)
                )
                .build();

        event = eventRepository.saveAndFlush(event);

        /*
         * Concurrent hold testi için kullanılacak
         * gerçek AVAILABLE seat.
         */
        Seat availableSeat = Seat.builder()
                .event(event)
                .seatNumber("REDIS-A1")
                .status(SeatStatus.AVAILABLE)
                .build();

        availableSeat =
                seatRepository.saveAndFlush(
                        availableSeat
                );

        availableSeatId =
                availableSeat.getId();

        /*
         * TTL expiration testi için ayrı seat.
         */
        Seat expiringSeat = Seat.builder()
                .event(event)
                .seatNumber("REDIS-A2")
                .status(SeatStatus.AVAILABLE)
                .build();

        expiringSeat =
                seatRepository.saveAndFlush(
                        expiringSeat
                );

        expiringSeatId =
                expiringSeat.getId();

        /*
         * BOOKED seat validation testi için.
         */
        Seat bookedSeat = Seat.builder()
                .event(event)
                .seatNumber("REDIS-A3")
                .status(SeatStatus.BOOKED)
                .build();

        bookedSeat =
                seatRepository.saveAndFlush(
                        bookedSeat
                );

        bookedSeatId =
                bookedSeat.getId();

        releaseHoldIfExists(
                availableSeatId
        );

        releaseHoldIfExists(
                expiringSeatId
        );

        releaseHoldIfExists(
                bookedSeatId
        );
    }

    @Test
    void shouldAllowOnlyOneUserToHoldSeatConcurrently()
            throws InterruptedException {

        ExecutorService executor =
                Executors.newFixedThreadPool(
                        THREAD_COUNT
                );

        try {

            CountDownLatch readyLatch =
                    new CountDownLatch(
                            THREAD_COUNT
                    );

            CountDownLatch startLatch =
                    new CountDownLatch(1);

            List<Future<Boolean>> futures =
                    new ArrayList<>();

            for (int i = 0; i < THREAD_COUNT; i++) {

                String userId =
                        "redis-user-" + i;

                futures.add(
                        executor.submit(() -> {

                            readyLatch.countDown();

                            startLatch.await();

                            return seatHoldService
                                    .holdSeat(
                                            availableSeatId,
                                            userId
                                    );
                        })
                );
            }

            readyLatch.await();

            startLatch.countDown();

            int successCount = 0;
            int failureCount = 0;

            for (Future<Boolean> future : futures) {

                try {

                    boolean acquired =
                            future.get();

                    if (acquired) {
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
                    "Exactly one user should acquire the seat hold"
            );

            assertEquals(
                    THREAD_COUNT - 1,
                    failureCount,
                    "All other users should fail to acquire the seat hold"
            );

            String owner =
                    seatHoldService.getHoldOwner(
                            availableSeatId
                    );

            assertTrue(
                    owner != null
                            && owner.startsWith(
                                    "redis-user-"
                            ),
                    "Seat should have exactly one valid hold owner"
            );

        } finally {

            executor.shutdown();

            if (!executor.awaitTermination(
                    5,
                    TimeUnit.SECONDS
            )) {

                executor.shutdownNow();
            }
        }
    }

    @Test
    void shouldReleaseSeatAutomaticallyWhenHoldExpires()
            throws InterruptedException {

        String userId = "ttl-user";

        boolean acquired =
                seatHoldService.holdSeat(
                        expiringSeatId,
                        userId
                );

        assertTrue(
                acquired,
                "User should acquire the seat hold"
        );

        assertEquals(
                userId,
                seatHoldService.getHoldOwner(
                        expiringSeatId
                ),
                "Seat should initially be held by the user"
        );

        /*
         * Test config:
         *
         * tickera.seat-hold.duration = 1s
         */
        Thread.sleep(1500);

        assertNull(
                seatHoldService.getHoldOwner(
                        expiringSeatId
                ),
                "Seat hold should disappear after TTL expires"
        );

        /*
         * TTL bittikten sonra farklı bir kullanıcı
         * seat'i tekrar hold edebilmeli.
         */
        boolean acquiredAfterExpiration =
                seatHoldService.holdSeat(
                        expiringSeatId,
                        "next-user"
                );

        assertTrue(
                acquiredAfterExpiration,
                "Seat should become holdable again after expiration"
        );
    }

    @Test
    void shouldRejectHoldForNonExistingSeat() {

        /*
         * PostgreSQL'de bulunmayan bir seat için
         * Redis state oluşturulmamalı.
         */
        assertThrows(
                IllegalArgumentException.class,
                () -> seatHoldService.holdSeat(
                        NON_EXISTING_SEAT_ID,
                        "irem"
                )
        );

        assertNull(
                seatHoldService.getHoldOwner(
                        NON_EXISTING_SEAT_ID
                ),
                "Non-existing seat must not create Redis hold state"
        );
    }

    @Test
    void shouldRejectHoldForBookedSeat() {

        /*
         * Seat PostgreSQL'de var ama zaten BOOKED.
         *
         * Redis'te tekrar geçici reservation
         * oluşturulmamalı.
         */
        assertThrows(
                SeatNotAvailableException.class,
                () -> seatHoldService.holdSeat(
                        bookedSeatId,
                        "irem"
                )
        );

        assertNull(
                seatHoldService.getHoldOwner(
                        bookedSeatId
                ),
                "BOOKED seat must not create Redis hold state"
        );

        Seat bookedSeat =
                seatRepository.findById(
                                bookedSeatId
                        )
                        .orElseThrow();

        assertEquals(
                SeatStatus.BOOKED,
                bookedSeat.getStatus(),
                "Seat should remain BOOKED"
        );
    }

    private void releaseHoldIfExists(
            Long seatId
    ) {

        String currentOwner =
                seatHoldService.getHoldOwner(
                        seatId
                );

        if (currentOwner != null) {

            seatHoldService.releaseSeat(
                    seatId,
                    currentOwner
            );
        }
    }
}