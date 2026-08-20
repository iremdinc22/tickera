package com.iremdinc.tickera.service;

import com.iremdinc.tickera.dto.BookingResponse;
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
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(
        properties = "tickera.seat-hold.duration=30s"
)
class HeldBookingIntegrationTest
        extends PostgresIntegrationTest {

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
    private BookingService bookingService;

    @Autowired
    private SeatHoldService seatHoldService;

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
                .name("Held Booking Integration Test")
                .venue("Test Venue")
                .eventDate(
                        LocalDateTime.now()
                                .plusDays(1)
                )
                .build();

        event = eventRepository
                .saveAndFlush(event);

        Seat seat = Seat.builder()
                .event(event)
                .seatNumber("HOLD-A1")
                .status(SeatStatus.AVAILABLE)
                .build();

        seat = seatRepository
                .saveAndFlush(seat);

        seatId = seat.getId();

        releaseHoldIfExists(seatId);
    }

    @Test
    void shouldAllowHoldOwnerToCreateBookingAndReleaseHold() {

        String userId = "irem";

        /*
         * Step 1:
         * User successfully acquires the Redis hold.
         */
        boolean acquired =
                seatHoldService.holdSeat(
                        seatId,
                        userId
                );

        assertTrue(
                acquired,
                "User should acquire the seat hold"
        );

        assertTrue(
                seatHoldService.isHeldByUser(
                        seatId,
                        userId
                ),
                "Seat should be held by the booking user"
        );

        /*
         * Step 2:
         * Hold owner creates the real booking.
         */
        BookingResponse response =
                bookingService.createHeldBooking(
                        new CreateBookingRequest(
                                seatId,
                                userId
                        )
                );

        assertEquals(
                seatId,
                response.seatId(),
                "Booking should reference the held seat"
        );

        assertEquals(
                userId,
                response.userId(),
                "Booking should belong to the hold owner"
        );

        /*
         * Step 3:
         * Exactly one booking should exist.
         */
        assertEquals(
                1,
                bookingRepository.count(),
                "Exactly one booking should exist"
        );

        /*
         * Step 4:
         * Seat should now be permanently BOOKED
         * in PostgreSQL.
         */
        Seat bookedSeat =
                seatRepository.findById(seatId)
                        .orElseThrow();

        assertEquals(
                SeatStatus.BOOKED,
                bookedSeat.getStatus(),
                "Seat should be BOOKED after successful booking"
        );

        /*
         * Step 5:
         * Redis hold should be removed after the
         * booking succeeds.
         */
        String holdOwner =
                seatHoldService.getHoldOwner(
                        seatId
                );

        assertNull(
                holdOwner,
                "Redis hold should be released after booking"
        );
    }

    @Test
    void shouldRejectBookingWhenSeatIsHeldByAnotherUser() {

        String holdOwner = "irem";
        String otherUser = "ahmet";

        boolean acquired =
                seatHoldService.holdSeat(
                        seatId,
                        holdOwner
                );

        assertTrue(
                acquired,
                "Hold owner should acquire the seat"
        );

        /*
         * Another user tries to bypass the hold
         * and directly create a held booking.
         */
        assertThrows(
                SeatNotAvailableException.class,
                () -> bookingService.createHeldBooking(
                        new CreateBookingRequest(
                                seatId,
                                otherUser
                        )
                )
        );

        /*
         * Booking must not be persisted.
         */
        assertEquals(
                0,
                bookingRepository.count(),
                "Unauthorized user must not create a booking"
        );

        /*
         * PostgreSQL seat should remain AVAILABLE.
         */
        Seat seat =
                seatRepository.findById(seatId)
                        .orElseThrow();

        assertEquals(
                SeatStatus.AVAILABLE,
                seat.getStatus(),
                "Seat should remain AVAILABLE after rejected booking"
        );

        /*
         * The legitimate user's Redis hold must remain intact.
         */
        assertTrue(
                seatHoldService.isHeldByUser(
                        seatId,
                        holdOwner
                ),
                "Rejected booking must not remove the valid hold"
        );

        assertFalse(
                seatHoldService.isHeldByUser(
                        seatId,
                        otherUser
                ),
                "Other user must not become the hold owner"
        );
    }

    @Test
    void shouldRejectBookingWhenNoActiveHoldExists() {

        String userId = "irem";

        assertNull(
                seatHoldService.getHoldOwner(
                        seatId
                ),
                "Seat should not have an active hold"
        );

        assertThrows(
                SeatNotAvailableException.class,
                () -> bookingService.createHeldBooking(
                        new CreateBookingRequest(
                                seatId,
                                userId
                        )
                )
        );

        assertEquals(
                0,
                bookingRepository.count(),
                "Booking must not be created without a hold"
        );

        Seat seat =
                seatRepository.findById(seatId)
                        .orElseThrow();

        assertEquals(
                SeatStatus.AVAILABLE,
                seat.getStatus(),
                "Seat should remain AVAILABLE"
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
