package com.iremdinc.tickera.service;

import com.iremdinc.tickera.entity.Seat;
import com.iremdinc.tickera.enums.SeatStatus;
import com.iremdinc.tickera.exception.SeatNotAvailableException;
import com.iremdinc.tickera.repository.SeatRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.List;

@Service
public class SeatHoldService {

    private static final DefaultRedisScript<Long> RELEASE_SCRIPT =
            new DefaultRedisScript<>(
                    """
                    if redis.call('GET', KEYS[1]) == ARGV[1] then
                        return redis.call('DEL', KEYS[1])
                    else
                        return 0
                    end
                    """,
                    Long.class
            );

    private final StringRedisTemplate redisTemplate;
    private final SeatRepository seatRepository;
    private final Duration holdDuration;

    public SeatHoldService(
            StringRedisTemplate redisTemplate,
            SeatRepository seatRepository,
            @Value("${tickera.seat-hold.duration:2m}")
            Duration holdDuration
    ) {
        this.redisTemplate = redisTemplate;
        this.seatRepository = seatRepository;
        this.holdDuration = holdDuration;
    }

    public boolean holdSeat(
            Long seatId,
            String userId
    ) {

        /*
         * Redis'te hold oluşturmadan önce gerçek seat'in
         * PostgreSQL'de var olduğunu doğruluyoruz.
         */
        Seat seat = seatRepository
                .findById(seatId)
                .orElseThrow(() ->
                        new IllegalArgumentException(
                                "Seat not found"
                        )
                );

        /*
         * Zaten BOOKED olan bir seat için geçici
         * Redis hold oluşturulmamalı.
         */
        if (seat.getStatus() != SeatStatus.AVAILABLE) {
            throw new SeatNotAvailableException(
                    "Seat is not available"
            );
        }

        String key = buildKey(seatId);

        /*
         * SET NX + TTL
         *
         * Key yoksa atomik olarak oluşturulur.
         * Başka bir kullanıcı daha önce hold aldıysa
         * işlem false döner.
         */
        Boolean acquired = redisTemplate
                .opsForValue()
                .setIfAbsent(
                        key,
                        userId,
                        holdDuration
                );

        return Boolean.TRUE.equals(acquired);
    }

    public boolean isHeldByUser(
            Long seatId,
            String userId
    ) {

        String currentOwner =
                redisTemplate
                        .opsForValue()
                        .get(buildKey(seatId));

        return userId.equals(currentOwner);
    }

    public String getHoldOwner(
            Long seatId
    ) {

        return redisTemplate
                .opsForValue()
                .get(buildKey(seatId));
    }

    public boolean releaseSeat(
            Long seatId,
            String userId
    ) {

        String key = buildKey(seatId);

        Long result = redisTemplate.execute(
                RELEASE_SCRIPT,
                List.of(key),
                userId
        );

        return result != null && result == 1L;
    }

    private String buildKey(
            Long seatId
    ) {

        return "seat:hold:" + seatId;
    }
}