package com.iremdinc.tickera.repository;

import com.iremdinc.tickera.entity.OutboxEvent;
import com.iremdinc.tickera.enums.OutboxStatus;
import jakarta.persistence.LockModeType;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

public interface OutboxEventRepository
        extends JpaRepository<OutboxEvent, UUID> {

    /**
     * Publish edilmeye uygun PENDING event'leri claim etmek için kullanılır.
     *
     * Event:
     *
     * status = PENDING
     *
     * ve
     *
     * nextRetryAt = NULL
     * veya
     * nextRetryAt <= now
     *
     * ise publish edilmeye uygundur.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("""
            SELECT o
            FROM OutboxEvent o
            WHERE o.status = :status
              AND (
                    o.nextRetryAt IS NULL
                    OR o.nextRetryAt <= :now
                  )
            ORDER BY o.createdAt ASC
            """)
    List<OutboxEvent> findRetryableEventsForUpdate(
            @Param("status") OutboxStatus status,
            @Param("now") LocalDateTime now,
            Pageable pageable
    );

    /**
     * Uzun süredir PROCESSING durumunda kalan event'leri bulur.
     *
     * Örneğin application event'i claim ettikten sonra
     * Kafka publish tamamlanmadan crash ederse kayıt
     * PROCESSING state'inde kalabilir.
     *
     * processingStartedAt < cutoff olan kayıtlar
     * stale kabul edilir.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("""
            SELECT o
            FROM OutboxEvent o
            WHERE o.status = :status
              AND o.processingStartedAt IS NOT NULL
              AND o.processingStartedAt < :cutoff
            ORDER BY o.processingStartedAt ASC
            """)
    List<OutboxEvent> findStaleProcessingEventsForUpdate(
            @Param("status") OutboxStatus status,
            @Param("cutoff") LocalDateTime cutoff,
            Pageable pageable
    );
}