package com.iremdinc.tickera.entity;

import com.iremdinc.tickera.enums.OutboxStatus;
import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(
        name = "outbox_events",
        indexes = {
                @Index(
                        name = "idx_outbox_status_created_at",
                        columnList = "status, created_at"
                ),
                @Index(
                        name = "idx_outbox_status_next_retry_at",
                        columnList = "status, next_retry_at"
                ),
                @Index(
                        name = "idx_outbox_status_processing_started_at",
                        columnList = "status, processing_started_at"
                )
        }
)
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OutboxEvent {

    @Id
    private UUID id;

    @Column(
            name = "aggregate_type",
            nullable = false,
            length = 50
    )
    private String aggregateType;

    @Column(
            name = "aggregate_id",
            nullable = false,
            length = 100
    )
    private String aggregateId;

    @Column(
            name = "event_type",
            nullable = false,
            length = 100
    )
    private String eventType;

    @Column(
            name = "payload",
            nullable = false,
            columnDefinition = "TEXT"
    )
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(
            name = "status",
            nullable = false,
            length = 20
    )
    @Builder.Default
    private OutboxStatus status =
            OutboxStatus.PENDING;

    @Column(
            name = "retry_count",
            nullable = false
    )
    @Builder.Default
    private Integer retryCount = 0;

    @Column(
            name = "next_retry_at"
    )
    private LocalDateTime nextRetryAt;

    @Column(
            name = "processing_started_at"
    )
    private LocalDateTime processingStartedAt;

    @Column(
            name = "last_error",
            columnDefinition = "TEXT"
    )
    private String lastError;

    @Column(
            name = "created_at",
            nullable = false,
            updatable = false
    )
    @Builder.Default
    private LocalDateTime createdAt =
            LocalDateTime.now();

    @Column(
            name = "published_at"
    )
    private LocalDateTime publishedAt;
}