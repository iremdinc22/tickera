package com.iremdinc.tickera.service;

import com.iremdinc.tickera.entity.OutboxEvent;
import com.iremdinc.tickera.enums.OutboxStatus;
import com.iremdinc.tickera.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
@RequiredArgsConstructor
public class OutboxAdminService {

    private final OutboxEventRepository outboxEventRepository;

    @Transactional
    public OutboxEvent requeueFailedEvent(
            UUID eventId
    ) {

        OutboxEvent event =
                outboxEventRepository
                        .findById(eventId)
                        .orElseThrow(() ->
                                new IllegalStateException(
                                        "Outbox event not found: "
                                                + eventId
                                )
                        );

        if (event.getStatus()
                != OutboxStatus.FAILED) {

            throw new IllegalStateException(
                    "Only FAILED outbox events can be requeued"
            );
        }

        event.setStatus(
                OutboxStatus.PENDING
        );

        event.setRetryCount(
                0
        );

        event.setNextRetryAt(
                null
        );

        event.setLastError(
                null
        );

        event.setPublishedAt(
                null
        );

        return outboxEventRepository
                .saveAndFlush(
                        event
                );
    }
}