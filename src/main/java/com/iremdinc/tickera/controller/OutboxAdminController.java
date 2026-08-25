package com.iremdinc.tickera.controller;

import com.iremdinc.tickera.entity.OutboxEvent;
import com.iremdinc.tickera.service.OutboxAdminService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/admin/outbox")
@RequiredArgsConstructor
public class OutboxAdminController {

    private final OutboxAdminService outboxAdminService;

    @PostMapping("/{eventId}/requeue")
    public ResponseEntity<OutboxEvent> requeueFailedEvent(
            @PathVariable UUID eventId
    ) {

        OutboxEvent event =
                outboxAdminService
                        .requeueFailedEvent(
                                eventId
                        );

        return ResponseEntity.ok(
                event
        );
    }
}