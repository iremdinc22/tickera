package com.iremdinc.tickera.controller;

import com.iremdinc.tickera.dto.CreateEventRequest;
import com.iremdinc.tickera.dto.EventResponse;
import com.iremdinc.tickera.dto.SeatResponse;
import com.iremdinc.tickera.service.EventService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/events")
@RequiredArgsConstructor
public class EventController {

    private final EventService eventService;

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public EventResponse createEvent(
            @Valid @RequestBody CreateEventRequest request
    ) {
        return eventService.createEvent(request);
    }

    @GetMapping("/{eventId}/seats")
    public List<SeatResponse> getSeatsByEventId(
            @PathVariable Long eventId
    ) {
        return eventService.getSeatsByEventId(eventId);
    }
}