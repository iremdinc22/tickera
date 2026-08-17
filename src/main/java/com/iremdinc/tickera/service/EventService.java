package com.iremdinc.tickera.service;

import com.iremdinc.tickera.dto.CreateEventRequest;
import com.iremdinc.tickera.dto.EventResponse;
import com.iremdinc.tickera.dto.SeatResponse;
import com.iremdinc.tickera.entity.Event;
import com.iremdinc.tickera.entity.Seat;
import com.iremdinc.tickera.enums.SeatStatus;
import com.iremdinc.tickera.repository.EventRepository;
import com.iremdinc.tickera.repository.SeatRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@RequiredArgsConstructor
public class EventService {

    private final EventRepository eventRepository;
    private final SeatRepository seatRepository;

    @Transactional
    public EventResponse createEvent(CreateEventRequest request) {

        Event event = Event.builder()
                .name(request.name())
                .venue(request.venue())
                .eventDate(request.eventDate())
                .build();

        List<Seat> seats = request.seatNumbers()
                .stream()
                .map(seatNumber -> Seat.builder()
                        .event(event)
                        .seatNumber(seatNumber)
                        .status(SeatStatus.AVAILABLE)
                        .build())
                .toList();

        event.setSeats(seats);

        Event savedEvent = eventRepository.save(event);

        return toResponse(savedEvent);
    }

    @Transactional(readOnly = true)
    public List<SeatResponse> getSeatsByEventId(Long eventId) {
        return seatRepository.findByEventId(eventId)
                .stream()
                .map(seat -> new SeatResponse(
                        seat.getId(),
                        seat.getSeatNumber(),
                        seat.getStatus()
                ))
                .toList();
    }

    private EventResponse toResponse(Event event) {

        List<SeatResponse> seatResponses = event.getSeats()
                .stream()
                .map(seat -> new SeatResponse(
                        seat.getId(),
                        seat.getSeatNumber(),
                        seat.getStatus()
                ))
                .toList();

        return new EventResponse(
                event.getId(),
                event.getName(),
                event.getVenue(),
                event.getEventDate(),
                seatResponses
        );
    }
}