package com.iremdinc.tickera.event;

import com.iremdinc.tickera.entity.ProcessedEvent;
import com.iremdinc.tickera.repository.ProcessedEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

@Component
@RequiredArgsConstructor
@Slf4j
public class BookingCreatedEventConsumer {

    private static final String EVENT_TYPE =
            "BOOKING_CREATED";

    private final ProcessedEventRepository processedEventRepository;

    @KafkaListener(
            topics = "booking-created",
            groupId = "tickera-booking-consumers"
    )
    @Transactional
    public void consume(
            BookingCreatedEvent event
    ) {

        /*
         * Fast-path duplicate check.
         *
         * Aynı event daha önce başarılı şekilde
         * işlendi ise tekrar işlemiyoruz.
         */
        if (processedEventRepository.existsById(
                event.eventId()
        )) {

            log.info(
                    "Duplicate BookingCreatedEvent ignored: eventId={}, bookingId={}",
                    event.eventId(),
                    event.bookingId()
            );

            return;
        }

        try {

            /*
             * Gerçek bir sistemde notification,
             * analytics, audit vb. business logic
             * burada çalışabilir.
             *
             * Şimdilik event'i logluyoruz.
             */
            log.info(
                    "Processing BookingCreatedEvent: eventId={}, bookingId={}, seatId={}, userId={}, status={}",
                    event.eventId(),
                    event.bookingId(),
                    event.seatId(),
                    event.userId(),
                    event.bookingStatus()
            );

            /*
             * İşlem tamamlandıktan sonra eventId'yi
             * processed_events tablosuna kaydediyoruz.
             *
             * eventId primary key olduğu için aynı
             * event concurrent olarak ikinci kez
             * insert edilmeye çalışılırsa DB engeller.
             */
            ProcessedEvent processedEvent =
                    ProcessedEvent.builder()
                            .eventId(
                                    event.eventId()
                            )
                            .eventType(
                                    EVENT_TYPE
                            )
                            .processedAt(
                                    LocalDateTime.now()
                            )
                            .build();

            processedEventRepository.saveAndFlush(
                    processedEvent
            );

            log.info(
                    "BookingCreatedEvent processed successfully: eventId={}, bookingId={}",
                    event.eventId(),
                    event.bookingId()
            );

        } catch (
                DataIntegrityViolationException exception
        ) {

            /*
             * Concurrent duplicate delivery durumunda
             * başka consumer aynı eventId'yi bizden
             * önce kaydetmiş olabilir.
             */
            log.info(
                    "Concurrent duplicate BookingCreatedEvent ignored: eventId={}, bookingId={}",
                    event.eventId(),
                    event.bookingId()
            );
        }
    }
}