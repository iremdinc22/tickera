package com.iremdinc.tickera.repository;

import com.iremdinc.tickera.entity.Event;
import org.springframework.data.jpa.repository.JpaRepository;

public interface EventRepository extends JpaRepository<Event, Long> {
}