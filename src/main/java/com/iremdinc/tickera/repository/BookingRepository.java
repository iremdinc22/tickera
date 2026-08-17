package com.iremdinc.tickera.repository;

import com.iremdinc.tickera.entity.Booking;
import org.springframework.data.jpa.repository.JpaRepository;

public interface BookingRepository extends JpaRepository<Booking, Long> {
}