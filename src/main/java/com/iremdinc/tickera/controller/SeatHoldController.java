package com.iremdinc.tickera.controller;

import com.iremdinc.tickera.service.SeatHoldService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/seats")
@RequiredArgsConstructor
public class SeatHoldController {

    private final SeatHoldService seatHoldService;

    @PostMapping("/{seatId}/hold")
    public ResponseEntity<String> holdSeat(
            @PathVariable Long seatId,
            @RequestParam String userId
    ) {

        boolean acquired =
                seatHoldService.holdSeat(
                        seatId,
                        userId
                );

        if (!acquired) {
            return ResponseEntity
                    .status(HttpStatus.CONFLICT)
                    .body("Seat is already held");
        }

        return ResponseEntity.ok(
                "Seat held successfully"
        );
    }

    @GetMapping("/{seatId}/hold")
    public ResponseEntity<String> getHoldOwner(
            @PathVariable Long seatId
    ) {

        String owner =
                seatHoldService.getHoldOwner(seatId);

        if (owner == null) {
            return ResponseEntity
                    .notFound()
                    .build();
        }

        return ResponseEntity.ok(owner);
    }

    @DeleteMapping("/{seatId}/hold")
    public ResponseEntity<String> releaseSeat(
            @PathVariable Long seatId,
            @RequestParam String userId
    ) {

        boolean released =
                seatHoldService.releaseSeat(
                        seatId,
                        userId
                );

        if (!released) {
            return ResponseEntity
                    .status(HttpStatus.CONFLICT)
                    .body("Seat hold is not owned by this user");
        }

        return ResponseEntity.ok(
                "Seat hold released successfully"
        );
    }
}