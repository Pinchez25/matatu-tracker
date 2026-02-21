package org.matatu.tracker.controller;

import java.math.BigDecimal;
import java.time.Instant;

import org.matatu.tracker.model.FareEvent;
import org.matatu.tracker.model.FareEvent.PaymentMethod;
import org.matatu.tracker.model.FareEvent.PaymentStatus;
import org.matatu.tracker.model.LocationEvent;
import org.matatu.tracker.producer.FareEventProducer;
import org.matatu.tracker.producer.LocationEventProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import lombok.RequiredArgsConstructor;

/**
 * HTTP endpoints so you can manually trigger Kafka events without waiting for the simulator. Great
 * for learning and debugging.
 *
 * <p>Delegates to the producer services rather than KafkaTemplate directly — the same separation of
 * concerns you'd use in any layered Spring app.
 */
@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
public class MatatuController {

    private final LocationEventProducer locationProducer;
    private final FareEventProducer fareProducer;

    /**
     * POST /api/v1/location
     *
     * <p>curl -X POST http://localhost:8080/api/v1/location \ -H "Content-Type: application/json" \
     * -d '{"matatuId":"KBZ 123A","routeId":"route_33","routeName":"Route 33",
     * "latitude":-1.2921,"longitude":36.8219,"speedKmh":55.0,"passengersOnboard":20}'
     */
    @PostMapping("/location")
    public ResponseEntity<String> publishLocation(@RequestBody LocationEventRequest req) {
        var event =
                new LocationEvent(
                        req.matatuId(),
                        req.routeId(),
                        req.routeName(),
                        req.latitude(),
                        req.longitude(),
                        req.speedKmh(),
                        req.passengersOnboard(),
                        Instant.now());
        locationProducer.send(event);
        return ResponseEntity.accepted()
                .body("Location event queued for matatu: " + req.matatuId());
    }

    /**
     * POST /api/v1/fare
     *
     * <p>curl -X POST http://localhost:8080/api/v1/fare \ -H "Content-Type: application/json" \ -d
     * '{"matatuId":"KBZ 123A","amountKes":50,"paymentMethod":"MPESA","status":"SUCCESS"}'
     */
    @PostMapping("/fare")
    public ResponseEntity<String> publishFare(@RequestBody FareEventRequest req) {
        var event =
                new FareEvent(
                        "TXN-MANUAL-" + System.currentTimeMillis(),
                        req.matatuId(),
                        "PAX-MANUAL",
                        BigDecimal.valueOf(req.amountKes()),
                        PaymentMethod.valueOf(req.paymentMethod()),
                        PaymentStatus.valueOf(req.status()),
                        Instant.now());
        fareProducer.send(event);
        return ResponseEntity.accepted().body("Fare event queued: " + event.transactionId());
    }

    // ── Request records ────────────────────────────────────────────────────

    public record LocationEventRequest(
            String matatuId,
            String routeId,
            String routeName,
            double latitude,
            double longitude,
            double speedKmh,
            int passengersOnboard) {}

    public record FareEventRequest(
            String matatuId, double amountKes, String paymentMethod, String status) {}
}
