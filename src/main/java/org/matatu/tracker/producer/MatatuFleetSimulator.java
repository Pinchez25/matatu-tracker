package org.matatu.tracker.producer;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.matatu.tracker.model.FareEvent;
import org.matatu.tracker.model.FareEvent.PaymentMethod;
import org.matatu.tracker.model.FareEvent.PaymentStatus;
import org.matatu.tracker.model.LocationEvent;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;

/**
 * Simulates a fleet of Nairobi matatus broadcasting GPS pings and fare payments.
 *
 * <p>In a real system each physical matatu device would be its own Kafka producer. Here we simulate
 * the entire fleet from a single scheduled method — the Kafka concepts are identical.
 *
 * <p>Notice this class delegates to {@link LocationEventProducer} and {@link FareEventProducer}
 * rather than touching KafkaTemplate directly. This is the right layering: Simulator (what to send)
 * → Producer service (how to send) → KafkaTemplate (send it)
 *
 * <p>KEY CONCEPT — Partitioning in action: Watch the logs and notice that events for the same
 * routeId always land on the same partition number. That's Kafka's key-based partitioning at work.
 */
@Component
@RequiredArgsConstructor
public class MatatuFleetSimulator {

    //    private static final Logger log = LoggerFactory.getLogger(MatatuFleetSimulator.class);
    private static final Random RANDOM = new Random();

    private final LocationEventProducer locationProducer;
    private final FareEventProducer fareProducer;
    private final AtomicInteger fareCounter = new AtomicInteger(1000);

    // Our simulated Nairobi routes
    private static final List<RouteInfo> ROUTES =
            List.of(
                    new RouteInfo(
                            "route_33",
                            "Route 33",
                            "CBD → Kikuyu",
                            new double[] {-1.2921, 36.8219}),
                    new RouteInfo(
                            "route_23",
                            "Route 23",
                            "CBD → Westlands",
                            new double[] {-1.2650, 36.8100}),
                    new RouteInfo(
                            "route_58",
                            "Route 58",
                            "CBD → Kawangware",
                            new double[] {-1.2800, 36.7500}),
                    new RouteInfo(
                            "route_111",
                            "Route 111",
                            "CBD → Rongai",
                            new double[] {-1.4200, 36.7800}),
                    new RouteInfo(
                            "route_46",
                            "Route 46",
                            "CBD → Eastleigh",
                            new double[] {-1.2780, 36.8450}));

    // Simulated matatus — each belongs to a route
    private static final List<MatatuInfo> MATATUS =
            List.of(
                    new MatatuInfo("KBZ 123A", "route_33"),
                    new MatatuInfo("KDA 456B", "route_33"),
                    new MatatuInfo("KCX 789C", "route_23"),
                    new MatatuInfo("KDF 321D", "route_58"),
                    new MatatuInfo("KCB 654E", "route_111"),
                    new MatatuInfo("KDG 987F", "route_46"));

    /**
     * Fires every 3 seconds, sending a GPS ping for each matatu in the fleet. Adjust the rate in
     * application.yml via app.simulator.gps-interval-ms.
     */
    @Scheduled(fixedRateString = "${app.simulator.gps-interval-ms:3000}")
    public void broadcastGpsLocations() {
        MATATUS.forEach(
                matatu -> {
                    RouteInfo route = findRoute(matatu.routeId());
                    LocationEvent event =
                            new LocationEvent(
                                    matatu.id(),
                                    route.id(),
                                    route.name(),
                                    jitter(route.baseCoords()[0], 0.01), // slight random movement
                                    jitter(route.baseCoords()[1], 0.01),
                                    RANDOM.nextDouble(20, 90), // speed between 20–90 km/h
                                    RANDOM.nextInt(1, 34), // 1–33 passengers
                                    Instant.now());
                    locationProducer.send(event);
                });
    }

    /**
     * Fires every 5 seconds, simulating fare payments across the fleet. Occasionally generates a
     * FAILED payment to make the logs interesting.
     */
    @Scheduled(fixedRateString = "${app.simulator.fare-interval-ms:5000}")
    public void broadcastFarePayments() {
        MATATUS.stream()
                .flatMap(matatu -> Stream.of(buildFare(matatu), buildFare(matatu)))
                .forEach(fareProducer::send);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private FareEvent buildFare(MatatuInfo matatu) {
        boolean failed = RANDOM.nextInt(10) == 0; // 10% failure rate
        return new FareEvent(
                "TXN-%d".formatted(fareCounter.incrementAndGet()),
                matatu.id(),
                "PAX-%d".formatted(RANDOM.nextInt(1, 500)),
                BigDecimal.valueOf(RANDOM.nextInt(30, 150)),
                RANDOM.nextBoolean() ? PaymentMethod.MPESA : PaymentMethod.CASH,
                failed ? PaymentStatus.FAILED : PaymentStatus.SUCCESS,
                Instant.now());
    }

    private RouteInfo findRoute(String routeId) {
        return ROUTES.stream()
                .filter(r -> r.id().equals(routeId))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Unknown route: " + routeId));
    }

    private double jitter(double base, double maxDelta) {
        return base + (RANDOM.nextDouble() * 2 * maxDelta) - maxDelta;
    }

    // ── Internal data carriers ────────────────────────────────────────────────

    private record RouteInfo(String id, String name, String description, double[] baseCoords) {}

    private record MatatuInfo(String id, String routeId) {}
}
