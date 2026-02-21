package org.matatu.tracker.model;

import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonFormat;

/**
 * Immutable GPS location event produced by a matatu.
 *
 * <p>Using a Java record gives us: - Immutability by default - Auto-generated
 * equals/hashCode/toString - Compact, readable data carrier
 *
 * <p>The {@code routeId} is used as the Kafka message key so that all location events for the same
 * route land in the same partition, guaranteeing ordering per route. This is a fundamental Kafka
 * concept you'll observe first-hand in Phase 1.
 */
public record LocationEvent(
        String matatuId,
        String routeId,
        String routeName,
        double latitude,
        double longitude,
        double speedKmh,
        int passengersOnboard,
        @JsonFormat(shape = JsonFormat.Shape.STRING) Instant timestamp) {
    /** Compact constructor for basic validation */
    public LocationEvent {
        if (matatuId == null || matatuId.isBlank())
            throw new IllegalArgumentException("matatuId must not be blank");
        if (routeId == null || routeId.isBlank())
            throw new IllegalArgumentException("routeId must not be blank");
        if (speedKmh < 0) throw new IllegalArgumentException("speedKmh must not be negative");
    }
}
