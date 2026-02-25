package org.matatu.tracker.model;

import java.time.Instant;

// Stored in matatu-snapshot-store keyed by matatuId.
// Remembers each matatu's last reported passenger count so we can compute deltas.
public record MatatuSnapshot(
        String matatuId, String routeId, int lastPassengerCount, Instant lastUpdated) {}
