package org.matatu.tracker.model;

import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonFormat;

public record EnrichedLocationEvent(
        String matatuId,
        String routeId,
        String routeName,
        String saccoName,
        String saccoId,
        String terminus,
        double latitude,
        double longitude,
        double speedKmh,
        int passengersOnboard,
        boolean isSpeeding,
        @JsonFormat(shape = JsonFormat.Shape.STRING) Instant originalTimestamp,
        @JsonFormat(shape = JsonFormat.Shape.STRING) Instant enrichedAt) {}
