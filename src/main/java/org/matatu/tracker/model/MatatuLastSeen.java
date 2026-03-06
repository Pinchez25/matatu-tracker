package org.matatu.tracker.model;

import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonFormat;

public record MatatuLastSeen(
        String matatuId,
        String routeId,
        String routeName,
        double lastLatitude,
        double lastLongitude,
        @JsonFormat(shape = JsonFormat.Shape.STRING) Instant lastSeenAt) {}
