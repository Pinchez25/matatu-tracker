package org.matatu.tracker.model;

import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonFormat;

// Published to matatu.route-occupancy.
// Represents the true real-time state of a route across all matatus.
public record RouteOccupancy(
        String routeId,
        String routeName,
        int currentPassengers, // true total across all active matatus
        long totalBoardings, // cumulative boardings since stream start
        long totalAlightings, // cumulative alightings since stream start
        @JsonFormat(shape = JsonFormat.Shape.STRING) Instant lastUpdated) {}
