package org.matatu.tracker.model;

// Internal stream record — carries the computed delta between the processor
// and the route-level aggregator. Never published to an external topic.
public record DeltaEvent(
        String matatuId,
        String routeId,
        String routeName,
        int passengerDelta, // negative means alighted
        int boardings,
        int alightings) {}
