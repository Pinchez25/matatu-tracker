package org.matatu.tracker.model;

import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonFormat;

/**
 * Represents the current passenger count state for a single route. This is the VALUE stored in the
 * KTable and emitted to {@code matatu.passenger.counts}.
 *
 * <p>KEY CONCEPT — Why a dedicated record for aggregation output? A KTable aggregation produces a
 * NEW type of value — it is no longer a raw LocationEvent but a computed summary. Keeping this as
 * its own record makes the data contract between the stream and its consumers explicit.
 *
 * <p>Note the {@code totalBoardings} field — we track a running lifetime total alongside the
 * current snapshot so downstream consumers have richer data without having to maintain their own
 * counters.
 */
public record PassengerCount(
        String routeId,
        String routeName,
        int currentPassengers, // latest snapshot from the most recent GPS ping
        long totalBoardings, // running lifetime total of all passenger counts seen
        long eventCount, // How many GPS pings contributed to this aggregate
        @JsonFormat(shape = JsonFormat.Shape.STRING) Instant lastUpdated) {}
