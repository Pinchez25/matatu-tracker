package org.matatu.tracker.model;

import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonFormat;

/**
 * Alert emitted when a matatu has not sent a GPS ping for longer than the configured silence
 * threshold (default: 5 minutes).
 *
 * <p>Emitted to {@code matatu.offgrid.alerts}.
 *
 * <p>KEY CONCEPT — Punctuator-driven output: Unlike filter/map/aggregate which emit records
 * reactively in response to incoming events, this record is emitted PROACTIVELY by a scheduled
 * punctuator that fires on wall-clock time even when no events are arriving. That is the only way
 * to detect ABSENCE of events — you cannot filter for something that never came.
 */
public record OffGridAlert(
        String matatuId,
        String routeId,
        String routeName,
        double lastKnownLatitude,
        double lastKnownLongitude,
        long silenceDurationMs,
        @JsonFormat(shape = JsonFormat.Shape.STRING) Instant lastSeenAt,
        @JsonFormat(shape = JsonFormat.Shape.STRING) Instant alertRaisedAt) {}
