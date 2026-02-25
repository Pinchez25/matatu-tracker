package org.matatu.tracker.model;

import java.math.BigDecimal;
import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonFormat;

/**
 * Represents the total fare revenue collected by a SACCO within a time window. Emitted to {@code
 * matatu.sacco.revenue} at the end of each tumbling window.
 *
 * <p>KEY CONCEPT — Windowed aggregation output: Unlike {@link PassengerCount} which is a running
 * total, this record is scoped to a specific time window [windowStart, windowEnd). Each window
 * produces exactly one record per SACCO once the window closes.
 *
 * <p>The window boundaries are included so downstream consumers (e.g. a dashboard or accounting
 * system) know exactly which time period the revenue covers without needing to query Kafka
 * internals.
 */
public record SaccoRevenueWindow(
        String saccoId,
        String saccoName,
        BigDecimal totalRevenueKes,
        long transactionCount,
        @JsonFormat(shape = JsonFormat.Shape.STRING) Instant windowStart,
        @JsonFormat(shape = JsonFormat.Shape.STRING) Instant windowEnd,
        @JsonFormat(shape = JsonFormat.Shape.STRING) Instant computedAt) {}
