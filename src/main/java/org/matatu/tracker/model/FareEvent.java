package org.matatu.tracker.model;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Immutable fare payment event (M-Pesa style).
 * <p>
 * {@code matatuId} is the Kafka message key so all fares for the same
 * matatu are co-located in the same partition.
 */
public record FareEvent(
        String transactionId,
        String matatuId,
        String passengerId,
        BigDecimal amountKes,
        PaymentMethod paymentMethod,
        PaymentStatus status,

        @JsonFormat(shape = JsonFormat.Shape.STRING)
        Instant timestamp
) {
    public enum PaymentMethod { MPESA, CASH, CARD }
    public enum PaymentStatus { SUCCESS, FAILED, PENDING }
}