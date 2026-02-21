package org.matatu.tracker.dto;

public record FareEventRequest(
        String matatuId, double amountKes, String paymentMethod, String status) {}
