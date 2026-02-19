package org.matatu.tracker.dto;

public record LocationEventRequest(
        String matatuId, String routeId, String routeName,
        double latitude, double longitude, double speedKmh, int passengersOnboard
) {
}
