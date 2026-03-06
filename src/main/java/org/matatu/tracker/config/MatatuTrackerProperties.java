package org.matatu.tracker.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import lombok.Data;
import lombok.Getter;

@Getter
@Configuration
@ConfigurationProperties(prefix = "app")
public class MatatuTrackerProperties {

    private final Kafka kafka = new Kafka();
    private final Streams streams = new Streams();

    @Data
    public static class Kafka {
        private String bootstrapServers;
        private int partitions = 3;
        private short replicationFactor = 1;
    }

    @Data
    public static class Streams {
        private String applicationId = "matatu-streams-app";
        private double speedThresholdKmh = 80.0;
        private int revenueWindowMinutes = 60;
        private int revenueGraceMinutes = 5;
        private long offgridSilenceMs = 300000;
        private long offgridCheckIntervalMs = 60000;
    }
}
