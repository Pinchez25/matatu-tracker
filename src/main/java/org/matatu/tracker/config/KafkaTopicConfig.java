package org.matatu.tracker.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.matatu.tracker.topics.Topics;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import lombok.RequiredArgsConstructor;

/**
 * Declares Kafka topics as Spring beans.
 *
 * <p>Spring Kafka will auto-create these topics on startup via KafkaAdmin (which Spring Boot
 * autoconfigures for you).
 *
 * <p>KEY CONCEPTS introduced here: - Partitions: splitting a topic into parallel lanes. We
 * partition by routeId so each route's events are ordered independently. - Replication factor: how
 * many broker copies exist. Use 1 for local dev, 3+ in production.
 */
@Configuration
@RequiredArgsConstructor
public class KafkaTopicConfig {

    private final MatatuTrackerProperties properties;

    @Bean
    public NewTopic locationTopic() {
        return build(Topics.MATATU_LOCATION);
    }

    @Bean
    public NewTopic faresTopic() {
        return build(Topics.MATATU_FARES);
    }

    @Bean
    public NewTopic speedAlertsTopic() {
        return build(Topics.MATATU_SPEED_ALERTS);
    }

    @Bean
    public NewTopic enrichedLocationTopic() {
        return build(Topics.MATATU_LOCATION_ENRICHED);
    }

    @Bean
    public NewTopic failedFaresTopic() {
        return build(Topics.MATATU_FARES_FAILED);
    }

    private NewTopic build(String name) {
        return TopicBuilder.name(name)
                .partitions(properties.getKafka().getPartitions())
                .replicas(properties.getKafka().getReplicationFactor())
                .build();
    }
}
