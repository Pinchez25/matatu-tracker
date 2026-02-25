package org.matatu.tracker.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.matatu.tracker.topics.Topics;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

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
    public KafkaAdmin.NewTopics matatuTopics() {
        int partitions = properties.getKafka().getPartitions();
        short replicas = properties.getKafka().getReplicationFactor();

        return new KafkaAdmin.NewTopics(
                build(Topics.MATATU_LOCATION, partitions, replicas),
                build(Topics.MATATU_FARES, partitions, replicas),
                build(Topics.MATATU_SPEED_ALERTS, partitions, replicas),
                build(Topics.MATATU_LOCATION_ENRICHED, partitions, replicas),
                build(Topics.MATATU_FARES_FAILED, partitions, replicas),
                build(Topics.MATATU_PASSENGER_COUNTS, partitions, replicas),
                build(Topics.MATATU_SACCO_REVENUE, partitions, replicas),
                build(Topics.MATATU_OFFGRID_ALERTS, partitions, replicas),
                build(Topics.MATATU_ROUTE_OCCUPANCY, partitions, replicas));
    }

    private NewTopic build(String name, int partitions, short replicas) {
        return TopicBuilder.name(name).partitions(partitions).replicas(replicas).build();
    }
}
