package org.matatu.tracker.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.matatu.tracker.topics.Topics;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares Kafka topics as Spring beans.
 * <p>
 * Spring Kafka will auto-create these topics on startup via KafkaAdmin
 * (which Spring Boot autoconfigures for you).
 * <p>
 * KEY CONCEPTS introduced here:
 *  - Partitions: splitting a topic into parallel lanes. We partition by routeId
 *    so each route's events are ordered independently.
 *  - Replication factor: how many broker copies exist. Use 1 for local dev,
 *    3+ in production.
 */
@Configuration
public class KafkaTopicConfig {

    @Value("${app.kafka.partitions:3}")
    private int partitions;

    @Value("${app.kafka.replication-factor:1}")
    private short replicationFactor;

    @Bean
    public NewTopic locationTopic() {
        return TopicBuilder.name(Topics.MATATU_LOCATION)
                .partitions(partitions)
                .replicas(replicationFactor)
                .build();
    }

    @Bean
    public NewTopic faresTopic() {
        return TopicBuilder.name(Topics.MATATU_FARES)
                .partitions(partitions)
                .replicas(replicationFactor)
                .build();
    }
}