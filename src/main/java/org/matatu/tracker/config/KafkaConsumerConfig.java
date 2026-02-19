package org.matatu.tracker.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.matatu.tracker.model.FareEvent;
import org.matatu.tracker.model.LocationEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;

import java.util.Map;

/**
 * Configures typed Kafka listener container factories for each event type.
 * <p>
 * KEY CONCEPTS:
 * - ConsumerFactory: creates Kafka Consumer instances with the correct deserializers.
 * - ConcurrentKafkaListenerContainerFactory: the Spring abstraction that manages
 * a pool of consumer threads. The 'concurrency' on @KafkaListener overrides
 * the factory default.
 * - MANUAL_IMMEDIATE ack mode: we acknowledge each message immediately after
 * processing. Later phases will explore batch acks and transactions.
 * - TRUSTED_PACKAGES: required by JsonDeserializer to deserialise our event records.
 * In production you'd lock this down to specific packages only.
 */
@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    // ── Location Events ──────────────────────────────────────────────────────

    @Bean
    public ConsumerFactory<String, LocationEvent> locationConsumerFactory() {
        JacksonJsonDeserializer<LocationEvent> deserializer = new JacksonJsonDeserializer<>(LocationEvent.class, false);
        deserializer.addTrustedPackages("org.matatu.tracker.model");

        return new DefaultKafkaConsumerFactory<>(
                baseConsumerProps(),
                new StringDeserializer(),
                deserializer
        );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, LocationEvent> locationListenerContainerFactory() {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, LocationEvent>();
        factory.setConsumerFactory(locationConsumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);

        return factory;
    }

    // ── Fare Events ───────────────────────────────────────────────────────────

    @Bean
    public ConsumerFactory<String, FareEvent> fareConsumerFactory() {
        JacksonJsonDeserializer<FareEvent> deserializer = new JacksonJsonDeserializer<>(FareEvent.class, false);
        deserializer.addTrustedPackages("org.matatu.tracker.model");

        return new DefaultKafkaConsumerFactory<>(
                baseConsumerProps(),
                new StringDeserializer(),
                deserializer
        );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, FareEvent> fareListenerContainerFactory() {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, FareEvent>();
        factory.setConsumerFactory(fareConsumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        return factory;
    }

    // ── Shared base consumer properties ──────────────────────────────────────

    /**
     * Properties common to all consumers.
     * "earliest" means: when a new consumer group starts, read from the very
     * beginning of the topic — great for development and debugging.
     */
    private Map<String, Object> baseConsumerProps() {
        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false  // we control commits manually via ack mode
        );
    }
}