package org.matatu.tracker.config;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.matatu.tracker.model.EnrichedLocationEvent;
import org.matatu.tracker.model.FareEvent;
import org.matatu.tracker.model.LocationEvent;
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
@RequiredArgsConstructor
public class KafkaConsumerConfig {

    private final MatatuTrackerProperties properties;


    private static final String TRUSTED_PACKAGES = "org.matatu.tracker.model";

    @Bean
    public ConsumerFactory<String, LocationEvent> locationConsumerFactory() {
        return consumerFactory(LocationEvent.class);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, LocationEvent> locationListenerContainerFactory() {
        return listenerFactory(locationConsumerFactory());
    }

    // ── FareEvent ─────────────────────────────────────────────────────────────

    @Bean
    public ConsumerFactory<String, FareEvent> fareConsumerFactory() {
        return consumerFactory(FareEvent.class);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, FareEvent> fareListenerContainerFactory() {
        return listenerFactory(fareConsumerFactory());
    }

    // ── EnrichedLocationEvent (Phase 2) ───────────────────────────────────────

    @Bean
    public ConsumerFactory<String, EnrichedLocationEvent> enrichedLocationConsumerFactory() {
        return consumerFactory(EnrichedLocationEvent.class);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, EnrichedLocationEvent> enrichedLocationListenerContainerFactory() {
        return listenerFactory(enrichedLocationConsumerFactory());
    }


    private <T> ConsumerFactory<String, T> consumerFactory(Class<T> targetType) {
        JacksonJsonDeserializer<T> deserializer = new JacksonJsonDeserializer<>(targetType, false);
        deserializer.addTrustedPackages(TRUSTED_PACKAGES);
        return new DefaultKafkaConsumerFactory<>(baseProps(), new StringDeserializer(), deserializer);
    }

    private <T> ConcurrentKafkaListenerContainerFactory<String, T> listenerFactory(
            ConsumerFactory<String, T> consumerFactory) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, T>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        return factory;
    }

    private Map<String, Object> baseProps() {
        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getKafka().getBootstrapServers(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false
        );
    }
}