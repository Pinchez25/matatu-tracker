package org.matatu.tracker.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

import java.util.Map;

/**
 * Configures a single, shared ProducerFactory and KafkaTemplate.
 * <p>
 * KEY CONCEPTS:
 * - ProducerFactory: creates Kafka Producer instances (they are thread-safe
 * and expensive to create, so one per app is the right approach).
 * - KafkaTemplate<K, V>: typed wrapper around ProducerFactory. Spring Boot
 * auto-configures a default one, but we define our own to control serialisation.
 * - JsonSerializer: serialises our Java records to JSON bytes on the wire.
 * - ACKS_CONFIG "all": wait for all in-sync replicas to confirm the write.
 * This is the safest setting. In Phase 4 you'll learn when to trade this off.
 * - RETRIES_CONFIG: automatically retry transient send failures. In Phase 4
 * we pair this with idempotence for exactly-once guarantees.
 */
@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        return new DefaultKafkaProducerFactory<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class,
                ProducerConfig.ACKS_CONFIG, "all",   // wait for all replicas
                ProducerConfig.RETRIES_CONFIG, 3        // retry on transient failure
        ));
    }

    /**
     * A single KafkaTemplate typed to Object — it can send any serialisable type.
     * Spring will use the JsonSerializer configured above to handle both
     * LocationEvent and FareEvent records.
     */
    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}