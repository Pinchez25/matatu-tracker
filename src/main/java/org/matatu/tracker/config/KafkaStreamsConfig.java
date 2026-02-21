package org.matatu.tracker.config;

import java.util.Map;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;

import lombok.RequiredArgsConstructor;

@Configuration
@EnableKafkaStreams
@RequiredArgsConstructor
public class KafkaStreamsConfig {

    private final MatatuTrackerProperties properties;

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kafkaStreamsConfiguration() {
        return new KafkaStreamsConfiguration(
                Map.of(
                        StreamsConfig.APPLICATION_ID_CONFIG,
                        properties.getStreams().getApplicationId(),
                        StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                        properties.getKafka().getBootstrapServers(),
                        StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                        Serdes.StringSerde.class,
                        StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                        JacksonJsonSerde.class,
                        StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,
                        1000L,
                        StreamsConfig.NUM_STREAM_THREADS_CONFIG,
                        3));
    }
}
