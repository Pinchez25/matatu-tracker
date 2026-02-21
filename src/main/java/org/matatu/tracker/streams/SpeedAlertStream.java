package org.matatu.tracker.streams;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.matatu.tracker.model.LocationEvent;
import org.matatu.tracker.topics.Topics;
import org.matatu.tracker.config.MatatuTrackerProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;

/**
 * Topology 1 — Speed Alert Stream
 * <p>
 * Reads from : matatu.location
 * Filters    : events where speedKmh > threshold (default 80)
 * Writes to  : matatu.speed.alerts
 * <p>
 * ┌──────────────────┐     filter(speed > 80)    ┌──────────────────────┐
 * │  matatu.location │ ─────────────────────────► │ matatu.speed.alerts  │
 * └──────────────────┘                            └──────────────────────┘
 * <p>
 * KEY CONCEPTS:
 * <p>
 * StreamsBuilder — the entry point for building a stream topology. You describe
 * WHAT should happen (filter, map, etc.) and Kafka Streams figures out HOW to
 * execute it. This is declarative, not imperative.
 * <p>
 * KStream<K, V> — an unbounded stream of key-value records. Think of it as an
 * infinite sequence of events. Each record is processed independently.
 * <p>
 * Consumed.with(keySerde, valueSerde) — tells Kafka Streams how to DESERIALISE
 * records coming off the topic. We use JsonSerde<LocationEvent> for values.
 * <p>
 * .filter(predicate) — a STATELESS operation. Kafka Streams evaluates each record
 * independently with no memory of previous records. Very cheap — no state store needed.
 * <p>
 * .peek(action) — like .forEach but non-terminal; passes the record through unchanged.
 * Perfect for logging without breaking the pipeline.
 * <p>
 * .to(topic, Produced.with(...)) — terminal operation; writes matching records to
 * the output topic with the specified serialisers.
 * <p>
 * WHY @Bean on the KStream method?
 * Spring's @EnableKafkaStreams injects a StreamsBuilder and scans for @Bean methods
 * that accept it. It builds all topologies into a single KafkaStreams instance
 * and manages its lifecycle (start/stop) for you.
 */

@Slf4j
@Configuration
@RequiredArgsConstructor
public class SpeedAlertStream {

    private final MatatuTrackerProperties properties;

    @Bean(name = "speedAlertKStream")
    public KStream<String, LocationEvent> speedAlertStream(StreamsBuilder builder) {
        var locationSerde = new JacksonJsonSerde<>(LocationEvent.class);

        KStream<String, LocationEvent> locationStream = builder
                .stream(
                        Topics.MATATU_LOCATION,
                        Consumed.with(Serdes.String(), locationSerde)
                )
                .filter((routeId, event) -> event.speedKmh() > properties.getStreams().getSpeedThresholdKmh())
                .peek((routeId, event) -> log.warn(
                        "[SPEED ALERT]  Matatu {} on {} doing {} km/h (threshold: {} km/h)",
                        event.matatuId(), event.routeName(), event.speedKmh(), properties.getStreams().getSpeedThresholdKmh()
                ));

        locationStream.to(
                Topics.MATATU_SPEED_ALERTS,
                Produced.with(Serdes.String(), locationSerde)
        );
        return locationStream;


    }
}
