package org.matatu.tracker.streams;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.matatu.tracker.model.EnrichedLocationEvent;
import org.matatu.tracker.model.LocationEvent;
import org.matatu.tracker.model.SaccoInfo;
import org.matatu.tracker.topics.Topics;
import org.matatu.tracker.config.MatatuTrackerProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;

import java.time.Instant;
import java.util.Map;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class RouteEnrichmentStream {

    private final Map<String, SaccoInfo> saccoLookup;
    private final MatatuTrackerProperties properties;


    @Bean
    public KStream<String, LocationEvent> enrichmentStream(StreamsBuilder builder) {
        var locationSerde = new JacksonJsonSerde<>(LocationEvent.class);
        var enrichedSerde = new JacksonJsonSerde<>(EnrichedLocationEvent.class);

        KStream<String, LocationEvent> locationStream = builder
                .stream(Topics.MATATU_LOCATION, Consumed.with(Serdes.String(), locationSerde));

        locationStream
                .mapValues(this::enrich)
                .peek((routeId, enriched) -> log.debug(
                        "[ENRICHMENT] ✅ {} on {} ({}) enriched with SACCO: {}",
                        enriched.matatuId(), enriched.routeName(), routeId, enriched.saccoName()
                )).to(Topics.MATATU_LOCATION_ENRICHED, Produced.with(Serdes.String(), enrichedSerde));

        return locationStream;
    }

    EnrichedLocationEvent enrich(LocationEvent event) {
        SaccoInfo sacco = saccoLookup.getOrDefault(
                event.routeId(),
                new SaccoInfo("unknown", "Unknown Sacco", "Unknown Terminus")
        );

        if (sacco.saccoId().equals("unknown")) {
            log.warn("[ENRICHMENT] ⚠️ No SACCO found for routeId='{}' on matatu '{}'",
                    event.routeId(), event.matatuId());

        }

        return new EnrichedLocationEvent(
                event.matatuId(),
                event.routeId(),
                event.routeName(),
                sacco.saccoName(),
                sacco.saccoId(),
                sacco.terminus(),
                event.latitude(),
                event.longitude(),
                event.speedKmh(),
                event.passengersOnboard(),
                event.speedKmh() > properties.getStreams().getSpeedThresholdKmh(),
                event.timestamp(),
                Instant.now()
        );

    }
}
