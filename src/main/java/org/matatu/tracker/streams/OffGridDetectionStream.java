package org.matatu.tracker.streams;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
import org.matatu.tracker.config.MatatuTrackerProperties;
import org.matatu.tracker.model.LocationEvent;
import org.matatu.tracker.model.MatatuLastSeen;
import org.matatu.tracker.model.OffGridAlert;
import org.matatu.tracker.topics.Topics;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class OffGridDetectionStream {

    static final String STORE_NAME = "matatu-last-seen-store";

    private final MatatuTrackerProperties properties;

    @Bean
    public KStream<String, LocationEvent> offGridDetectionStream(StreamsBuilder builder) {

        var locationSerde = new JacksonJsonSerde<>(LocationEvent.class);
        var lastSeenSerde = new JacksonJsonSerde<>(MatatuLastSeen.class);
        var offGridAlertSerde = new JacksonJsonSerde<>(OffGridAlert.class);

        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(STORE_NAME),
                        Serdes.String(),
                        lastSeenSerde));

        KStream<String, LocationEvent> locationStream =
                builder.stream(
                        Topics.MATATU_LOCATION, Consumed.with(Serdes.String(), locationSerde));

        //     <editor-fold desc="has been replaced with refactored code">
        //        locationStream
        //                .map((routeId, event) -> KeyValue.pair(event.matatuId(), event))
        //                .process(
        //                        offGridProcessorSupplier(
        //                                properties.getStreams().getOffgridSilenceMs(),
        //                                properties.getStreams().getOffgridCheckIntervalMs()),
        //                        STORE_NAME)
        //                .to(
        //                        Topics.MATATU_OFFGRID_ALERTS,
        //                        Produced.with(Serdes.String(), offGridAlertSerde));
        // </editor-fold>
        locationStream
//                .map((routeId, event) -> KeyValue.pair(event.matatuId(), event))
                .selectKey((routeId, event) -> event.matatuId())

                .process(
                        new OffGridProcessorSupplier(
                                properties.getStreams().getOffgridSilenceMs(),
                                properties.getStreams().getOffgridCheckIntervalMs()),
                        STORE_NAME)
                .to(
                        Topics.MATATU_OFFGRID_ALERTS,
                        Produced.with(Serdes.String(), offGridAlertSerde));

        return locationStream;
    }

    // <editor-fold desc="code refactored">
    //    private ProcessorSupplier<String, LocationEvent, String, OffGridAlert>
    // offGridProcessorSupplier(
    //            long silenceThresholdMs, long checkIntervalMs) {
    //
    //        return () ->
    //                new Processor<>() {
    //
    //                    private ProcessorContext<String, OffGridAlert> context;
    //                    private KeyValueStore<String, MatatuLastSeen> store;
    //
    //                    @Override
    //                    public void init(ProcessorContext<String, OffGridAlert> context) {
    //                        this.context = context;
    //                        this.store = context.getStateStore(STORE_NAME);
    //
    //                        context.schedule(
    //                                Duration.ofMillis(checkIntervalMs),
    //                                PunctuationType.WALL_CLOCK_TIME,
    //                                this::checkForOffGridMatatus);
    //                    }
    //
    //                    @Override
    //                    public void process(Record<String, LocationEvent> record) {
    //                        LocationEvent event = record.value();
    //                        store.put(
    //                                event.matatuId(),
    //                                new MatatuLastSeen(
    //                                        event.matatuId(),
    //                                        event.routeId(),
    //                                        event.routeName(),
    //                                        event.latitude(),
    //                                        event.longitude(),
    //                                        Instant.now()));
    //                    }
    //
    //                    private void checkForOffGridMatatus(long currentWallClockMs) {
    //                        Instant now = Instant.ofEpochMilli(currentWallClockMs);
    //
    //                        try (KeyValueIterator<String, MatatuLastSeen> iterator = store.all())
    // {
    //                            while (iterator.hasNext()) {
    //                                var entry = iterator.next();
    //                                var lastSeen = entry.value;
    //                                long silenceMs =
    //                                        currentWallClockMs -
    // lastSeen.lastSeenAt().toEpochMilli();
    //
    //                                if (silenceMs > silenceThresholdMs) {
    //                                    var alert =
    //                                            new OffGridAlert(
    //                                                    lastSeen.matatuId(),
    //                                                    lastSeen.routeId(),
    //                                                    lastSeen.routeName(),
    //                                                    lastSeen.lastLatitude(),
    //                                                    lastSeen.lastLongitude(),
    //                                                    silenceMs,
    //                                                    lastSeen.lastSeenAt(),
    //                                                    now);
    //
    //                                    log.warn(
    //                                            "[OFF-GRID] 📡 Matatu {} on {} has been silent for
    // {} minutes. Last seen: {}",
    //                                            lastSeen.matatuId(),
    //                                            lastSeen.routeName(),
    //                                            silenceMs / 60_000,
    //                                            lastSeen.lastSeenAt());
    //                                    context.forward(
    //                                            new Record<>(
    //                                                    lastSeen.matatuId(),
    //                                                    alert,
    //                                                    currentWallClockMs));
    //                                }
    //                            }
    //                        }
    //                    }
    //                };
    //    }
    // </editor-fold>
}
