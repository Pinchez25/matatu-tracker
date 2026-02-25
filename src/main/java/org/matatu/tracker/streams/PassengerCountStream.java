package org.matatu.tracker.streams;

import java.time.Instant;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.matatu.tracker.model.*;
import org.matatu.tracker.topics.Topics;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
public class PassengerCountStream {

    static final String MATATU_SNAPSHOT_STORE = "matatu-snapshot-store";
    static final String ROUTE_OCCUPANCY_STORE = "route-occupancy-store";

    @Bean
    public KTable<String, RouteOccupancy> routeOccupancyTable(StreamsBuilder builder) {

        var locationSerde = new JacksonJsonSerde<>(LocationEvent.class);
        var deltaEventSerde = new JacksonJsonSerde<>(DeltaEvent.class);
        var snapshotSerde = new JacksonJsonSerde<>(MatatuSnapshot.class);
        var occupancySerde = new JacksonJsonSerde<>(RouteOccupancy.class);

        //  registers the store with the topology, making it available for processors to request by
        // name.
        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(MATATU_SNAPSHOT_STORE),
                        Serdes.String(),
                        snapshotSerde));

        // ── Step 1: consume raw location events (key = routeId) ──────────
        KStream<String, LocationEvent> locationStream =
                builder.stream(
                        Topics.MATATU_LOCATION, Consumed.with(Serdes.String(), locationSerde));

        // ── Step 2: compute per-matatu delta ─────────────────────────────
        KStream<String, DeltaEvent> deltaStream =
                locationStream
                        // Re-key by matatuId so each matatu's events hit the same
                        // partition and the processor sees a consistent history
                        .selectKey((routeId, event) -> event.matatuId())
                        // processValues - keeps the keys fixed
                        .processValues(
                                () -> new MatatuDeltaProcessor(MATATU_SNAPSHOT_STORE),
                                Named.as("matatu-delta-processor"),
                                MATATU_SNAPSHOT_STORE)
                        .filter((matatuId, delta) -> delta != null)
                        // Re-key back to routeId for route-level aggregation
                        .selectKey((matatuId, delta) -> delta.routeId());

        // ── Step 3: aggregate deltas into route-level occupancy ───────────
        KTable<String, RouteOccupancy> occupancyTable =
                deltaStream
                        .groupByKey(Grouped.with(Serdes.String(), deltaEventSerde))
                        .aggregate(
                                () -> new RouteOccupancy("", "", 0, 0L, 0L, Instant.now()),
                                (routeId, delta, current) ->
                                        new RouteOccupancy(
                                                routeId,
                                                delta.routeName(),
                                                Math.max(
                                                        0,
                                                        current.currentPassengers()
                                                                + delta.passengerDelta()),
                                                current.totalBoardings() + delta.boardings(),
                                                current.totalAlightings() + delta.alightings(),
                                                Instant.now()),
                                Materialized.<String, RouteOccupancy>as(
                                                Stores.persistentKeyValueStore(
                                                        ROUTE_OCCUPANCY_STORE))
                                        .withKeySerde(Serdes.String())
                                        .withValueSerde(occupancySerde));

        // ── Step 4: log and publish ───────────────────────────────────────
        occupancyTable
                .toStream()
                .peek(
                        (routeId, occ) ->
                                log.info(
                                        "[OCCUPANCY] 🚌 Route {} | On board: {} | Boarded: {} | Alighted: {}",
                                        routeId,
                                        occ.currentPassengers(),
                                        occ.totalBoardings(),
                                        occ.totalAlightings()))
                .to(Topics.MATATU_ROUTE_OCCUPANCY, Produced.with(Serdes.String(), occupancySerde));

        return occupancyTable;
    }
}
