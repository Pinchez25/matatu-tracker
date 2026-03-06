package org.matatu.tracker.streams;

import java.time.Duration;
import java.time.Instant;

import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.matatu.tracker.model.LocationEvent;
import org.matatu.tracker.model.MatatuLastSeen;
import org.matatu.tracker.model.OffGridAlert;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class OffGridProcessor implements Processor<String, LocationEvent, String, OffGridAlert> {

    private final long silenceThresholdMs;
    private final long checkIntervalMs;

    private ProcessorContext<String, OffGridAlert> context;
    private KeyValueStore<String, MatatuLastSeen> store;

    @Override
    public void init(ProcessorContext<String, OffGridAlert> context) {
        this.context = context;
        this.store = context.getStateStore(OffGridDetectionStream.STORE_NAME);

        context.schedule(
                Duration.ofMillis(checkIntervalMs),
                PunctuationType.WALL_CLOCK_TIME,
                this::checkForOffGridMatatus
        );
    }

    @Override
    public void process(Record<String, LocationEvent> record) {

        LocationEvent event = record.value();

        store.put(
                event.matatuId(),
                new MatatuLastSeen(
                        event.matatuId(),
                        event.routeId(),
                        event.routeName(),
                        event.latitude(),
                        event.longitude(),
                        Instant.now()));
    }

    private void checkForOffGridMatatus(long nowMs) {

        Instant now = Instant.ofEpochMilli(nowMs);

        try (KeyValueIterator<String, MatatuLastSeen> iterator = store.all()) {

            while (iterator.hasNext()) {

                var entry = iterator.next();
                var lastSeen = entry.value;

                long silenceMs = nowMs - lastSeen.lastSeenAt().toEpochMilli();

                if (silenceMs > silenceThresholdMs) {

                    OffGridAlert alert =
                            new OffGridAlert(
                                    lastSeen.matatuId(),
                                    lastSeen.routeId(),
                                    lastSeen.routeName(),
                                    lastSeen.lastLatitude(),
                                    lastSeen.lastLongitude(),
                                    silenceMs,
                                    lastSeen.lastSeenAt(),
                                    now);

                    context.forward(new Record<>(lastSeen.matatuId(), alert, nowMs));
                }
            }
        }
    }
}
