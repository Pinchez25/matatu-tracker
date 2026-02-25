package org.matatu.tracker.streams;

import java.time.Instant;

import org.apache.kafka.streams.processor.api.ContextualFixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;
import org.matatu.tracker.model.DeltaEvent;
import org.matatu.tracker.model.LocationEvent;
import org.matatu.tracker.model.MatatuSnapshot;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class MatatuDeltaProcessor
        extends ContextualFixedKeyProcessor<String, LocationEvent, DeltaEvent> {

    private final String storeName;
    private KeyValueStore<String, MatatuSnapshot> snapshotStore;

    @Override
    public void init(FixedKeyProcessorContext<String, DeltaEvent> context) {
        super.init(context);
        this.snapshotStore = context.getStateStore(storeName);
    }

    @Override
    public void process(FixedKeyRecord<String, LocationEvent> record) {
        String matatuId = record.key();
        LocationEvent event = record.value();

        MatatuSnapshot previous = snapshotStore.get(matatuId);
        int previousCount = (previous == null) ? 0 : previous.lastPassengerCount();
        int delta = event.passengersOnboard() - previousCount;
        int boardings = Math.max(0, delta);
        int alightings = Math.max(0, -delta);

        log.debug(
                "[DELTA] 🚌 {} | prev={} cur={} delta={}",
                matatuId,
                previousCount,
                event.passengersOnboard(),
                delta);

        snapshotStore.put(
                matatuId,
                new MatatuSnapshot(
                        matatuId, event.routeId(), event.passengersOnboard(), Instant.now()));

        context()
                .forward(
                        record.withValue(
                                new DeltaEvent(
                                        matatuId,
                                        event.routeId(),
                                        event.routeName(),
                                        delta,
                                        boardings,
                                        alightings)));
    }
}
