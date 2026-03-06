package org.matatu.tracker.streams;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.matatu.tracker.model.LocationEvent;
import org.matatu.tracker.model.OffGridAlert;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class OffGridProcessorSupplier
        implements ProcessorSupplier<String, LocationEvent, String, OffGridAlert> {

    private final long silenceThresholdMs;
    private final long checkIntervalMs;

    @Override
    public Processor<String, LocationEvent, String, OffGridAlert> get() {
        return new OffGridProcessor(silenceThresholdMs, checkIntervalMs);
    }
}
