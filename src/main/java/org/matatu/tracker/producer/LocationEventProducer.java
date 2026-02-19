package org.matatu.tracker.producer;

import lombok.RequiredArgsConstructor;
import org.matatu.tracker.model.LocationEvent;
import org.matatu.tracker.topics.Topics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * Publishes {@link LocationEvent} messages to the {@code matatu.location} topic.
 * <p>
 * KEY CONCEPTS:
 * - KafkaTemplate: Spring's high-level abstraction over the raw Kafka Producer.
 * - Message key (routeId): Kafka hashes the key to decide which partition the
 * message goes to. Using routeId ensures all events for Route 33 always go
 * to the same partition, preserving ordering per route.
 * - CompletableFuture: send() is async. We attach callbacks to log success/failure
 * without blocking the calling thread. In production you'd send these to a
 * Dead Letter Queue on failure (Phase 4 concept, previewed here).
 */
@Service
@RequiredArgsConstructor
public class LocationEventProducer {

    private static final Logger log = LoggerFactory.getLogger(LocationEventProducer.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * Sends a location event to Kafka.
     *
     * @param event the GPS location event
     * @return a CompletableFuture that completes when Kafka acknowledges the send
     */
    public CompletableFuture<SendResult<String, Object>> send(LocationEvent event) {
        CompletableFuture<SendResult<String, Object>> future =
                kafkaTemplate.send(Topics.MATATU_LOCATION, event.routeId(), event);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish location event for matatu [{}] on route [{}]: {}",
                        event.matatuId(), event.routeId(), ex.getMessage());
            } else {
                log.debug("Published location event → topic={}, partition={}, offset={}, matatu={}, route={}",
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),   // 👈 notice which partition was chosen
                        result.getRecordMetadata().offset(),      // 👈 ever-increasing offset per partition
                        event.matatuId(),
                        event.routeId());
            }
        });

        return future;
    }
}