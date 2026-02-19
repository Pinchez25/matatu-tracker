package org.matatu.tracker.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.matatu.tracker.model.LocationEvent;
import org.matatu.tracker.topics.Topics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumer Group 2 — simulates a persistence/logging service.
 * <p>
 * KEY CONCEPT — Independent Consumer Groups:
 * Both this class and {@link DisplayBoardConsumer} subscribe to the same topic,
 * but they belong to DIFFERENT groups ("location-logger-group" vs "display-board-group").
 * <p>
 * This means:
 * - Kafka tracks a separate offset for each group
 * - If this consumer falls behind, the display board is not affected at all
 * - If you restart only this consumer, it resumes from where it left off,
 * completely independently of the display board consumer
 * <p>
 * In production, this consumer would write to PostgreSQL/BigQuery.
 * For Phase 1, we just log — focus is on understanding the consumer group mechanic.
 */
@Component
public class LocationLoggerConsumer {

    private static final Logger log = LoggerFactory.getLogger(LocationLoggerConsumer.class);

    @KafkaListener(
            topics = Topics.MATATU_LOCATION,
            groupId = "location-logger-group",
            concurrency = "3",
            containerFactory = "locationListenerContainerFactory"
    )
    public void onLocationEvent(ConsumerRecord<String, LocationEvent> record) {
        LocationEvent event = record.value();

        // Simulates writing to a DB — in Phase 4 we'll use Kafka Connect instead
        log.info("[LOGGER] 📝 Persisting → matatu={}, route={}, lat={}, lng={}, speed={}, offset={}",
                event.matatuId(),
                event.routeId(),
                event.latitude(),
                event.longitude(),
                event.speedKmh(),
                record.offset()
        );
    }
}
