package org.matatu.tracker.consumer;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.matatu.tracker.model.LocationEvent;
import org.matatu.tracker.topics.Topics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumer Group 1 — simulates a real-time display board (like those at bus stages).
 * <p>
 * KEY CONCEPTS:
 * - @KafkaListener: Spring's annotation-driven consumer. Under the hood it creates
 * a ConcurrentMessageListenerContainer that manages threads for you.
 * - groupId "display-board-group": Every consumer group gets its OWN copy of every
 * message. This group and LocationLoggerConsumer both read the same topic
 * independently, each maintaining their own offsets.
 * - ConsumerRecord<K,V>: gives you access to the raw Kafka metadata — partition,
 * offset, key, timestamp — not just the payload. This is very educational.
 * - concurrency = "3": spins up 3 listener threads, one per partition, allowing
 * parallel consumption. Must not exceed the number of partitions.
 */
@Component
public class DisplayBoardConsumer {

    private static final Logger log = LoggerFactory.getLogger(DisplayBoardConsumer.class);

    @KafkaListener(
            topics = Topics.MATATU_LOCATION,
            groupId = "display-board-group",
            concurrency = "3",
            containerFactory = "locationListenerContainerFactory"
    )
    public void onLocationEvent(ConsumerRecord<String, LocationEvent> record) {
        LocationEvent event = record.value();

        // Print the raw Kafka metadata alongside the payload — crucial for learning
        log.info("""
                        [DISPLAY BOARD] 🚌 Live Update
                          Matatu  : {}
                          Route   : {} ({})
                          Position: {}, {}
                          Speed   : {} km/h
                          Pax     : {} onboard
                          ── Kafka Metadata ──
                          Partition : {}   (routeId '{}' always hashes to this partition)
                          Offset    : {}   (position in the partition log)
                          Timestamp : {}
                        """,
                event.matatuId(),
                event.routeId(), event.routeName(),
                event.latitude(), event.longitude(),
                event.speedKmh(),
                event.passengersOnboard(),
                record.partition(), record.key(),
                record.offset(),
                record.timestamp()
        );
    }
}

