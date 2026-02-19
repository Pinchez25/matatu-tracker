package org.matatu.tracker.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.matatu.tracker.model.FareEvent;
import org.matatu.tracker.topics.Topics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes fare payment events from {@code matatu.fares}.
 * <p>
 * Demonstrates that a single application can host multiple @KafkaListener
 * methods across different topics and groups simultaneously.
 */
@Component
public class FareConsumer {

    private static final Logger log = LoggerFactory.getLogger(FareConsumer.class);

    @KafkaListener(
            topics = Topics.MATATU_FARES,
            groupId = "fare-processor-group",
            concurrency = "3",
            containerFactory = "fareListenerContainerFactory"
    )
    public void onFareEvent(ConsumerRecord<String, FareEvent> record) {
        FareEvent event = record.value();

        // Use a switch expression (Java 14+) to handle each payment status cleanly
        String icon = switch (event.status()) {
            case SUCCESS -> "✅";
            case FAILED -> "❌";
            case PENDING -> "⏳";
        };

        log.info("[FARES] {} Payment → txn={}, matatu={}, amount=KES {}, method={}, status={}",
                icon,
                event.transactionId(),
                event.matatuId(),
                event.amountKes(),
                event.paymentMethod(),
                event.status()
        );
    }
}