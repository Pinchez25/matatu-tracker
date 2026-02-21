package org.matatu.tracker.producer;

import java.util.concurrent.CompletableFuture;

import org.matatu.tracker.model.FareEvent;
import org.matatu.tracker.topics.Topics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;

/**
 * Publishes {@link FareEvent} messages to the {@code matatu.fares} topic.
 *
 * <p>Note that both producers share the same pattern — KafkaTemplate, async send, callback logging.
 * This is intentional: learning to recognise this pattern means you can extend it to any new event
 * type.
 */
@Service
@RequiredArgsConstructor
public class FareEventProducer {

    private static final Logger log = LoggerFactory.getLogger(FareEventProducer.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public CompletableFuture<SendResult<String, Object>> send(FareEvent event) {
        // matatuId is the key so all fares for a given matatu are ordered
        CompletableFuture<SendResult<String, Object>> future =
                kafkaTemplate.send(Topics.MATATU_FARES, event.matatuId(), event);

        future.whenComplete(
                (result, ex) -> {
                    if (ex != null) {
                        log.error(
                                "Failed to publish fare event [txn={}] for matatu [{}]: {}",
                                event.transactionId(),
                                event.matatuId(),
                                ex.getMessage());
                    } else {
                        log.debug(
                                "Published fare event → topic={}, partition={}, offset={}, txn={}, status={}",
                                result.getRecordMetadata().topic(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset(),
                                event.transactionId(),
                                event.status());
                    }
                });

        return future;
    }
}
