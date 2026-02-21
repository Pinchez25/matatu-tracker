package org.matatu.tracker.streams;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.matatu.tracker.model.FareEvent;
import org.matatu.tracker.topics.Topics;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;

@Slf4j
@Configuration
public class FailedFareStream {

    @Bean
    public KStream<String, FareEvent> fareStatusStream(StreamsBuilder builder) {
        var fareSerde = new JacksonJsonSerde<>(FareEvent.class);

        KStream<String, FareEvent> fareStream = builder.stream(
                Topics.MATATU_FARES,
                Consumed.with(Serdes.String(), fareSerde)
        );

        BranchedKStream<String, FareEvent> branches = fareStream.split();

        branches.branch(
                (matatuId, fare) -> fare.status() == FareEvent.PaymentStatus.FAILED,
                Branched.withConsumer(failedStream -> {
                    failedStream
                            .peek((matatuId, fare) -> log.error(
                                    "[FARES] ❌ Failed payment → txn={}, matatu={}, amount=KES {}, method={}",
                                    fare.transactionId(), fare.matatuId(),
                                    fare.amountKes(), fare.paymentMethod()
                            ))
                            .to(Topics.MATATU_FARES_FAILED, Produced.with(Serdes.String(), fareSerde));
                })
        );

        branches.branch(
                (matatuId, fare) -> fare.status() == FareEvent.PaymentStatus.SUCCESS,
                Branched.withConsumer(successStream ->
                        successStream.peek((matatuId, fare) -> log.info(
                                "[FARES] ✅ Success → txn={}, matatu={}, amount=KES {}, method={}",
                                fare.transactionId(), fare.matatuId(),
                                fare.amountKes(), fare.paymentMethod()
                        ))
                )
        );

        branches.defaultBranch(
                Branched.withConsumer(pendingStream ->
                        pendingStream.peek((matatuId, fare) -> log.warn(
                                "[FARES] ⏳ Pending → txn={}, matatu={}",
                                fare.transactionId(), fare.matatuId()
                        ))
                )
        );
        return fareStream;

    }

}
