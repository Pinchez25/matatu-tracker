package org.matatu.tracker.streams;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.matatu.tracker.config.MatatuTrackerProperties;
import org.matatu.tracker.model.FareEvent;
import org.matatu.tracker.model.SaccoInfo;
import org.matatu.tracker.model.SaccoRevenueWindow;
import org.matatu.tracker.topics.Topics;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
public class SaccoRevenueStream {

    private final Map<String, SaccoInfo> saccoLookup;
    private final List<String> sortedSaccoIds;
    private final MatatuTrackerProperties properties;

    /**
     * Explicit constructor required — final fields sortedSaccoIds is derived from saccoLookup and
     * cannot be handled by @RequiredArgsConstructor.
     *
     * <p>saccoLookup is expected to be keyed by saccoId (e.g. "sacco_01") — this eliminates the
     * need for a separate inverted index map.
     *
     * <p>Note: idempotency (duplicate event protection) is handled at the infrastructure level via
     * processing.guarantee=exactly_once_v2 in Kafka Streams config, not in application code.
     */
    public SaccoRevenueStream(
            Map<String, SaccoInfo> saccoLookup, MatatuTrackerProperties properties) {
        this.saccoLookup = saccoLookup;
        this.properties = properties;
        this.sortedSaccoIds = saccoLookup.keySet().stream().sorted().toList();
    }

    @Bean
    public KStream<String, FareEvent> saccoRevenueStream(StreamsBuilder builder) {

        var fareSerde = new JacksonJsonSerde<>(FareEvent.class);
        var revenueSerde = new JacksonJsonSerde<>(SaccoRevenueWindow.class);

        KStream<String, FareEvent> fareStream =
                builder.stream(Topics.MATATU_FARES, Consumed.with(Serdes.String(), fareSerde));

        //        {
        //           transactionId, matatuId, passengerId, amountKes, paymentMethod, status,
        // timestamp
        //         }

        fareStream
                // Step 1: only count successful payments toward revenue
                .filter((matatuId, fare) -> fare.status() == FareEvent.PaymentStatus.SUCCESS)

                // Step 2: re-key from matatuId → saccoId
                // Triggers an internal repartition topic automatically.
                .selectKey((matatuId, fare) -> resolveSaccoId(fare))

                // Step 3: group by the new key (saccoId)
                .groupByKey(Grouped.with(Serdes.String(), fareSerde))

                // Step 4: apply a tumbling time window
                .windowedBy(
                        TimeWindows.ofSizeAndGrace(
                                Duration.ofMinutes(
                                        properties.getStreams().getRevenueWindowMinutes()),
                                Duration.ofMinutes(
                                        properties.getStreams().getRevenueGraceMinutes())))

                // Step 5: aggregate — sum revenue and count transactions per window
                .aggregate(
                        // Initialiser — sentinel values for a new (saccoId, window) pair.
                        // windowStart/windowEnd use EPOCH as they are overwritten with
                        // authoritative values when unwrapping Windowed<K> in Step 7.
                        // {saccoId, saccoName, totalRevenueKes, transactionCount, windowStart,
                        // windowEnd, computedAt}
                        () ->
                                new SaccoRevenueWindow(
                                        "",
                                        "",
                                        BigDecimal.ZERO,
                                        0L,
                                        Instant.EPOCH,
                                        Instant.EPOCH,
                                        Instant.EPOCH),

                        // Aggregator — accumulate revenue for each successful fare.
                        // saccoLookup is keyed by saccoId so lookup is O(1).
                        (saccoId, fare, current) -> {
                            SaccoInfo sacco =
                                    saccoLookup.getOrDefault(
                                            saccoId, new SaccoInfo(saccoId, "Unknown SACCO", ""));
                            return new SaccoRevenueWindow(
                                    saccoId,
                                    sacco.saccoName(),
                                    current.totalRevenueKes().add(fare.amountKes()),
                                    current.transactionCount() + 1,
                                    current.windowStart(),
                                    current.windowEnd(),
                                    Instant.now());
                        },
                        Materialized.with(Serdes.String(), revenueSerde))

                // Step 6: suppress intermediate updates — only emit once when window closes.
                // unbounded() is safe here: cardinality is bounded to 5 SACCOs maximum,
                // so heap pressure is negligible regardless of throughput.

                /*
                *  Unbounded — use as much heap as needed (safe for low cardinality like our 5 SACCOs)
                * Suppressed.BufferConfig.unbounded()

                     Bounded by size — spill to disk or throw exception if exceeded
                     Suppressed.BufferConfig.maxBytes(10 * 1024 * 1024L)  // 10MB cap

                     .withNoBoundEnforcement()   // silently emit early if exceeded

                    * // or
                     .shutDownWhenFull()
                *
                *
                * */
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))

                // Step 7: convert windowed KTable back to a stream, unwrapping the window
                // key to set authoritative windowStart and windowEnd from the Windowed<K> bounds.
                .toStream()
                .map(
                        (windowedKey, revenue) -> {
                            var window = windowedKey.window();
                            var finalRevenue =
                                    new SaccoRevenueWindow(
                                            revenue.saccoId(),
                                            revenue.saccoName(),
                                            revenue.totalRevenueKes(),
                                            revenue.transactionCount(),
                                            Instant.ofEpochMilli(window.start()),
                                            Instant.ofEpochMilli(window.end()),
                                            Instant.now());
                            return KeyValue.pair(windowedKey.key(), finalRevenue);
                        })
                .peek(
                        (saccoId, revenue) ->
                                log.info(
                                        "[REVENUE] 💰 SACCO: {} | Window: {} → {} | Total: KES {} | Transactions: {}",
                                        revenue.saccoName(),
                                        revenue.windowStart(),
                                        revenue.windowEnd(),
                                        revenue.totalRevenueKes(),
                                        revenue.transactionCount()))
                .to(Topics.MATATU_SACCO_REVENUE, Produced.with(Serdes.String(), revenueSerde));

        return fareStream;
    }

    /**
     * Deterministically assigns a saccoId to a fare event by hashing matatuId. The same matatuId
     * will always resolve to the same SACCO within a JVM session.
     *
     * <p>Bitmasking with Integer.MAX_VALUE is used instead of Math.abs() to avoid the
     * Integer.MIN_VALUE edge case where Math.abs() returns a negative value.
     */
    private String resolveSaccoId(FareEvent fare) {
        if (sortedSaccoIds.isEmpty()) {
            throw new IllegalStateException(
                    "SACCO lookup is not configured — cannot resolve SACCO for fare: "
                            + fare.transactionId());
        }
        int index = (fare.matatuId().hashCode() & Integer.MAX_VALUE) % sortedSaccoIds.size();
        return sortedSaccoIds.get(index);
    }
}
