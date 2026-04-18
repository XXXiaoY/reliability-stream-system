package com.vehicletelemetry;

import com.vehicletelemetry.model.TelemetryEvent;
import com.vehicletelemetry.serde.TelemetryEventDeserializer;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class TelemetryJob {

    // Side output for late events
    private static final OutputTag<TelemetryEvent> LATE_EVENTS =
            new OutputTag<TelemetryEvent>("late-events") {};

    // Side output for duplicate events
    private static final OutputTag<TelemetryEvent> DUPLICATE_EVENTS =
            new OutputTag<TelemetryEvent>("duplicate-events") {};

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30_000);

        // Kafka source
        KafkaSource<TelemetryEvent> source = KafkaSource.<TelemetryEvent>builder()
                .setBootstrapServers("kafka:29092")
                .setTopics("vehicle-telemetry")
                .setGroupId("telemetry-processor-v3")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new TelemetryEventDeserializer())
                .build();

        // Watermark: 30s out-of-orderness tolerance
        WatermarkStrategy<TelemetryEvent> watermarkStrategy = WatermarkStrategy
                .<TelemetryEvent>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                .withTimestampAssigner(
                        (SerializableTimestampAssigner<TelemetryEvent>)
                                (event, ts) -> event.getEventTime()
                )
                .withIdleness(Duration.ofSeconds(10));

        DataStream<TelemetryEvent> telemetryStream = env
                .fromSource(source, watermarkStrategy, "Kafka Source");

        // Core processing: dedup + late detection
        SingleOutputStreamOperator<TelemetryEvent> processed = telemetryStream
                .keyBy(TelemetryEvent::getVehicleId)
                .process(new DeduplicateAndDetectLate());

        // Main output: valid, deduplicated events
        processed.map(e -> String.format("[OK] %s vehicle=%s time=%d",
                        e.getEventId().substring(0, 8), e.getVehicleId(), e.getEventTime()))
                .print();

        // Late events side output
        processed.getSideOutput(LATE_EVENTS)
                .map(e -> String.format("[LATE] %s vehicle=%s time=%d",
                        e.getEventId().substring(0, 8), e.getVehicleId(), e.getEventTime()))
                .print();

        // Duplicate events side output
        processed.getSideOutput(DUPLICATE_EVENTS)
                .map(e -> String.format("[DUP] %s vehicle=%s time=%d",
                        e.getEventId().substring(0, 8), e.getVehicleId(), e.getEventTime()))
                .print();

        env.execute("Vehicle Telemetry Processor");
    }

    /**
     * Keyed process function that:
     * 1. Detects late events (event_time older than watermark by > threshold)
     * 2. Deduplicates by event_id using MapState with TTL via timer cleanup
     */
    public static class DeduplicateAndDetectLate
            extends KeyedProcessFunction<String, TelemetryEvent, TelemetryEvent> {

        // Track seen event IDs for dedup
        private transient MapState<String, Long> seenEventIds;

        // Track counts per vehicle for logging
        private transient ValueState<long[]> counters;

        // How long to keep event IDs for dedup (5 minutes)
        private static final long DEDUP_TTL_MS = 5 * 60 * 1000;

        // Late threshold: if event_time is more than 45s behind watermark
        private static final long LATE_THRESHOLD_MS = 45 * 1000;

        @Override
        public void open(Configuration parameters) {
            seenEventIds = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("seen-ids", Types.STRING, Types.LONG));

            counters = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("counters", long[].class));
        }

        @Override
        public void processElement(TelemetryEvent event,
                                   KeyedProcessFunction<String, TelemetryEvent, TelemetryEvent>.Context ctx,
                                   Collector<TelemetryEvent> out) throws Exception {

            long[] c = counters.value();
            if (c == null) {
                c = new long[]{0, 0, 0}; // [processed, duplicates, late]
            }

            long watermark = ctx.timerService().currentWatermark();

            // --- Check duplicate ---
            if (seenEventIds.contains(event.getEventId())) {
                c[1]++;
                counters.update(c);
                ctx.output(DUPLICATE_EVENTS, event);
                return;
            }

            // Record this event ID and register cleanup timer
            long cleanupTime = ctx.timerService().currentProcessingTime() + DEDUP_TTL_MS;
            seenEventIds.put(event.getEventId(), cleanupTime);
            ctx.timerService().registerProcessingTimeTimer(cleanupTime);

            // --- Check late ---
            if (watermark > 0 && event.getEventTime() < watermark - LATE_THRESHOLD_MS) {
                c[2]++;
                counters.update(c);
                ctx.output(LATE_EVENTS, event);
                return;
            }

            // --- Normal event ---
            c[0]++;
            counters.update(c);
            out.collect(event);

            // Log stats every 100 events
            if ((c[0] + c[1] + c[2]) % 100 == 0) {
                System.out.printf("[STATS] vehicle=%s processed=%d duplicates=%d late=%d%n",
                        ctx.getCurrentKey(), c[0], c[1], c[2]);
            }
        }

        @Override
        public void onTimer(long timestamp,
                            KeyedProcessFunction<String, TelemetryEvent, TelemetryEvent>.OnTimerContext ctx,
                            Collector<TelemetryEvent> out) throws Exception {
            // Cleanup expired event IDs to prevent unbounded state growth
            long now = ctx.timerService().currentProcessingTime();
            var iterator = seenEventIds.iterator();
            while (iterator.hasNext()) {
                var entry = iterator.next();
                if (entry.getValue() <= now) {
                    iterator.remove();
                }
            }
        }
    }
}
