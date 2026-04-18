package com.vehicletelemetry;

import com.vehicletelemetry.model.TelemetryEvent;
import com.vehicletelemetry.serde.TelemetryEventDeserializer;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class TelemetryJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Enable checkpointing every 30 seconds
        env.enableCheckpointing(30_000);

        // Kafka source with typed deserialization
        KafkaSource<TelemetryEvent> source = KafkaSource.<TelemetryEvent>builder()
                .setBootstrapServers("kafka:29092")
                .setTopics("vehicle-telemetry")
                .setGroupId("telemetry-processor")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new TelemetryEventDeserializer())
                .build();

        // Watermark strategy: tolerate up to 30 seconds of out-of-orderness
        WatermarkStrategy<TelemetryEvent> watermarkStrategy = WatermarkStrategy
                .<TelemetryEvent>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                .withTimestampAssigner(
                        (SerializableTimestampAssigner<TelemetryEvent>) (event, timestamp) -> event.getEventTime()
                );

        // Build the pipeline
        DataStream<TelemetryEvent> telemetryStream = env
                .fromSource(source, watermarkStrategy, "Kafka Source");

        // Key by vehicle_id and print (next phase: add real processing)
        telemetryStream
                .keyBy(TelemetryEvent::getVehicleId)
                .print();

        env.execute("Vehicle Telemetry Processor");
    }
}
