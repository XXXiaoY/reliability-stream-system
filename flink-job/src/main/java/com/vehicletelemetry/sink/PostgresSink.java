package com.vehicletelemetry.sink;

import com.vehicletelemetry.model.TelemetryEvent;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class PostgresSink {

    private static final String UPSERT_SQL =
            "INSERT INTO telemetry_events " +
            "(event_id, vehicle_id, event_time, ingest_time, event_type, " +
            " latitude, longitude, altitude, heading) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) " +
            "ON CONFLICT (event_id) DO NOTHING";

    public static SinkFunction<TelemetryEvent> create() {
        return JdbcSink.sink(
                UPSERT_SQL,
                (ps, event) -> {
                    ps.setString(1, event.getEventId());
                    ps.setString(2, event.getVehicleId());
                    ps.setLong(3, event.getEventTime());
                    ps.setLong(4, event.getIngestTime());
                    ps.setString(5, event.getEventType());
                    ps.setDouble(6, event.getPayload().getLatitude());
                    ps.setDouble(7, event.getPayload().getLongitude());
                    ps.setDouble(8, event.getPayload().getAltitude());
                    ps.setDouble(9, event.getPayload().getHeading());
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(50)
                        .withBatchIntervalMs(3000)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:postgresql://postgres:5432/telemetry")
                        .withDriverName("org.postgresql.Driver")
                        .withUsername("telemetry")
                        .withPassword("telemetry123")
                        .build()
        );
    }
}
