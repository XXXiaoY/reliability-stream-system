package com.vehicletelemetry.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class TelemetryEvent {

    @JsonProperty("event_id")
    private String eventId;

    @JsonProperty("vehicle_id")
    private String vehicleId;

    @JsonProperty("event_time")
    private long eventTime;

    @JsonProperty("ingest_time")
    private long ingestTime;

    @JsonProperty("schema_version")
    private String schemaVersion;

    @JsonProperty("event_type")
    private String eventType;

    @JsonProperty("payload")
    private Map<String, Object> payload;

    @Override
    public String toString() {
        return String.format("[%s] vehicle=%s type=%s version=%s time=%d payload=%s",
                eventId != null && eventId.length() >= 8 ? eventId.substring(0, 8) : eventId,
                vehicleId, eventType, schemaVersion, eventTime, payload);
    }
}
