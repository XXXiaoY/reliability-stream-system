package com.vehicletelemetry.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
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
    private LocationPayload payload;

    @Override
    public String toString() {
        return String.format("[%s] vehicle=%s type=%s time=%d lat=%.4f lng=%.4f",
                eventId.substring(0, 8), vehicleId, eventType, eventTime,
                payload != null ? payload.getLatitude() : 0,
                payload != null ? payload.getLongitude() : 0);
    }
}
