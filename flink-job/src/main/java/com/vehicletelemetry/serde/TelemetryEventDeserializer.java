package com.vehicletelemetry.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vehicletelemetry.model.TelemetryEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class TelemetryEventDeserializer implements DeserializationSchema<TelemetryEvent> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(TelemetryEventDeserializer.class);
    private transient ObjectMapper mapper;

    private ObjectMapper getMapper() {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        return mapper;
    }

    @Override
    public TelemetryEvent deserialize(byte[] bytes) throws IOException {
        try {
            return getMapper().readValue(bytes, TelemetryEvent.class);
        } catch (Exception e) {
            // Return a poison-pill marker instead of crashing.
            LOG.warn("Failed to deserialize event: {}", new String(bytes), e);
            TelemetryEvent poison = new TelemetryEvent();
            poison.setEventId("POISON-" + System.currentTimeMillis());
            poison.setVehicleId("unknown");
            poison.setEventType("PARSE_ERROR");
            poison.setEventTime(System.currentTimeMillis());
            poison.setIngestTime(System.currentTimeMillis());
            poison.setSchemaVersion("error");
            return poison;
        }
    }

    @Override
    public boolean isEndOfStream(TelemetryEvent event) {
        return false;
    }

    @Override
    public TypeInformation<TelemetryEvent> getProducedType() {
        return TypeInformation.of(TelemetryEvent.class);
    }
}
