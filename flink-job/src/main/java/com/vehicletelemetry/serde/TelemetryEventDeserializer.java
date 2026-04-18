package com.vehicletelemetry.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vehicletelemetry.model.TelemetryEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class TelemetryEventDeserializer implements DeserializationSchema<TelemetryEvent> {

    private static final long serialVersionUID = 1L;
    private transient ObjectMapper mapper;

    private ObjectMapper getMapper() {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        return mapper;
    }

    @Override
    public TelemetryEvent deserialize(byte[] bytes) throws IOException {
        return getMapper().readValue(bytes, TelemetryEvent.class);
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
