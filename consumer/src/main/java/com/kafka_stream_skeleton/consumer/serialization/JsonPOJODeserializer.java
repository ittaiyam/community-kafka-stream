package com.kafka_stream_skeleton.consumer.serialization;


import com.cellwize.model.KPIDataPoint;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class JsonPOJODeserializer implements Deserializer<KPIDataPoint> {

    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> map, boolean b) {
    }

    @Override
    public KPIDataPoint deserialize(String topic, byte[] data) {
        if (data == null)
            return null;

        KPIDataPoint kpiDataPoint;
        try {
            kpiDataPoint = objectMapper.readValue(data, KPIDataPoint.class);
        } catch (Exception e) {
            throw new SerializationException(e);
        }

        return kpiDataPoint;
    }


    @Override
    public void close() {
    }

}