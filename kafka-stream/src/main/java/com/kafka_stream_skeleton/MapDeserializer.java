package com.kafka_stream_skeleton;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class MapDeserializer implements Deserializer<Map<String, Long>> {

    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public Map<String, Long> deserialize(String s, byte[] bytes) {
        try {
            return objectMapper.readValue(bytes, new TypeReference<Map<String, Long>>() {});
        } catch (IOException e) {
            System.out.println("ERROR - " +  e.getMessage());
            return null;
        }
    }

    @Override
    public void close() {

    }
}
