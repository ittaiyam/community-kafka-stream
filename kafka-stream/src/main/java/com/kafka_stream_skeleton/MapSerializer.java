package com.kafka_stream_skeleton;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class MapSerializer implements Serializer<Map<String, Long>> {

    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, Map<String, Long> stringLongMap) {
        try {
            return objectMapper.writeValueAsBytes(stringLongMap);
        } catch (JsonProcessingException e) {
            System.out.println("ERROR - " +  e.getMessage());
            return null;
        }
    }

    @Override
    public void close() {

    }
}
