package com.epam.kafkastream.serde;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;

public class JsonEntitySerializer<T> implements Serializer<T> {
    private final Serializer<String> stringSerializer = new StringSerializer();
    private final Gson gson = new Gson();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        stringSerializer.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String topic, T data) {
        final String json = gson.toJson(data);
        return stringSerializer.serialize(topic, json);
    }

    @Override
    public void close() {
        stringSerializer.close();
    }
}
