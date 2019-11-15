package com.epam.kafkastream.serde;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;
import java.util.Objects;

public class JsonEntityDeserializer<T> implements Deserializer<T> {
    private final Gson gson = new Gson();
    private final Deserializer<String> stringDeserializer = new StringDeserializer();
    private Class<T> clazz;

    public JsonEntityDeserializer(Class<T> clazz) {
        this.clazz = clazz;
    }

    public JsonEntityDeserializer() {
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        stringDeserializer.configure(configs, isKey);

        String propertyName = isKey ? "key.deserializer.class" : "value.deserializer.class";
        clazz = (Class<T>) configs.get(propertyName);
        if (Objects.isNull(clazz)) {
            throw new IllegalArgumentException("deserializer.class is mandatory");
        }
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (Objects.isNull(clazz)) {
            throw new IllegalArgumentException("Not configured properly. deserializer.class is missed");
        }
        final String json = stringDeserializer.deserialize(topic, data);
        return gson.fromJson(json, clazz);
    }

    @Override
    public void close() {
        stringDeserializer.close();
    }
}