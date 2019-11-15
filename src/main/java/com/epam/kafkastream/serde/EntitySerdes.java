package com.epam.kafkastream.serde;

import java.util.Collections;

import com.epam.kafkastream.model.Component;
import com.epam.kafkastream.model.Subcomponent;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public final class EntitySerdes {
    private EntitySerdes() {
    }

    public static Serde<Component> getComponentSerde() {
        return getSerdeFor(Component.class);
    }

    public static Serde<Subcomponent> getSubcomponentSerde() {
        return getSerdeFor(Subcomponent.class);
    }

    private static <T> Serde<T> getSerdeFor(Class<T> clazz) {
        final Serde<T> serde =
                Serdes.serdeFrom(new JsonEntitySerializer<>(), new JsonEntityDeserializer<>());
        serde.configure(Collections.singletonMap("value.deserializer.class", clazz), false);
        return serde;
    }
}

