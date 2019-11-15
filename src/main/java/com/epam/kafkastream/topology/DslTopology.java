package com.epam.kafkastream.topology;

import com.epam.kafkastream.model.Component;
import com.epam.kafkastream.serde.EntitySerdes;
import com.epam.kafkastream.service.ComponentAggregator;
import com.epam.kafkastream.service.ComponentInitializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;

import java.util.Properties;

public class DslTopology {
    private static final String INPUT_TOPIC = "dsl_input";
    private static final String OUTPUT_TOPIC = "dsl_output";
    private static final long TIME_WINDOW_MS = 30000;

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "dslTopology");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.99.100:9092");
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 30000L);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), EntitySerdes.getSubcomponentSerde()))
                .groupByKey()
                .windowedBy(TimeWindows.of(TIME_WINDOW_MS))
                .aggregate(new ComponentInitializer(), new ComponentAggregator(), getWindowedMaterialized())
                .toStream()
                .map((windowedKey, value) -> new KeyValue<>(windowedKey.key(), value))
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), EntitySerdes.getComponentSerde()));

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);
        kafkaStreams.start();
    }

    private static Materialized<String, Component, WindowStore<Bytes, byte[]>>  getWindowedMaterialized() {
        return Materialized.<String, Component, WindowStore<Bytes, byte[]>>as("dsl_windowed_stateStore")
                .withKeySerde(Serdes.String())
                .withValueSerde(EntitySerdes.getComponentSerde())
                .withLoggingDisabled();
    }

    private static Materialized<String, Component, KeyValueStore<Bytes, byte[]>>  getNonWindowedMaterialized() {
        return Materialized.<String, Component, KeyValueStore<Bytes, byte[]>>as("dsl_stateStore")
                .withKeySerde(Serdes.String())
                .withValueSerde(EntitySerdes.getComponentSerde())
                .withLoggingDisabled();
    }
}