package com.epam.overview;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class SimpleStreamApp {
    private static final String INPUT_TOPIC = "simple_input";
    private static final String OUTPUT_TOPIC = "simple_output";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "simpleStreamApp");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.99.100:9092");

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilder
                .stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                .mapValues(value -> value.toUpperCase())
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);
        kafkaStreams.start();
    }
}
