package com.epam.kafkastream.topology;

import com.epam.kafkastream.model.Component;
import com.epam.kafkastream.model.Status;
import com.epam.kafkastream.model.Subcomponent;
import com.epam.kafkastream.serde.EntitySerdes;
import com.epam.kafkastream.service.ComponentAggregator;
import com.epam.kafkastream.service.ScheduledProcessor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.*;

public class ProcessorTopology {
    private static final String INPUT_TOPIC = "processor_input";
    private static final String OUTPUT_TOPIC = "processor_output";
    private static final String STORE_NAME = "processor_state_store";
    private static final long TIME_WINDOW_MS = 60 * 1000;

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream_processor_app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        final StoreBuilder<KeyValueStore<String, Component>> storeBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore(STORE_NAME), Serdes.String(), EntitySerdes.getComponentSerde())
                        .withLoggingDisabled();

        Serde<Subcomponent> subcomponentSerde = EntitySerdes.getSubcomponentSerde();
        Serde<Component> componentSerde = EntitySerdes.getComponentSerde();

        Topology topology = new Topology();
        topology.addSource("SourceTopicProcessor", new StringDeserializer(), subcomponentSerde.deserializer(), INPUT_TOPIC)
                .addProcessor("SubcomponentAggregator", () -> getProcessor(), "SourceTopicProcessor")
                .addStateStore(storeBuilder, "SubcomponentAggregator")
                .addSink("AggregationSink", OUTPUT_TOPIC, new StringSerializer(), componentSerde.serializer(), "SubcomponentAggregator");
        KafkaStreams kafkaStreams = new KafkaStreams(topology, props);
        kafkaStreams.start();
    }

    private static ScheduledProcessor<String, Subcomponent, Component> getProcessor() {
        return new ScheduledProcessor<>(TIME_WINDOW_MS, STORE_NAME, new ComponentAggregator(), getStubValues());
    }

    private static Map<String, Component> getStubValues() {
        Subcomponent subcomponent1 = new Subcomponent("subcomponent1", Status.UNKNOWN);
        Subcomponent subcomponent2 = new Subcomponent("subcomponent2", Status.UNKNOWN);
        List<Subcomponent> subcomponents = new ArrayList<>(Arrays.asList(subcomponent1, subcomponent2));

        Component component = new Component();
        component.setComponentId("component1");
        component.setComponentStatus(Status.UNKNOWN);
        component.setSubcomponents(subcomponents);

        Map<String, Component> initValues = new HashMap<>();
        initValues.put(component.getComponentId(), component);
        return initValues;
    }
}
