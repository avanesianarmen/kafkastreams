package com.epam.kafkastream.topology;

import com.epam.kafkastream.model.Component;
import com.epam.kafkastream.model.Status;
import com.epam.kafkastream.model.Subcomponent;
import com.epam.kafkastream.serde.EntitySerdes;
import com.epam.kafkastream.service.ComponentAggregator;
import com.epam.kafkastream.service.ScheduledTransformer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.*;

public class DslProcessorTopology {
    private static final String INPUT_TOPIC = "processor_input2";
    private static final String OUTPUT_TOPIC = "processor_output";
    private static final String STORE_NAME = "processor_state_store";
    private static final long TIME_WINDOW_MS = 10000L;

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "dslProcessorTopology");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.99.100:9092");

        final StoreBuilder<KeyValueStore<String, Component>> storeBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore(STORE_NAME), Serdes.String(), EntitySerdes.getComponentSerde())
                        .withLoggingDisabled();

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilder.addStateStore(storeBuilder);

        streamsBuilder
                .stream(INPUT_TOPIC, Consumed.with(Serdes.String(), EntitySerdes.getSubcomponentSerde()))
                .transform(() -> getTransformer(), STORE_NAME)
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), EntitySerdes.getComponentSerde()));


        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);
        kafkaStreams.start();

    }

    private static ScheduledTransformer<String, Subcomponent, Component> getTransformer() {
        return new ScheduledTransformer<>(TIME_WINDOW_MS, STORE_NAME, new ComponentAggregator(), getStubValues());
    }

    private static Map<String, Component> getStubValues() {
        Subcomponent subcomponent1 = new Subcomponent("subcomponent1", Status.UNKNOWN);
        Subcomponent subcomponent2 = new Subcomponent("subcomponent2", Status.UNKNOWN);
        Subcomponent subcomponent3 = new Subcomponent("subcomponent3", Status.UNKNOWN);
        List<Subcomponent> subcomponents = new ArrayList<>(Arrays.asList(subcomponent1, subcomponent2, subcomponent3));

        Component component = new Component();
        component.setComponentId("component1");
        component.setComponentStatus(Status.UNKNOWN);
        component.setSubcomponents(subcomponents);

        Map<String, Component> initValues = new HashMap<>();
        initValues.put(component.getComponentId(), component);
        return initValues;
    }
}
