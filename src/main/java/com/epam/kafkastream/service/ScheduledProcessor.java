package com.epam.kafkastream.service;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Map;

public class ScheduledProcessor<K, V, R> implements Processor<K, V> {

    private long interval;
    private String stateStoreName;
    private Aggregator<K, V, R> aggregator;
    private Map<K, R> initValues;
    private KeyValueStore<K, R> stateStore;
    private ProcessorContext context;
    private Cancellable schedule;

    public ScheduledProcessor(long interval, String stateStoreName, Aggregator<K, V, R> aggregator, Map<K, R> initValues) {
        this.interval = interval;
        this.stateStoreName = stateStoreName;
        this.aggregator = aggregator;
        this.initValues = initValues;
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.stateStore = (KeyValueStore<K, R>) context.getStateStore(stateStoreName);
        this.schedule = context.schedule(interval, PunctuationType.WALL_CLOCK_TIME, this::punctuate);
        initValues.forEach(stateStore::put);
    }

    @Override
    public void process(K key, V newValue) {
        R currentValue = stateStore.get(key);
        R aggregatedValue = aggregator.apply(key, newValue, currentValue);
        stateStore.put(key, aggregatedValue);
    }

    @Override
    public void punctuate(long timestamp) {
        stateStore.all().forEachRemaining(this::forward);
    }

    protected void forward(KeyValue<K, R> keyValue) {
        context.forward(keyValue.key, keyValue.value);
    }

    @Override
    public void close() {

    }
}
