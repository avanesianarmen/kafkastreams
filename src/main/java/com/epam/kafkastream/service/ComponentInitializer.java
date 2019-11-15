package com.epam.kafkastream.service;

import com.epam.kafkastream.model.Component;
import com.epam.kafkastream.model.Status;
import com.epam.kafkastream.model.Subcomponent;
import org.apache.kafka.streams.kstream.Initializer;

import java.util.ArrayList;
import java.util.Arrays;

public class ComponentInitializer implements Initializer<Component> {
    @Override
    public Component apply() {
        Component initialComponent = new Component();
        initialComponent.setComponentStatus(Status.UNKNOWN);
        initialComponent.setSubcomponents(new ArrayList<>(Arrays.asList(
                new Subcomponent("subcomponent1", Status.UNKNOWN),
                new Subcomponent("subcomponent2",Status.UNKNOWN),
                new Subcomponent("subcomponent3",Status.UNKNOWN))));
        return initialComponent;
    }
}
