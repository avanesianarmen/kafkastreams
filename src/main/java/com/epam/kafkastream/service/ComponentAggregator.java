package com.epam.kafkastream.service;

import com.epam.kafkastream.model.Component;
import com.epam.kafkastream.model.Status;
import com.epam.kafkastream.model.Subcomponent;
import org.apache.kafka.streams.kstream.Aggregator;

import java.util.Optional;

public class ComponentAggregator implements Aggregator<String, Subcomponent, Component> {
    @Override
    public Component apply(String componentKey, Subcomponent subcomponent, Component aggregatedComponent) {
        aggregatedComponent.setComponentId(componentKey);
        Optional<Subcomponent> subcomponentToUpdate = aggregatedComponent.getSubcomponents()
                .stream()
                .filter(s -> s.getSubcomponentId().equals(subcomponent.getSubcomponentId()))
                .findFirst();
        if (subcomponentToUpdate.isPresent()) {
            subcomponentToUpdate.get().setSubcomponentStatus(subcomponent.getSubcomponentStatus());
        } else {
            aggregatedComponent.getSubcomponents().add(subcomponent);
        }

        if (anyStatus(aggregatedComponent, Status.KO)) {
            aggregatedComponent.setComponentStatus(Status.KO);
        } else if(anyStatus(aggregatedComponent, Status.UNKNOWN)) {
            aggregatedComponent.setComponentStatus(Status.UNKNOWN);
        } else if(allStatus(aggregatedComponent, Status.OK)) {
            aggregatedComponent.setComponentStatus(Status.OK);

        }

        return aggregatedComponent;
    }

    private boolean anyStatus(Component aggregatedComponent, Status status) {
        return aggregatedComponent.getSubcomponents().stream()
                .map(Subcomponent::getSubcomponentStatus)
                .anyMatch(status::equals);
    }

    private boolean allStatus(Component aggregatedComponent, Status status) {
        return aggregatedComponent.getSubcomponents().stream()
                .map(Subcomponent::getSubcomponentStatus)
                .allMatch(status::equals);
    }


}
