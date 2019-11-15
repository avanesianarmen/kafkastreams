package com.epam.kafkastream.model;


import java.util.ArrayList;
import java.util.List;

public class Component {

    private String componentId;
    private Status componentStatus;
    private List<Subcomponent> subcomponents = new ArrayList<>();

    public Component() {
    }

    public Component(String componentId, Status componentStatus, List<Subcomponent> subcomponents) {
        this.componentId = componentId;
        this.componentStatus = componentStatus;
        this.subcomponents = subcomponents;
    }

    public String getComponentId() {
        return componentId;
    }

    public void setComponentId(String componentId) {
        this.componentId = componentId;
    }

    public Status getComponentStatus() {
        return componentStatus;
    }

    public void setComponentStatus(Status componentStatus) {
        this.componentStatus = componentStatus;
    }

    public List<Subcomponent> getSubcomponents() {
        return subcomponents;
    }

    public void setSubcomponents(List<Subcomponent> subcomponents) {
        this.subcomponents = subcomponents;
    }

    public void addSubcomponent(Subcomponent subcomponent) {
        subcomponents.add(subcomponent);
    }
}
