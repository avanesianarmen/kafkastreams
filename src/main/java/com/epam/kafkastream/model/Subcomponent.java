package com.epam.kafkastream.model;

public class Subcomponent {

    private String subcomponentId;
    private Status subcomponentStatus;

    public Subcomponent() {
    }

    public Subcomponent(String subcomponentId, Status subcomponentStatus) {
        this.subcomponentId = subcomponentId;
        this.subcomponentStatus = subcomponentStatus;
    }

    public String getSubcomponentId() {
        return subcomponentId;
    }

    public void setSubcomponentId(String subcomponentId) {
        this.subcomponentId = subcomponentId;
    }

    public Status getSubcomponentStatus() {
        return subcomponentStatus;
    }

    public void setSubcomponentStatus(Status subcomponentStatus) {
        this.subcomponentStatus = subcomponentStatus;
    }
}
