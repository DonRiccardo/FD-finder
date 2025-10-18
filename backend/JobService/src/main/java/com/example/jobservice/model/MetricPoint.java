package com.example.jobservice.model;

import com.example.jobservice.JobController;
import jakarta.persistence.Embeddable;
import org.springframework.hateoas.EntityModel;
import org.springframework.hateoas.server.RepresentationModelAssembler;
import org.springframework.stereotype.Component;

import static org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.linkTo;
import static org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.methodOn;

@Embeddable
public class MetricPoint {

    private long timeStamp;
    private double cpuLoad;
    private long usedMemory;

    public MetricPoint(long timestamp, double cpuLoad, long usedMemory) {
        this.timeStamp = timestamp;
        this.cpuLoad = cpuLoad;
        this.usedMemory = usedMemory;
    }

    public MetricPoint() {

    }

    public long getTimestamp() {

        return timeStamp;
    }

    public void setTimestamp(long timestamp) {

        this.timeStamp = timestamp;
    }

    public double getCpuLoad() {

        return cpuLoad;
    }

    public void setCpuLoad(double cpuLoad) {

        this.cpuLoad = cpuLoad;
    }

    public long getUsedMemory() {

        return usedMemory;
    }

    public void setUsedMemory(long usedMemory) {

        this.usedMemory = usedMemory;
    }


}
