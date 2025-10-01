package com.example.jobservice;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;

@Entity
public class JobResult {

    @Id
    Long jobId;
    Long startTime;
    Long endTime;
    Long duration;

    int numFoundFd;

    public JobResult(Long jobId) {

        this.jobId = jobId;
    }

    public JobResult() {

    }

    public void setStartTime(Long startTime) {

        this.startTime = startTime;
        recomputeDuration();
    }

    public void setEndTime(Long endTime) {

        this.endTime = endTime;
        recomputeDuration();
    }

    private void recomputeDuration() {

        if (endTime != null && startTime != null) {
            duration = endTime - startTime;
        }
    }

    public void setNumFoundFd(int numFoundFd) {

        this.numFoundFd = numFoundFd;
    }
}
