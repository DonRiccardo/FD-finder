package com.example.jobservice;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
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

    public Long getStartTime() {

        return startTime;
    }

    public void setJobId(Long jobId) {

        this.jobId = jobId;
    }

    public Long getJobId() {

        return jobId;
    }

    public void setEndTime(Long endTime) {

        this.endTime = endTime;
        recomputeDuration();
    }

    public Long getEndTime() {

        return endTime;
    }

    private void recomputeDuration() {

        if (endTime != null && startTime != null) {
            duration = endTime - startTime;
        }
    }

    public Long getDuration() {

        return duration;
    }

    public void setNumFoundFd(int numFoundFd) {

        this.numFoundFd = numFoundFd;
    }

    public int getNumFoundFd() {

        return numFoundFd;
    }
}
