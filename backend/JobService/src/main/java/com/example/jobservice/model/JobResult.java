package com.example.jobservice.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.persistence.*;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;

import java.util.List;

@Entity
public class JobResult {

    @Id
    @GeneratedValue
    private Long id;
    @ManyToOne
    @JoinColumn(name = "job_id")
    @JsonIgnoreProperties("jobResults")
    private Job job;
    @Enumerated(EnumType.STRING)
    private JobStatus status;
    @Positive
    private int iteration;
    @NotEmpty
    private String algorithm;
    private Long startTime;
    private Long endTime;
    private Long duration;
    @PositiveOrZero
    private int numFoundFd;
    //@JsonIgnore
    @ElementCollection(fetch = FetchType.EAGER)
    private List<Snapshot> snapshots;

    public JobResult(Job job) {

        this.status = JobStatus.CREATED;
        this.job = job;
    }

    public JobResult() {
        this.status = JobStatus.CREATED;
    }

    public boolean isJobRunning() {

        return (this.status == JobStatus.RUNNING || this.status == JobStatus.WAITING);
    }

    public boolean isJobPossibleToRun() {

        return (this.status == JobStatus.CREATED ||
                this.status == JobStatus.CANCELLED ||
                this.status == JobStatus.FAILED);
    }

    public Long getId(){

        return this.id;
    }

    public void setId(Long id){

        this.id = id;
    }

    public void setStartTime(Long startTime) {

        this.startTime = startTime;
        recomputeDuration();
    }

    public Long getStartTime() {

        return this.startTime;
    }

    public JobStatus getStatus(){

        return this.status;
    }

    public void setStatus(JobStatus status){

        this.status = status;
    }

    public void setJob(Job job) {

        this.job = job;
    }

    public Job getJob() {

        return this.job;
    }

    public int getIteration() {

        return this.iteration;
    }

    public void setIteration(int iteration) {

        this.iteration = iteration;
    }

    public void setAlgorithm(String algorithm) {

        this.algorithm = algorithm;
    }

    public String getAlgorithm() {

        return this.algorithm;
    }

    public void setEndTime(Long endTime) {

        this.endTime = endTime;
        recomputeDuration();
    }

    public Long getEndTime() {

        return this.endTime;
    }

    private void recomputeDuration() {

        if (this.endTime != null && this.startTime != null) {
            this.duration = this.endTime - this.startTime;
        }
    }

    public Long getDuration() {

        return this.duration;
    }

    public void setNumFoundFd(int numFoundFd) {

        this.numFoundFd = numFoundFd;
    }

    public int getNumFoundFd() {

        return this.numFoundFd;
    }

    public List<Snapshot> getSnapshots() {

        return snapshots;
    }

    public void setSnapshots(List<Snapshot> snapshots) {

        this.snapshots = snapshots;
    }

    @Override
    public String toString() {

        return "JobResult{" +
                "id=" + id +
                ", jobId=" + (job != null ? job.getId() : null) +
                ", status=" + status +
                ", iteration=" + iteration +
                ", algorithm='" + algorithm + '\'' +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", duration=" + duration +
                ", numFoundFd=" + numFoundFd +
                '}';
    }
}
