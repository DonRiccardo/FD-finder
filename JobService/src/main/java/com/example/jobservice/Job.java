package com.example.jobservice;

import jakarta.persistence.*;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Objects;

@Entity
public class Job {

    @Id
    @GeneratedValue
    private Long id;
    @Enumerated(EnumType.STRING)
    private JobStatus status;
    @NotEmpty
    private String algorithm;
    @NotEmpty
    private Long dataset;
    private Long maxEntries;
    private Long skipEntries;
    private int maxLHS;
    private String output;

    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;

    public Job() {}

    public Job(String algorithm, Long dataset, Long maxEntries, Long skipEntries, int maxLHS, String output) {

        this.algorithm = algorithm;
        this.dataset = dataset;
        this.maxEntries = maxEntries;
        this.skipEntries = skipEntries;
        this.maxLHS = maxLHS;
        this.output = output;
    }

    @PrePersist
    public void prePersist() {
        this.createdAt = LocalDateTime.now();
        this.updatedAt = LocalDateTime.now();
        this.status = JobStatus.CREATED;
    }

    @PreUpdate
    public void preUpdate() {

        this.updatedAt = LocalDateTime.now();
    }

    public Long getId() {

        return this.id;
    }

    public void setId(Long id) {

        this.id = id;
    }

    public JobStatus getStatus() {

        return this.status;
    }

    public void setStatus(JobStatus status) {

        this.status = status;
    }

    public boolean isJobRunning() {

        return (this.status == JobStatus.RUNNING || this.status == JobStatus.WAITING);
    }

    public boolean isJobPossibleToRun() {

        return (this.status == JobStatus.CREATED ||
                this.status == JobStatus.CANCELLED);
    }

    public String getAlgorithm() {

        return this.algorithm;
    }

    public void setAlgorithm(String algorithm) {

        this.algorithm = algorithm;
    }

    public Long getDataset() {

        return this.dataset;
    }

    public void setDataset(Long dataset) {

        this.dataset = dataset;
    }

    public Long getMaxEntries() {

        return this.maxEntries;
    }

    public void setMaxEntries(Long maxEntries) {

        this.maxEntries = maxEntries;
    }

    public Long getSkipEntries() {

        return this.skipEntries;
    }

    public void setSkipEntries(Long skipEntries) {

        this.skipEntries = skipEntries;
    }

    public int getMaxLHS() {

        return this.maxLHS;
    }

    public void setMaxLHS(int maxLHS) {

        this.maxLHS = maxLHS;
    }

    public String getOutput() {

        return this.output;
    }

    public void setOutput(String output) {

        this.output = output;
    }








    @Override
    public boolean equals(Object o) {

        if (this == o)
            return true;
        if (!(o instanceof Job))
            return false;
        Job job = (Job) o;
        return Objects.equals(this.id, job.id) &&
                Objects.equals(this.status, job.status) &&
                Objects.equals(this.algorithm, job.algorithm) &&
                Objects.equals(this.dataset, job.dataset) &&
                Objects.equals(this.maxEntries, job.maxEntries) &&
                Objects.equals(this.skipEntries, job.skipEntries) &&
                Objects.equals(this.maxLHS, job.maxLHS) &&
                Objects.equals(this.output, job.output);

    }

    @Override
    public int hashCode() {

        return Objects.hash(this.id, this.status, this.algorithm, this.dataset, this.maxEntries, this.skipEntries, this.maxLHS, this.output);
    }

    @Override
    public String toString() {
        return "Job{" + "id=" + this.id + ", status=" + this.status + ", algorithm=" + "ALG TODO"
                + ", dataset=" + this.dataset + ", MAX=" + this.maxEntries + ", SKIP="
                + this.skipEntries + ", MAX LHS=" + this.maxLHS
                + "output=" + this.output + '}';
    }
}
