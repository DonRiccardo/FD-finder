package com.example.jobservice;

import jakarta.persistence.*;

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


    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;

    public Job() {}

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

    @Override
    public boolean equals(Object o) {

        if (this == o)
            return true;
        if (!(o instanceof Job))
            return false;
        Job job = (Job) o;
        return Objects.equals(this.id, job.id) &&
                Objects.equals(this.status, job.status);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.id, this.status);
    }

    @Override
    public String toString() {
        return "Job{" + "id=" + this.id + ", status=" + this.status + ", algorithm=" + "ALG TODO"
                + ", dataset=" + "DATASET TODO" + ", MAX=" + "MAX TODO" + ", SKIP="
                + "SKIP TODO" + ", MAX LHS=" + "MAXLHS TODO"
                + "output=" + "OUTPUT TODO" + '}';
    }
}
