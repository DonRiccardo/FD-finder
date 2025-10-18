package com.example.jobservice.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.persistence.*;
import jakarta.validation.constraints.*;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Entity
public class Job {

    @Id
    @GeneratedValue
    private Long id;
    @NotEmpty
    private String jobName;
    private String jobDescription;
    @Enumerated(EnumType.STRING)
    private JobStatus status;
    @NotEmpty
    private List<String> algorithm;
    @Positive
    @Max(20)
    private int repeat;
    @NotEmpty
    @NotNull
    private Long dataset;
    @NotEmpty
    @NotNull
    private String datasetName;
    @PositiveOrZero
    private int limitEntries = -1;
    @PositiveOrZero
    private int skipEntries = -1;
    @PositiveOrZero
    private int maxLHS = -1;
    @JsonIgnoreProperties("job")
    @OneToMany(mappedBy = "job", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<JobResult> jobResults = new ArrayList<>();

    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;

    public Job() {}

    public Job(List<String> algorithm, int repeat, Long dataset, String datasetName, int limitEntries, int skipEntries, int maxLHS) {

        this.algorithm = algorithm;
        this.repeat = repeat;
        this.dataset = dataset;
        this.datasetName = datasetName;
        this.limitEntries = limitEntries;
        this.skipEntries = skipEntries;
        this.maxLHS = maxLHS;
    }

    @PrePersist
    public void prePersist() {
        this.createdAt = LocalDateTime.now();
        this.updatedAt = getCreatedAt();
        this.status = JobStatus.CREATED;
        generateJobResultsObjects();
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

    public String getJobName() {

        return this.jobName;
    }

    public void setJobName(String jobName) {

        this.jobName = jobName;
    }

    public String getJobDescription() {

        return this.jobDescription;
    }

    public void setJobDescription(String jobDescription) {

        this.jobDescription = jobDescription;
    }

    public JobStatus getStatus() {

        return this.status;
    }

    public void setStatus(JobStatus status) {

        this.status = status;
    }

    public void setStatusToIterationsAndJob(JobStatus status) {

        for (JobResult jobResult : jobResults) {

            jobResult.setStatus(status);
        }
        this.status = status;
    }

    public boolean isJobRunning() {

        return (this.status == JobStatus.RUNNING || this.status == JobStatus.WAITING);
    }

    public boolean isJobPossibleToRun() {

        return (this.status == JobStatus.CREATED ||
                this.status == JobStatus.CANCELLED ||
                this.status == JobStatus.FAILED);
    }

    public boolean setAndChangedJobStatusBasedOnIterations() {

        JobStatus oldJobStatus = this.status;

        boolean allDone = jobResults.stream()
                .allMatch(run -> run.getStatus() == JobStatus.DONE);
        boolean anyRunning = jobResults.stream()
                .anyMatch(run -> run.getStatus() == JobStatus.RUNNING);
        boolean anyFailed = jobResults.stream()
                .anyMatch(run -> run.getStatus() == JobStatus.FAILED);

        if (anyFailed) {
            setStatus(JobStatus.FAILED);
        } else if (allDone) {
            setStatus(JobStatus.DONE);
        } else if (anyRunning) {
            setStatus(JobStatus.RUNNING);
        } else {

        }

        return oldJobStatus != this.status;
    }

    public void setAlgorithmIterationStatus(String algorithm, JobStatus status) {

        for (JobResult jobResult : this.jobResults) {
            if (jobResult.getAlgorithm().equals(algorithm)) {

                jobResult.setStatus(status);
            }
        }

        setAndChangedJobStatusBasedOnIterations();
    }

    public List<String> getAlgorithm() {

        return this.algorithm;
    }

    public void setAlgorithm(List<String> algorithm) {

        this.algorithm = algorithm;
    }

    public int getRepeat() {

        return this.repeat;
    }

    public void setRepeat(int repeat) {

        this.repeat = repeat;
    }

    public Long getDataset() {

        return this.dataset;
    }

    public void setDataset(Long dataset) {

        this.dataset = dataset;
    }

    public void setDatasetName(String datasetName) {

        this.datasetName = datasetName;
    }

    public String getDatasetName() {

        return this.datasetName;
    }

    public int getLimitEntries() {

        return this.limitEntries;
    }

    public void setLimitEntries(int limitEntries) {

        this.limitEntries = limitEntries;
    }

    public int getSkipEntries() {

        return this.skipEntries;
    }

    public void setSkipEntries(int skipEntries) {

        this.skipEntries = skipEntries;
    }

    public int getMaxLHS() {

        return this.maxLHS;
    }

    public void setMaxLHS(int maxLHS) {

        this.maxLHS = maxLHS;
    }

    public  LocalDateTime getCreatedAt() {

        return this.createdAt;
    }

    public LocalDateTime getUpdatedAt() {

        return this.updatedAt;
    }

    public List<JobResult> getJobResults() {

        return this.jobResults;
    }

    public void setJobResults(List<JobResult> jobResults) {

        this.jobResults = jobResults;
    }

    private void generateJobResultsObjects() {
        for (String algName : this.algorithm) {
            for (int i = 1; i <= this.repeat; i++) {

                JobResult jobResult = new JobResult();
                jobResult.setAlgorithm(algName);
                jobResult.setIteration(i);
                jobResult.setJob(this);
                jobResults.add(jobResult);
            }
        }
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
                Objects.equals(this.limitEntries, job.limitEntries) &&
                Objects.equals(this.skipEntries, job.skipEntries) &&
                Objects.equals(this.maxLHS, job.maxLHS) &&
                Objects.equals(this.repeat, job.repeat);

    }

    @Override
    public int hashCode() {

        return Objects.hash(this.id, this.status, this.algorithm, this.dataset, this.limitEntries, this.skipEntries, this.maxLHS, this.repeat);
    }

    @Override
    public String toString() {
        return "Job{" + "id=" + this.id + ", status=" + this.status + ", algorithm=" + this.algorithm
                + ", dataset=" + this.dataset + ", MAX=" + this.limitEntries + ", SKIP="
                + this.skipEntries + ", MAX LHS=" + this.maxLHS
                + "repeat=" + this.repeat + '}';
    }
}
