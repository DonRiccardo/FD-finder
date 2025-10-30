package cz.cuni.mff.fdfinder.jobservice.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.persistence.*;
import jakarta.validation.constraints.*;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Class representing Job and its metadata.
 */
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
    @NotNull
    private Long dataset;
    @NotEmpty
    @NotNull
    private String datasetName;
    private int limitEntries = -1;
    private int skipEntries = -1;
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
        if (limitEntries > 0) this.limitEntries = limitEntries;
        if (skipEntries > 0) this.skipEntries = skipEntries;
        if (maxLHS >= 0) this.maxLHS = maxLHS;
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

    /**
     *
     * @return {@link Long} ID fo the {@link Job}
     */
    public Long getId() {

        return this.id;
    }

    /**
     *
     * @param id set {@link Long} ID of the {@link Job}
     */
    public void setId(Long id) {

        this.id = id;
    }

    /**
     *
     * @return {@link String} name of the {@link Job}
     */
    public String getJobName() {

        return this.jobName;
    }

    /**
     *
     * @param jobName set {@link String} name of the {@link Job}
     */
    public void setJobName(String jobName) {

        this.jobName = jobName;
    }

    /**
     *
     * @return {@link String} description of the {@link Job}
     */
    public String getJobDescription() {

        return this.jobDescription;
    }

    /**
     *
     * @param jobDescription set {@link String} description of the {@link Job}
     */
    public void setJobDescription(String jobDescription) {

        this.jobDescription = jobDescription;
    }

    /**
     *
     * @return {@link JobStatus} of the {@link Job}
     */
    public JobStatus getStatus() {

        return this.status;
    }

    /**
     *
     * @param status set {@link JobStatus} of the {@link Job}
     */
    public void setStatus(JobStatus status) {

        this.status = status;
    }

    /**
     * Set {@code status} to all {@link JobResult}s.
     * @param status {@link JobStatus} which will be set to all {@link Job} iterations.
     */
    public void setStatusToIterationsAndJob(JobStatus status) {

        for (JobResult jobResult : jobResults) {

            jobResult.setStatus(status);
        }
        this.status = status;
    }

    /**
     *
     * @return {@code true} if the {@link Job} is in running state;
     * {@code false} otherwise
     */
    public boolean isJobRunning() {

        return (this.status == JobStatus.RUNNING || this.status == JobStatus.WAITING);
    }

    /**
     *
     * @return {@code true} if the {@link Job} is in the state ready to run;
     * {@code false} otherwise
     */
    public boolean isJobPossibleToRun() {

        return (this.status == JobStatus.CREATED ||
                this.status == JobStatus.CANCELLED ||
                this.status == JobStatus.FAILED);
    }

    /**
     * Set new {@link JobStatus} to {@link Job} based on statuses of {@link JobResult}.
     * @return {@code true} if the new {@link JobStatus} changed; {@code false} otherwise
     */
    public boolean setAndChangeJobStatusBasedOnIterations() {

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

    /**
     * Set new {@link JobStatus} only to {@link JobResult} for the specified {@code algorithm}.
     * Then check and change {@link JobStatus} of the {@link Job}.
     * @param algorithm name of the algorithm
     * @param status new {@link JobStatus} to set
     */
    public void setAlgorithmIterationStatus(String algorithm, JobStatus status) {

        for (JobResult jobResult : this.jobResults) {
            if (jobResult.getAlgorithm().equals(algorithm)) {

                jobResult.setStatus(status);
            }
        }

        setAndChangeJobStatusBasedOnIterations();
    }

    /**
     *
     * @return List ({@link String}) of algorithm names on which the {@link Job} will run
     */
    public List<String> getAlgorithm() {

        return this.algorithm;
    }

    /**
     *
     * @param algorithm set List ({@link String}) of algorithm names on which the {@link Job} will run
     */
    public void setAlgorithm(List<String> algorithm) {

        this.algorithm = algorithm;
    }

    /**
     *
     * @return {@link Integer} number of repetition of the {@link Job} on every algorithm
     */
    public int getRepeat() {

        return this.repeat;
    }

    /**
     *
     * @param repeat set {@link Integer} number of repetition of the {@link Job} on every algorithm
     */
    public void setRepeat(int repeat) {

        this.repeat = repeat;
    }

    /**
     *
     * @return {@link Long} dataset ID
     */
    public Long getDataset() {

        return this.dataset;
    }

    /**
     *
     * @param dataset set {@link Long} dataset ID
     */
    public void setDataset(Long dataset) {

        this.dataset = dataset;
    }

    /**
     *
     * @param datasetName set {@link String} dataset name
     */
    public void setDatasetName(String datasetName) {

        this.datasetName = datasetName;
    }

    /**
     *
     * @return {@link String} dataset name
     */
    public String getDatasetName() {

        return this.datasetName;
    }

    /**
     *
     * @return {@link Integer} maximal number of rows which will be read in the {@link Job} execution
     */
    public int getLimitEntries() {

        return this.limitEntries;
    }

    /**
     * Set positive value
     * @param limitEntries set {@link Integer} maximal number of rows which will be read in the {@link Job} execution
     */
    public void setLimitEntries(int limitEntries) {

        if (limitEntries > 0) this.limitEntries = limitEntries;
    }

    /**
     *
     * @return {@link Integer} number of rows which will be skipped in the {@link Job} execution
     */
    public int getSkipEntries() {

        return this.skipEntries;
    }

    /**
     * Set positive value
     * @param skipEntries set {@link Integer} number of rows which will be skipped in the {@link Job} execution
     */
    public void setSkipEntries(int skipEntries) {

        if(skipEntries > 0) this.skipEntries = skipEntries;
    }

    /**
     *
     * @return {@link Integer} maximal number of attributes in the LHS pf found FDs
     */
    public int getMaxLHS() {

        return this.maxLHS;
    }

    /**
     * Set positive value or zero
     * @param maxLHS set {@link Integer} maximal number of attributes in the LHS pf found FDs
     */
    public void setMaxLHS(int maxLHS) {

        if (maxLHS >= 0) this.maxLHS = maxLHS;
    }

    /**
     *
     * @return {@link LocalDateTime} when the {@link Job} was created
     */
    public LocalDateTime getCreatedAt() {

        return this.createdAt;
    }

    /**
     *
     * @return {@link LocalDateTime} when the {@link Job} was updated
     */
    public LocalDateTime getUpdatedAt() {

        return this.updatedAt;
    }

    /**
     *
     * @return List ({@link JobResult}) of {@link Job} results from iterations
     */
    public List<JobResult> getJobResults() {

        return this.jobResults;
    }

    /**
     *
     * @param jobResults set {@link JobResult} for each {@link Job} iteration
     */
    public void setJobResults(List<JobResult> jobResults) {

        this.jobResults = jobResults;
    }

    /**
     * Generate and set new {@link JobResult} for each {@link Job} iteration
     */
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

    /**
     *
     * @return {@link String} form of the {@link Job} metadata
     */
    @Override
    public String toString() {
        return "Job{" + "id=" + this.id + ", status=" + this.status + ", algorithm=" + this.algorithm
                + ", dataset=" + this.dataset + ", MAX=" + this.limitEntries + ", SKIP="
                + this.skipEntries + ", MAX LHS=" + this.maxLHS
                + "repeat=" + this.repeat + '}';
    }
}
