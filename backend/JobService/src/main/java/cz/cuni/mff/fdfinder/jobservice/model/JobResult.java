package cz.cuni.mff.fdfinder.jobservice.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.persistence.*;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;

import java.util.List;

/**
 * Class representing ONE iteration of a {@link Job} on specified {@code algorithm}.
 */
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

    /**
     *
     * @return {@code true} if the {@link JobResult} is in running state;
     * {@code false} otherwise
     */
    public boolean isJobIterationRunning() {

        return (this.status == JobStatus.RUNNING || this.status == JobStatus.WAITING);
    }

    /**
     *
     * @return {@code true} if the {@link JobResult} is in the state ready to run;
     * {@code false} otherwise
     */
    public boolean isJobIterationPossibleToRun() {

        return (this.status == JobStatus.CREATED ||
                this.status == JobStatus.CANCELLED ||
                this.status == JobStatus.FAILED);
    }

    /**
     *
     * @return {@link Long} ID of the {@link JobResult}
     */
    public Long getId(){

        return this.id;
    }

    /**
     *
     * @param id set {@link Long} ID of the {@link JobResult}
     */
    public void setId(Long id){

        this.id = id;
    }

    /**
     *
     * @param startTime {@link Long} when the iteration started
     */
    public void setStartTime(Long startTime) {

        this.startTime = startTime;
        recomputeDuration();
    }

    /**
     *
     * @return {@link Long} when the iteration started
     */
    public Long getStartTime() {

        return this.startTime;
    }

    /**
     *
     * @return {@link JobStatus} of the {@link JobResult}
     */
    public JobStatus getStatus(){

        return this.status;
    }

    /**
     *
     * @param status {@link JobStatus} of the {@link JobResult}
     */
    public void setStatus(JobStatus status){

        this.status = status;
    }

    /**
     *
     * @param job {@link Job} of the {@link JobResult}
     */
    public void setJob(Job job) {

        this.job = job;
    }

    /**
     *
     * @return {@link Job} of the {@link JobResult}
     */
    public Job getJob() {

        return this.job;
    }

    /**
     *
     * @return {@link Integer} number of the iteration order of this {@link JobResult}
     */
    public int getIteration() {

        return this.iteration;
    }

    /**
     *
     * @param iteration {@link Integer} number of the iteration order of this {@link JobResult}
     */
    public void setIteration(int iteration) {

        this.iteration = iteration;
    }

    /**
     *
     * @param algorithm {@link String} algorithm name on which this {@link JobResult} will run
     */
    public void setAlgorithm(String algorithm) {

        this.algorithm = algorithm;
    }

    /**
     *
     * @return {@link String} algorithm name on which this {@link JobResult} will run
     */
    public String getAlgorithm() {

        return this.algorithm;
    }

    /**
     *
     * @param endTime {@link Long} timestamp when this iteration ended
     */
    public void setEndTime(Long endTime) {

        this.endTime = endTime;
        recomputeDuration();
    }

    /**
     *
     * @return {@link Long} timestamp when this iteration ended
     */
    public Long getEndTime() {

        return this.endTime;
    }

    /**
     * Recompute duration of the iteration based on {@code endTime} and {@code startTime}.
     */
    private void recomputeDuration() {

        if (this.endTime != null && this.startTime != null) {
            this.duration = this.endTime - this.startTime;
        }
    }

    /**
     *
     * @return {@link Long} duration of the iteration
     */
    public Long getDuration() {

        return this.duration;
    }

    /**
     *
     * @param numFoundFd {@link Integer} number of founf FDs
     */
    public void setNumFoundFd(int numFoundFd) {

        this.numFoundFd = numFoundFd;
    }

    /**
     *
     * @return {@link Integer} number of founf FDs
     */
    public int getNumFoundFd() {

        return this.numFoundFd;
    }

    /**
     *
     * @return List ({@link Snapshot}) of the {@link JobResult}
     */
    public List<Snapshot> getSnapshots() {

        return snapshots;
    }

    /**
     *
     * @param snapshots List ({@link Snapshot}) of the {@link JobResult}
     */
    public void setSnapshots(List<Snapshot> snapshots) {

        this.snapshots = snapshots;
    }

    /**
     *
     * @return {@link String} form of the {@link JobResult} metadata
     */
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
