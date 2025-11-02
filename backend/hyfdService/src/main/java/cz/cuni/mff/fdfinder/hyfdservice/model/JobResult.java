package cz.cuni.mff.fdfinder.hyfdservice.model;

import java.util.List;

/**
 * Class representing ONE iteration of a {@link JobDto} on specified {@code algorithm}.
 */
public class JobResult {

    private Long id;
    private JobStatus status;
    private int iteration;
    private String algorithm;
    private Long startTime;
    private Long endTime;
    private Long duration;

    private int numFoundFd;
    private List<Snapshot> snapshots;

    public JobResult() {

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

        snapshots.forEach(snapshot -> {
           snapshot.setTimestamp(snapshot.getTimestamp() - this.startTime);
        });

        this.snapshots = snapshots;
    }

    /**
     *
     * @return {@link Long} ID of the {@link JobResult}
     */
    public Long getId() {

        return id;
    }

    /**
     *
     * @param id set {@link Long} ID of the {@link JobResult}
     */
    public void setId(Long id) {

        this.id = id;
    }

    /**
     *
     * @return {@link JobStatus} of the {@link JobResult}
     */
    public JobStatus getStatus() {

        return status;
    }

    /**
     *
     * @param status {@link JobStatus} of the {@link JobResult}
     */
    public void setStatus(JobStatus status) {

        this.status = status;
    }

    /**
     *
     * @return {@link Integer} number of the iteration order of this {@link JobResult}
     */
    public int getIteration() {

        return iteration;
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
     * @return {@link String} algorithm name on which this {@link JobResult} will run
     */
    public String getAlgorithm() {

        return algorithm;
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

        return startTime;
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

        return endTime;
    }

    /**
     * Recompute duration of the iteration based on {@code endTime} and {@code startTime}.
     */
    private void recomputeDuration() {

        if (endTime != null && startTime != null) {
            duration = endTime - startTime;
            System.out.println("RECOMPUTED DURATION: " + duration);
        }
    }

    /**
     *
     * @return {@link Long} duration of the iteration
     */
    public Long getDuration() {

        return duration;
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

        return numFoundFd;
    }
}
