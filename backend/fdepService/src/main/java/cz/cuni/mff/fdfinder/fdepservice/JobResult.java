package cz.cuni.mff.fdfinder.fdepservice;

import java.util.List;

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

    public List<Snapshot> getSnapshots() {

        return snapshots;
    }

    public void setSnapshots(List<Snapshot> snapshots) {

        snapshots.forEach(snapshot -> {
           snapshot.setTimestamp(snapshot.getTimestamp() - this.startTime);
        });

        this.snapshots = snapshots;
    }

    public Long getId() {

        return id;
    }

    public void setId(Long id) {

        this.id = id;
    }

    public JobStatus getStatus() {

        return status;
    }
    public void setStatus(JobStatus status) {

        this.status = status;
    }

    public int getIteration() {

        return iteration;
    }

    public void setIteration(int iteration) {

        this.iteration = iteration;
    }

    public String getAlgorithm() {

        return algorithm;
    }

    public void setAlgorithm(String algorithm) {

        this.algorithm = algorithm;
    }

    public void setStartTime(Long startTime) {

        this.startTime = startTime;
        recomputeDuration();
    }

    public Long getStartTime() {

        return startTime;
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
            System.out.println("RECOMPUTED DURATION: " + duration);
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
