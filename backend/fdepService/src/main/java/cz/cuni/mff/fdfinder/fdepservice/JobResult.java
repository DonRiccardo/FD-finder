package cz.cuni.mff.fdfinder.fdepservice;

public class JobResult {

    Long jobId;
    Long startTime;
    Long endTime;
    Long duration;

    int numFoundFd;

    public JobResult(Long jobId) {

        this.jobId = jobId;
    }

    public Long getJobId() {

        return jobId;
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
