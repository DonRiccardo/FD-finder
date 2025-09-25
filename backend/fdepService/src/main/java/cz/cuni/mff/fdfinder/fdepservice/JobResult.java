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

    public void setStartTime(Long startTime) {

        this.startTime = startTime;
        recomputeDuration();
    }

    public void setEndTime(Long endTime) {

        this.endTime = endTime;
        recomputeDuration();
    }

    private void recomputeDuration() {

        if (endTime != null && startTime != null) {
            duration = endTime - startTime;
        }
    }

    public void setNumFoundFd(int numFoundFd) {

        this.numFoundFd = numFoundFd;
    }
}
