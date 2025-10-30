package cz.cuni.mff.fdfinder.jobservice;

public class ResultsNotFoundException extends RuntimeException {
    public ResultsNotFoundException(Long jobId) {
        super("Could not found results of a job with ID: " + jobId);
    }
}
