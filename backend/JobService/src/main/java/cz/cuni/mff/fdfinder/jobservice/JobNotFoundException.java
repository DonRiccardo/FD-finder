package cz.cuni.mff.fdfinder.jobservice;

public class JobNotFoundException extends RuntimeException {
    public JobNotFoundException(Long id) {
        super("Could not find job with ID: " + id);
    }
}
