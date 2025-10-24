package cz.cuni.mff.fdfinder.fastfdsservice.model;

public enum JobStatus {
    CREATED,    // user can START job
    WAITING,    // user can CANCEL job
    RUNNING,    // user can CANCEL jon
    CANCELLED,  // user can reSTART job
    FAILED,     // user can do NOTHING
    DONE        // user can do nothing?
}
