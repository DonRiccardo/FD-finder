package cz.cuni.mff.fdfinder.jobservice.model;

/**
 * Enum representing Status of the {@link Job} and {@link JobResult}.
 */
public enum JobStatus {
    /**
     * Set ONLY when creating new {@link Job}.
     * User can START {@link Job}
     */
    CREATED,
    /**
     * Set when the {@link Job} was started.
     * User can CANCEL {@link Job}.
     */
    WAITING,
    /**
     * Set when the algorithm started running the {@link Job}.
     * User can CANCEL {@link Job}.
     */
    RUNNING,
    /**
     * Set when user canceled the {@link Job}.
     * User can reSTART {@link Job}.
     */
    CANCELLED,
    /**
     * Set when something went wrong.
     * User can reSTART {@link Job}.
     */
    FAILED,
    /**
     * Set when the {@link Job} is successfully done.
     * User can do NOTHING.
     */
    DONE
    ;

}
