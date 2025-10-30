package cz.cuni.mff.fdfinder.depminerservice.model;

/**
 * Enum representing Status of the {@link JobDto} and {@link JobResult}.
 */
public enum JobStatus {
    /**
     * Set ONLY when creating new {@link JobDto}.
     * User can START {@link JobDto}
     */
    CREATED,
    /**
     * Set when the {@link JobDto} was started.
     * User can CANCEL {@link JobDto}.
     */
    WAITING,
    /**
     * Set when the algorithm started running the {@link JobDto}.
     * User can CANCEL {@link JobDto}.
     */
    RUNNING,
    /**
     * Set when user canceled the {@link JobDto}.
     * User can reSTART {@link JobDto}.
     */
    CANCELLED,
    /**
     * Set when something went wrong.
     * User can reSTART {@link JobDto}.
     */
    FAILED,
    /**
     * Set when the {@link JobDto} is successfully done.
     * User can do NOTHING.
     */
    DONE
    ;

}
