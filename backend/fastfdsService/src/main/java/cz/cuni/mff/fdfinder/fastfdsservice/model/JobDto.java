package cz.cuni.mff.fdfinder.fastfdsservice.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Class representing Job and its metadata.
 */
public class JobDto {

    private Long id;
    private JobStatus status;
    private List<String> algorithm;
    private int repeat;
    private Long dataset;
    private String datasetName;
    private int limitEntries;
    private int skipEntries;
    private int maxLHS;
    private List<JobResult> jobResults = new ArrayList<>();


    public JobDto() {}

    public JobDto(Long id, List<String> algorithm, int repeat, Long dataset, String datasetName, JobStatus status, int limitEntries,
                  int skipEntries, int maxLHS, String output) {

        this.id = id;
        this.algorithm = algorithm;
        this.repeat = repeat;
        this.dataset = dataset;
        this.datasetName = datasetName;
        this.status = status;
        this.limitEntries = limitEntries;
        this.skipEntries = skipEntries;
        this.maxLHS = maxLHS;
    }

    /**
     *
     * @return {@link Long} ID fo the {@link JobDto}
     */
    public Long getId() {

        return this.id;
    }

    /**
     *
     * @param id set {@link Long} ID of the {@link JobDto}
     */
    public void setId(Long id) {

        this.id = id;
    }

    /**
     *
     * @return {@link JobStatus} of the {@link JobDto}
     */
    public JobStatus getStatus() {

        return this.status;
    }

    /**
     *
     * @param status set {@link JobStatus} of the {@link JobDto}
     */
    public void setStatus(JobStatus status) {

        this.status = status;
    }

    /**
     *
     * @return List ({@link String}) of algorithm names on which the {@link JobDto} will run
     */
    public List<String> getAlgorithm() {

        return this.algorithm;
    }

    /**
     *
     * @param algorithm set List ({@link String}) of algorithm names on which the {@link JobDto} will run
     */
    public void setAlgorithm(List<String> algorithm) {

        this.algorithm = algorithm;
    }

    /**
     *
     * @return {@link Integer} number of repetition of the {@link JobDto} on every algorithm
     */
    public int getRepeat() {

        return this.repeat;
    }

    /**
     *
     * @param repeat set {@link Integer} number of repetition of the {@link JobDto} on every algorithm
     */
    public void setRepeat(int repeat) {

        this.repeat = repeat;
    }

    /**
     *
     * @return {@link Long} dataset ID
     */
    public Long getDataset() {

        return this.dataset;
    }

    /**
     *
     * @param dataset set {@link Long} dataset ID
     */
    public void setDataset(Long dataset) {

        this.dataset = dataset;
    }

    /**
     *
     * @param datasetName set {@link String} dataset name
     */
    public void setDatasetName(String datasetName) {

        this.datasetName = datasetName;
    }

    /**
     *
     * @return {@link String} dataset name
     */
    public String getDatasetName() {

        return this.datasetName;
    }

    /**
     *
     * @return {@link Integer} maximal number of rows which will be read in the {@link JobDto} execution
     */
    public int getLimitEntries() {

        return this.limitEntries;
    }

    /**
     *
     * @param limitEntries set {@link Integer} maximal number of rows which will be read in the {@link JobDto} execution
     */
    public void setLimitEntries(int limitEntries) {

        this.limitEntries = limitEntries;
    }

    /**
     *
     * @return {@link Integer} number of rows which will be skipped in the {@link JobDto} execution
     */
    public int getSkipEntries() {

        return this.skipEntries;
    }

    /**
     *
     * @param skipEntries set {@link Integer} number of rows which will be skipped in the {@link JobDto} execution
     */
    public void setSkipEntries(int skipEntries) {

        this.skipEntries = skipEntries;
    }

    /**
     *
     * @return {@link Integer} maximal number of attributes in the LHS pf found FDs
     */
    public int getMaxLHS() {

        return this.maxLHS;
    }

    /**
     *
     * @param maxLHS set {@link Integer} maximal number of attributes in the LHS pf found FDs
     */
    public void setMaxLHS(int maxLHS) {

        this.maxLHS = maxLHS;
    }

    /**
     *
     * @return List ({@link JobResult}) of {@link JobDto} results from iterations
     */
    public  List<JobResult> getJobResults() {

        return jobResults.stream().filter(res -> Objects.equals(res.getAlgorithm(), "fastfds")).toList();
    }

    /**
     *
     * @param jobResults set {@link JobResult} for each {@link JobDto} iteration
     */
    public void setJobResults(List<JobResult> jobResults) {

        this.jobResults = jobResults;
    }

    @Override
    public boolean equals(Object o) {

        if (this == o)
            return true;
        if (!(o instanceof JobDto))
            return false;
        JobDto job = (JobDto) o;
        return Objects.equals(this.id, job.id) &&
                Objects.equals(this.status, job.status) &&
                Objects.equals(this.algorithm, job.algorithm) &&
                Objects.equals(this.dataset, job.dataset) &&
                Objects.equals(this.limitEntries, job.limitEntries) &&
                Objects.equals(this.skipEntries, job.skipEntries) &&
                Objects.equals(this.maxLHS, job.maxLHS) &&
                Objects.equals(this.repeat, job.repeat);

    }

    @Override
    public int hashCode() {

        return Objects.hash(this.id, this.status, this.algorithm, this.dataset, this.limitEntries, this.skipEntries, this.maxLHS, this.repeat);
    }

    /**
     *
     * @return {@link String} form of the {@link JobDto} metadata
     */
    @Override
    public String toString() {
        return "Job{" + "id=" + this.id + ", status=" + this.status + ", algorithm=" + this.algorithm
                + ", dataset=" + this.dataset + ", MAX=" + this.limitEntries + ", SKIP="
                + this.skipEntries + ", MAX LHS=" + this.maxLHS
                + "repeat=" + this.repeat + '}';
    }

}
