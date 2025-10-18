package cz.cuni.mff.fdfinder.depminerservice.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class JobDto {

    private Long id;
    private String status;
    private List<String> algorithm;
    private int repeat;
    private Long dataset;
    private String datasetName;
    private int limitEntries;
    private int skipEntries;
    private int maxLHS;
    private List<JobResult> jobResults = new ArrayList<>();


    public JobDto() {}

    public JobDto(Long id, List<String> algorithm, int repeat, Long dataset, String datasetName, String status, int limitEntries, int skipEntries, int maxLHS, String output) {

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

    public Long getId() {

        return this.id;
    }

    public void setId(Long id) {

        this.id = id;
    }

    public String getStatus() {

        return this.status;
    }

    public void setStatus(String status) {

        this.status = status;
    }

    public List<String> getAlgorithm() {

        return this.algorithm;
    }

    public void setAlgorithm(List<String> algorithm) {

        this.algorithm = algorithm;
    }

    public int getRepeat() {

        return this.repeat;
    }

    public void setRepeat(int repeat) {

        this.repeat = repeat;
    }

    public Long getDataset() {

        return this.dataset;
    }

    public void setDataset(Long dataset) {

        this.dataset = dataset;
    }

    public String getDatasetName() {

        return this.datasetName;
    }

    public void setDatasetName(String datasetName) {

        this.datasetName = datasetName;
    }

    public int getLimitEntries() {

        return this.limitEntries;
    }

    public void setLimitEntries(int limitEntries) {

        this.limitEntries = limitEntries;
    }

    public int getSkipEntries() {

        return this.skipEntries;
    }

    public void setSkipEntries(int skipEntries) {

        this.skipEntries = skipEntries;
    }

    public int getMaxLHS() {

        return this.maxLHS;
    }

    public void setMaxLHS(int maxLHS) {

        this.maxLHS = maxLHS;
    }

    public  List<JobResult> getJobResults() {

        return jobResults.stream().filter(res -> Objects.equals(res.getAlgorithm(), "depminer")).toList();
    }

    public void setJobResults(List<JobResult> jobResults) {

        this.jobResults = jobResults;
    }
}
