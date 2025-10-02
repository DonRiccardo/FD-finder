package cz.cuni.mff.fdfinder.fdepservice;



public class JobDto {

    private Long id;
    private String status;
    private String algorithm;
    private Long dataset;
    private int limitEntries;
    private int skipEntries;
    private int maxLHS;
    private String output;

    public JobDto() {}

    public JobDto(Long id, String algorithm, Long dataset, String status, int limitEntries, int skipEntries, int maxLHS, String output) {

        this.id = id;
        this.algorithm = algorithm;
        this.dataset = dataset;
        this.status = status;
        this.limitEntries = limitEntries;
        this.skipEntries = skipEntries;
        this.maxLHS = maxLHS;
        this.output = output;
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

    public String getAlgorithm() {

        return this.algorithm;
    }

    public void setAlgorithm(String algorithm) {

        this.algorithm = algorithm;
    }

    public Long getDataset() {

        return this.dataset;
    }

    public void setDataset(Long dataset) {

        this.dataset = dataset;
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

    public String getOutput() {

        return this.output;
    }

    public void setOutput(String output) {

        this.output = output;
    }
}
