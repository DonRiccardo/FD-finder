package cz.cuni.mff.fdfinder.fastfdsservice.model;

public class Snapshot {
    private Long timestamp;
    private Long usedMemory;
    private Double cpuLoad;
    private Integer threadCount;

    public Snapshot(Long timestamp, Long usedMemory, Double cpuLoad, Integer threadCount) {
        this.timestamp = timestamp;
        this.usedMemory = usedMemory;
        this.cpuLoad = cpuLoad;
        this.threadCount = threadCount;
    }

    public Long getTimestamp() {

        return timestamp;
    }

    public void setTimestamp(Long timestamp) {

        this.timestamp = timestamp;
    }

    public Long getUsedMemory() {

        return usedMemory;
    }

    public void setUsedMemory(Long usedMemory) {

        this.usedMemory = usedMemory;
    }

    public Double getCpuLoad() {

        return cpuLoad;
    }

    public void setCpuLoad(Double cpuLoad) {

        this.cpuLoad = cpuLoad;
    }

    public Integer getThreadCount() {

        return threadCount;
    }

    public void setThreadCount(Integer threadCount) {

        this.threadCount = threadCount;
    }

    public double getUsedMemoryMb() {

        return usedMemory / (1024.0 * 1024.0);
    }

    public double getCpuPercent() {

        return cpuLoad * 100.0;
    }

    @Override
    public String toString() {

        return "SNAPSHOT: timestamp=" + timestamp + ", usedMemory=" + usedMemory + ", cpuLoad=" + cpuLoad;
    }
}
