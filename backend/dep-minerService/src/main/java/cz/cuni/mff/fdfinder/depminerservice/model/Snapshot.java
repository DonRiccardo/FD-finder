package cz.cuni.mff.fdfinder.depminerservice.model;

/**
 * Class representing snapshot of JVM at {@code timestamp} with {@code usedMemory}, {@code cpuLoad} and {@code threadCount}.
 */
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

    /**
     *
     * @return {@link Long} timestamp when snapshot was taken
     */
    public Long getTimestamp() {

        return timestamp;
    }

    /**
     *
     * @param timestamp {@link Long} when snapshot was taken
     */
    public void setTimestamp(Long timestamp) {

        this.timestamp = timestamp;
    }

    /**
     *
     * @return {@link Long} used memory when snapshot was taken, in Bytes
     */
    public Long getUsedMemory() {

        return usedMemory;
    }

    /**
     *
     * @param usedMemory {@link Long} used memory when snapshot was taken, in Bytes
     */
    public void setUsedMemory(Long usedMemory) {

        this.usedMemory = usedMemory;
    }

    /**
     *
     * @return {@link Double} CPU load when snapshot was taken, number in range [0,1]
     */
    public Double getCpuLoad() {

        return cpuLoad;
    }

    /**
     *
     * @param cpuLoad {@link Double} CPU load when snapshot was taken, number in range [0,1]
     */
    public void setCpuLoad(Double cpuLoad) {

        this.cpuLoad = cpuLoad;
    }

    /**
     *
     * @return {@link Integer} number of threads active when snapshot was taken
     */
    public Integer getThreadCount() {

        return threadCount;
    }

    /**
     *
     * @param threadCount {@link Integer} number of threads active when snapshot was taken
     */
    public void setThreadCount(Integer threadCount) {

        this.threadCount = threadCount;
    }

    /**
     *
     * @return {@link Double} used memory when snapshot was taken, in MegaBytes
     */
    public double getUsedMemoryMb() {

        return usedMemory / (1024.0 * 1024.0);
    }

    /**
     * Returns {@code getCpuLoad()} * 100
     * @return {@link Double} CPU usage when snapshot was taken, in percentage
     */
    public double getCpuPercent() {

        return cpuLoad * 100.0;
    }

    /**
     *
     * @return {@link String} representation of {@link Snapshot} metadata
     */
    @Override
    public String toString() {

        return "SNAPSHOT: timestamp=" + timestamp + ", usedMemory=" + usedMemory + ", cpuLoad=" + cpuLoad;
    }
}
