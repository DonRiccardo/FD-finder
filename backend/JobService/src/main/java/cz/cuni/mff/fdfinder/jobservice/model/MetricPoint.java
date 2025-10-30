package cz.cuni.mff.fdfinder.jobservice.model;

import jakarta.persistence.Embeddable;

/**
 * Class representing data points for two graphs.
 *
 * FIRTS point: ({@code timestamp}, {@code setCpuLoad}) for cpuLoad graph,
 * SECOND point: ({@code timestamp}, {@code usedMemory}) for usedMemory graph.
 */
@Embeddable
public class MetricPoint {

    private long timeStamp;
    private double cpuLoad;
    private long usedMemory;

    public MetricPoint(long timestamp, double cpuLoad, long usedMemory) {
        this.timeStamp = timestamp;
        this.cpuLoad = cpuLoad;
        this.usedMemory = usedMemory;
    }

    public MetricPoint() {

    }

    /**
     *
     * @return {@link Long} timestamp of points
     */
    public long getTimestamp() {

        return timeStamp;
    }

    /**
     *
     * @param timestamp set {@link Long} timestamp for points
     */
    public void setTimestamp(long timestamp) {

        this.timeStamp = timestamp;
    }

    /**
     *
     * @return {@link Double} CPU load for the point
     */
    public double getCpuLoad() {

        return cpuLoad;
    }

    /**
     *
     * @param cpuLoad set {@link Double} CPU load for the point
     */
    public void setCpuLoad(double cpuLoad) {

        this.cpuLoad = cpuLoad;
    }

    /**
     *
     * @return {@link Long} used memory for the point
     */
    public long getUsedMemory() {

        return usedMemory;
    }

    /**
     *
     * @param usedMemory set {@link Long} used memory for the point
     */
    public void setUsedMemory(long usedMemory) {

        this.usedMemory = usedMemory;
    }


}
