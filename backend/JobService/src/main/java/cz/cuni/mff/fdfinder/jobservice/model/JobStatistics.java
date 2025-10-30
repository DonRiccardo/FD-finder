package cz.cuni.mff.fdfinder.jobservice.model;

/**
 * Class representing statistics of DONE {@link Job} for ONE ALGORITHM.
 * Contains Min, Avg and Max values for CPU load, memory usage, duration of {@link Job} and number of found FDs.
 */
public class JobStatistics {

    private double cpuMin;
    private double cpuMax;
    private double cpuAvg;

    private long memoryMin;
    private long memoryMax;
    private double memoryAvg;

    private long timeMin;
    private long timeMax;
    private double timeAvg;

    private int fdsMin;
    private int fdsMax;
    private int fdsAvg;


    public JobStatistics() {

        this.cpuMin = 0.0;
        this.cpuMax = 0.0;
        this.cpuAvg = 0.0;
        this.memoryMin = 0L;
        this.memoryMax = 0L;
        this.memoryAvg = 0.0;
        this.timeMin = 0L;
        this.timeMax = 0L;
        this.timeAvg = 0.0;
        this.fdsMin = 0;
        this.fdsMax = 0;
        this.fdsAvg = 0;
    }

    /**
     *
     * @return {@link Double} minimal CPU load
     */
    public double getCpuMin() {

        return cpuMin;
    }

    /**
     *
     * @param cpuMin set {@link Double} minimal CPU load
     */
    public void setCpuMin(double cpuMin) {

        this.cpuMin = cpuMin;
    }

    /**
     *
     * @return {@link Double} maximal CPU load
     */
    public double getCpuMax() {

        return cpuMax;
    }

    /**
     *
     * @param cpuMax set {@link Double} maximal CPU load
     */
    public void setCpuMax(double cpuMax) {

        this.cpuMax = cpuMax;
    }

    /**
     *
     * @return {@link Double} average CPU load
     */
    public double getCpuAvg() {

        return cpuAvg;
    }

    /**
     *
     * @param cpuAvg set {@link Double} average CPU load
     */
    public void setCpuAvg(double cpuAvg) {

        this.cpuAvg = cpuAvg;
    }

    /**
     *
     * @return {@link Long} minimal memory used
     */
    public long getMemoryMin() {

        return memoryMin;
    }

    /**
     *
     * @param memoryMin set {@link Long} minimal memory used
     */
    public void setMemoryMin(long memoryMin) {

        this.memoryMin = memoryMin;
    }

    /**
     *
     * @return {@link Long} maximal memory used
     */
    public long getMemoryMax() {

        return memoryMax;
    }

    /**
     *
     * @param memoryMax set {@link Long} maximal memory used
     */
    public void setMemoryMax(long memoryMax) {

        this.memoryMax = memoryMax;
    }

    /**
     *
     * @return {@link Double} average memory used
     */
    public double getMemoryAvg() {

        return memoryAvg;
    }

    /**
     *
     * @param memoryAvg set {@link Double} average memory used
     */
    public void setMemoryAvg(double memoryAvg) {

        this.memoryAvg = memoryAvg;
    }

    /**
     *
     * @return {@link Long} minimal duration of {@link Job}
     */
    public long getTimeMin() {

        return timeMin;
    }

    /**
     *
     * @param timeMin set {@link Long} minimal duration of {@link Job}
     */
    public void setTimeMin(long timeMin) {

        this.timeMin = timeMin;
    }

    /**
     *
     * @return {@link Long} maximal duration of the {@link Job}
     */
    public long getTimeMax() {

        return timeMax;
    }

    /**
     *
     * @param timeMax set {@link Long} maximal duration of the {@link Job}
     */
    public void setTimeMax(long timeMax) {

        this.timeMax = timeMax;
    }

    /**
     *
     * @return {@link Double} average duration of the {@link Job}
     */
    public double getTimeAvg() {

        return timeAvg;
    }

    /**
     *
     * @param timeAvg set {@link Double} average duration of the {@link Job}
     */
    public void setTimeAvg(double timeAvg) {

        this.timeAvg = timeAvg;
    }

    /**
     *
     * @return {@link Integer} minimal number of found FDs
     */
    public int getFdsMin() {

        return fdsMin;
    }

    /**
     *
     * @param fdsMin set {@link Integer} minimal number of found FDs
     */
    public void setFdsMin(int fdsMin) {

        this.fdsMin = fdsMin;
    }

    /**
     *
     * @return {@link Integer} maximal number of found FDs
     */
    public int getFdsMax() {

        return fdsMax;
    }

    /**
     *
     * @param fdsMax set {@link Integer} maximal number of found FDs
     */
    public void setFdsMax(int fdsMax) {

        this.fdsMax = fdsMax;
    }

    /**
     *
     * @return {@link Integer} average number of found FDs
     */
    public int getFdsAvg() {

        return fdsAvg;
    }

    /**
     *
     * @param fdsAvg set {@link Integer} average number of found FDs
     */
    public void setFdsAvg(int fdsAvg) {

        this.fdsAvg = fdsAvg;
    }


}
