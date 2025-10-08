package com.example.jobservice.model;

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

    public double getCpuMin() {

        return cpuMin;
    }

    public void setCpuMin(double cpuMin) {

        this.cpuMin = cpuMin;
    }

    public double getCpuMax() {

        return cpuMax;
    }

    public void setCpuMax(double cpuMax) {

        this.cpuMax = cpuMax;
    }

    public double getCpuAvg() {

        return cpuAvg;
    }

    public void setCpuAvg(double cpuAvg) {

        this.cpuAvg = cpuAvg;
    }

    public long getMemoryMin() {

        return memoryMin;
    }

    public void setMemoryMin(long memoryMin) {

        this.memoryMin = memoryMin;
    }

    public long getMemoryMax() {

        return memoryMax;
    }

    public void setMemoryMax(long memoryMax) {

        this.memoryMax = memoryMax;
    }

    public double getMemoryAvg() {

        return memoryAvg;
    }

    public void setMemoryAvg(double memoryAvg) {

        this.memoryAvg = memoryAvg;
    }

    public long getTimeMin() {

        return timeMin;
    }

    public void setTimeMin(long timeMin) {

        this.timeMin = timeMin;
    }

    public long getTimeMax() {

        return timeMax;
    }

    public void setTimeMax(long timeMax) {

        this.timeMax = timeMax;
    }

    public double getTimeAvg() {

        return timeAvg;
    }

    public void setTimeAvg(double timeAvg) {

        this.timeAvg = timeAvg;
    }

    public int getFdsMin() {

        return fdsMin;
    }

    public void setFdsMin(int fdsMin) {

        this.fdsMin = fdsMin;
    }

    public int getFdsMax() {

        return fdsMax;
    }

    public void setFdsMax(int fdsMax) {

        this.fdsMax = fdsMax;
    }

    public int getFdsAvg() {

        return fdsAvg;
    }

    public void setFdsAvg(int fdsAvg) {

        this.fdsAvg = fdsAvg;
    }


}
