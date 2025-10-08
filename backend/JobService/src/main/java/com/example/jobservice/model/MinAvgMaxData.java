package com.example.jobservice.model;


public class MinAvgMaxData {

    private int minValue;
    private int averageValue;
    private int maxValue;

    public MinAvgMaxData() {

        this.minValue = 0;
        this.averageValue = 0;
        this.maxValue = 0;
    }

    public MinAvgMaxData(int minValue, int averageValue, int maxValue) {

        this.minValue = minValue;
        this.averageValue = averageValue;
        this.maxValue = maxValue;
    }

    public int getMinValue() {

        return minValue;
    }

    public void setMinValue(int minValue) {

        this.minValue = minValue;
    }

    public int getAverageValue() {

        return averageValue;
    }

    public void setAverageValue(int averageValue) {

        this.averageValue = averageValue;
    }

    public int getMaxValue() {

        return maxValue;
    }

    public void setMaxValue(int maxValue) {

        this.maxValue = maxValue;
    }

}
