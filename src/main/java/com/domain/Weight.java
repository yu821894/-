package com.domain;

public class Weight {

    private double weight;
    private String doubleARate;
    private int packageCount;
    private int allSilkCount;

    public String getDoubleARate() {
        return doubleARate;
    }

    public void setDoubleARate(String doubleARate) {
        this.doubleARate = doubleARate;
    }

    public double getWeight() {
        return weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

    public int getPackageCount() {
        return packageCount;
    }

    public void setPackageCount(int packageCount) {
        this.packageCount = packageCount;
    }

    public int getAllSilkCount() {
        return allSilkCount;
    }

    public void setAllSilkCount(int allSilkCount) {
        this.allSilkCount = allSilkCount;
    }

    @Override
    public String toString() {
        return "Weight{" +
                "weight=" + weight +
                ", doubleARate='" + doubleARate + '\'' +
                ", packageCount=" + packageCount +
                ", allSilkCount=" + allSilkCount +
                '}';
    }
}
