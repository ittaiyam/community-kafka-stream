package com.cellwize.model;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Objects;

public class KPIDataPoint {
    private String kpiName;
    private String cellGuid;
    private long timestamp;
    private long value;

    @JsonCreator
    public KPIDataPoint(String kpiName, long timestamp, long value) {
        this.kpiName = kpiName;
        this.timestamp = timestamp;
        this.value = value;
    }

    public String getKpiName() {
        return kpiName;
    }

    public void setKpiName(String kpiName) {
        this.kpiName = kpiName;
    }

    public String getCellGuid() {
        return cellGuid;
    }

    public void setCellGuid(String cellGuid) {
        this.cellGuid = cellGuid;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(kpiName, cellGuid, timestamp, value);
    }

    @Override
    public boolean equals(Object obj) {
        boolean isEqual = false;
        if (obj instanceof KPIDataPoint) {
            final KPIDataPoint other = (KPIDataPoint) obj;
            isEqual = Objects.equals(other.kpiName, kpiName) &&
                    Objects.equals(other.cellGuid, cellGuid) &&
                    Objects.equals(other.timestamp, timestamp) &&
                    Objects.equals(other.value, value);
        }
        return isEqual;
    }
}
