package com.cellwize.model;

public class MeasResults {
    private String counterName;
    private String cellGuid;
    private long timestamp;
    private long value;

    public String getCounterName() {
        return counterName;
    }

    public void setCounterName(String counterName) {
        this.counterName = counterName;
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
}
