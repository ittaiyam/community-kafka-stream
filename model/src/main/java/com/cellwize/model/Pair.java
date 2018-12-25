package com.cellwize.model;

import java.util.Objects;

public class Pair {
    private String guid;
    private long timestamp;

    public Pair() {
    }

    public String getGuid() {
        return guid;
    }

    public void setGuid(String guid) {
        this.guid = guid;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(guid, timestamp);
    }

    @Override
    public boolean equals(Object obj) {
        boolean isEqual = false;
        if (obj instanceof Pair) {
            final Pair other = (Pair) obj;
            isEqual = Objects.equals(other.guid, guid) && Objects.equals(other.timestamp, timestamp);
        }
        return isEqual;
    }

    @Override
    public String toString() {
        return "Pair{" +
                "guid='" + guid + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
