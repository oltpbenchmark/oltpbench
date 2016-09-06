package com.oltpbenchmark.util;

public class EarlyAbortState {

    private final EarlyAbortConfiguration abortConfig;
    private final long startTimeNs;

    private boolean aborted;
    private int latencyCount;
    private double latencyUs;
    private long endTimeNs;
    
    public EarlyAbortState(EarlyAbortConfiguration abortConfig) {
        this.abortConfig = abortConfig;
        this.aborted = false;
        this.startTimeNs = System.nanoTime();
        this.latencyCount = 0;
        this.latencyUs = 0.0;
    }
    
    public boolean earlyAbortEnabled() {
        return this.abortConfig != null;
    }
    
    public EarlyAbortConfiguration getAbortConfig() {
        return this.abortConfig;
    }
    
    public boolean isAborted() {
        return this.aborted;
    }
    
    public void setAbort(boolean abortingEarly) {
        this.aborted = abortingEarly;
        this.endTimeNs = System.nanoTime();
    }
    
    public double updateLatencyUs(double currentLatencyUs, boolean weightedSum) {
        if (weightedSum && latencyUs != 0.0) {
            latencyUs = (latencyUs + currentLatencyUs) / 2.0;
        } else {
            latencyUs += currentLatencyUs;
        }
        return latencyUs;
    }
    
    public int updateLatencyCount(int currentCount) {
        this.latencyCount += currentCount;
        return this.latencyCount;
    }
    
    public long getStartTimeNs() {
        return this.startTimeNs;
    }
}
