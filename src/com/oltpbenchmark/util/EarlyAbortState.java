package com.oltpbenchmark.util;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;

public class EarlyAbortState {
    private static final Logger LOG = Logger.getLogger(EarlyAbortState.class);

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
    
    public void setAbort(boolean abortingEarly, boolean finished) {
        this.aborted = abortingEarly;
        if (finished) {
            this.endTimeNs = System.nanoTime();
        }
    }
    
    public double updateLatencyUs(double currentLatencyUs, boolean weightedSum) {

        if (weightedSum && latencyUs != 0.0) {
            latencyUs = (latencyUs + currentLatencyUs) / 2.0;
        } else {
            latencyUs += currentLatencyUs;
        }
        LOG.info("[EarlyAbort] current latency = " + currentLatencyUs / 1000 + 
                "ms, average latency = " + latencyUs / 1000 + "ms");
        return latencyUs;
    }
    
    public int updateLatencyCount(int currentCount) {
        this.latencyCount += currentCount;
        return this.latencyCount;
    }
    
    public long getStartTimeNs() {
        return this.startTimeNs;
    }
    
    public long getEndTimeNs() {
        return this.endTimeNs;
    }

    public Map<String, String> getSummary() {
        Map<String, String> m = new HashMap<String, String>();
        if (this.abortConfig != null) {
            m.putAll(this.abortConfig.getSummary());
            m.put("earlyAbortEnabled", "true");
        } else {
            m.put("earlyAbortEnabled", "false");
        }
        m.put("startTimeNs", Long.toString(startTimeNs));
        m.put("endTimeNs", Long.toString(endTimeNs));
        m.put("aborted", Boolean.toString(aborted));
        m.put("latencyCount", Integer.toString(latencyCount));
        m.put("latencyUs", Double.toString(latencyUs));
        return m;
    }
    
    @Override
    public String toString() {
        Class<?> confClass = this.getClass();
        Map<String, Object> m = new ListOrderedMap<String, Object>();
        for (Field f : confClass.getDeclaredFields()) {
            Object obj = null;
            try {
                obj = f.get(this);
            } catch (IllegalAccessException ex) {
                throw new RuntimeException(ex);
            }
            m.put(f.getName().toUpperCase(), obj);
        } // FOR
        return StringUtil.formatMaps(m);
    }
}
