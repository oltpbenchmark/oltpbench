package com.oltpbenchmark.util;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.log4j.Logger;

public class EarlyAbortConfiguration {
    private static final Logger LOG = Logger.getLogger(EarlyAbortConfiguration.class);
    
    public enum LatencyType {
        TOTAL_RESPONSE_TIME,
        MAXIMUM,
        PERCENTILE_99TH,
        PERCENTILE_95TH,
        PERCENTILE_90TH,
        PERCENTILE_75TH,
        MEDIAN,
        PERCENTILE_25TH,
        MEAN,
        MINIMUM
    }
    
    // Configuration Defaults
    private static final int DEFAULT_INTERVAL_SECONDS = 10;
    private static final int DEFAULT_ABORT_THRESHOLD_PERC = 20;
    private static final String DEFAULT_LATENCY_METRIC = "PERCENTILE_99TH";
    private static final String DEFAULT_ESTIMATOR = "EWA";
    private static final int DEFAULT_WAIT_TRANSACTIONS = 0;
    private static final int DEFAULT_WAIT_TIME_SECONDS = 0;
    private static final boolean DEFAULT_DRY_RUN = false;
    
    private final XMLConfiguration xmlConfig;
    private final int intervalSeconds;
    private final int abortThresholdPercentage;
    private final LatencyType latencyMetric;
    private final long latencyValueUs;
    private final String estimator;
    private final List<Long> responseTimesUs;
    private final int waitTransactions;
    private final int waitTimeSeconds;
    private final boolean dryRun;

    public EarlyAbortConfiguration(XMLConfiguration xmlConfig) {
        this.xmlConfig = xmlConfig;
        this.intervalSeconds = xmlConfig.getInt(
                "intervalSeconds", DEFAULT_INTERVAL_SECONDS);
        this.abortThresholdPercentage = xmlConfig.getInt(
                "abortThresholdPerc", DEFAULT_ABORT_THRESHOLD_PERC);
        this.estimator = xmlConfig.getString(
                "abortEstimator", DEFAULT_ESTIMATOR);

        this.latencyValueUs = xmlConfig.getLong("abortLatencyValueUs", 0L);
        this.latencyMetric = LatencyType.valueOf(xmlConfig.getString(
                "abortLatencyMetric", DEFAULT_LATENCY_METRIC));
        this.waitTransactions = xmlConfig.getInt(
                "waitTransactions", DEFAULT_WAIT_TRANSACTIONS);
        this.waitTimeSeconds = xmlConfig.getInt(
                "waitTimeSeconds", DEFAULT_WAIT_TIME_SECONDS);
        this.dryRun = xmlConfig.getBoolean("dryRun", DEFAULT_DRY_RUN);
        responseTimesUs = new ArrayList<Long>();
        if (this.latencyMetric == LatencyType.TOTAL_RESPONSE_TIME) {
            List<String> rts = xmlConfig.getList("/responseTimesUs/responseTimeUs");
            for (String rt : rts) {
                responseTimesUs.add(Long.parseLong(rt));
            }
        }
        LOG.info(this.toString());
    }
    
    public XMLConfiguration getXMLConfig() {
        return xmlConfig;
    }
    
    public int getIntervalSeconds() {
        return intervalSeconds;
    }
    
    public int getAbortThresholdPercentage() {
        return abortThresholdPercentage;
    }
    
    public LatencyType getLatencyMetric() {
        return latencyMetric;
    }
    
    public long getLatencyValueUs() {
        return latencyValueUs;
    }
    
    public String getEstimator() {
        return estimator;
    }
    
    public int getWaitTransactions() {
        return waitTransactions;
    }
    
    public int getWaitTimeSeconds() {
        return waitTimeSeconds;
    }
    
    public boolean isDryRun() {
        return dryRun;
    }
    
    public double getLatencyThreshold() {
        return (this.abortThresholdPercentage / 100.0 + 1.0)
                * this.latencyValueUs;
    }
    
    public double getLatencyThreshold(int responseTimesElapsed) {
        assert (responseTimesElapsed >= 0);
        assert (responseTimesElapsed <= responseTimesUs.size());
        long totalResponseTime = 0L;
        for (int i = 0; i < responseTimesElapsed; ++i) {
            totalResponseTime += responseTimesUs.get(i);
        }
        return (this.abortThresholdPercentage / 100.0 + 1.0)
                * totalResponseTime;
    }
    
    public Map<String, String> getSummary() {
        Map<String, String> m = new HashMap<String, String>();
        m.put("intervalSeconds", Integer.toString(this.intervalSeconds));
        m.put("abortThresholdPercentage", Integer.toString(this.abortThresholdPercentage));
        m.put("latencyMetric", this.latencyMetric.toString());
        m.put("latencyValueUs", Long.toString(this.latencyValueUs));
        m.put("estimator", this.estimator);
        m.put("responseTimeUs", this.responseTimesUs.toString());
        m.put("waitTransactions", Integer.toString(this.waitTransactions));
        m.put("waitTimeSeconds", Integer.toString(this.waitTimeSeconds));
        m.put("dryRun", Boolean.toString(this.dryRun));
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
