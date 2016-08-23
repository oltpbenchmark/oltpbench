package com.oltpbenchmark.util;

import java.lang.reflect.Field;
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
    
    private final XMLConfiguration xmlConfig;
    private final int intervalSeconds;
    private final int abortThresholdPercentage;
    private final LatencyType latencyMetric;
    private final long latencyValueUs;
    private final String estimator;
    private final List<Long> responseTimesUs; 

    public EarlyAbortConfiguration(XMLConfiguration xmlConfig) {
        this.xmlConfig = xmlConfig;
        this.intervalSeconds = xmlConfig.getInt(
                "intervalSeconds", DEFAULT_INTERVAL_SECONDS);
        this.abortThresholdPercentage = xmlConfig.getInt(
                "abortThresholdPerc", DEFAULT_ABORT_THRESHOLD_PERC);
        this.estimator = xmlConfig.getString(
                "abortEstimator", DEFAULT_ESTIMATOR);
        this.latencyValueUs = xmlConfig.getLong("abortLatencyValueUs");
        
        this.latencyMetric = LatencyType.valueOf(xmlConfig.getString(
                "abortLatencyMetric", DEFAULT_LATENCY_METRIC));
        if (this.latencyMetric == LatencyType.TOTAL_RESPONSE_TIME) {
            this.responseTimesUs = xmlConfig.getList("/responseTimesUs/responseTimeUs");            
            LOG.info("response times: " + responseTimesUs);
        } else {
            this.responseTimesUs = null;
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
