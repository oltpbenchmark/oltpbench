/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/


package com.oltpbenchmark;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.oltpbenchmark.LatencyRecord.Sample;
import com.oltpbenchmark.ThreadBench.TimeBucketIterable;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.util.Histogram;
import com.oltpbenchmark.util.TimeUtil.TimeUnit;

public final class Results {
    
    public enum ResultType {
        TIME ("measured time", "time"),
        REQUESTS ("measured requests", "requests", 1),
        THROUGHPUT ("throughput", "throughput", 2),
        AVG_LATENCY ("average latency","avg_lat"),
        MIN_LATENCY ("minimum latency", "min_lat"),
        P25_LATENCY ("25th percentile latency", "25th_lat"),
        MED_LATENCY ("median latency", "med_lat"),
        P75_LATENCY ("75th percentile latency", "75th_lat"),
        P90_LATENCY ("90th percentile latency", "90th_lat"),
        P95_LATENCY ("95th percentile latency", "95th_lat"),
        P99_LATENCY ("99th percentile latency", "99th_lat"),
        MAX_LATENCY ("maximum latency", "max_lat"),
        STDEV_LATENCY ("latency standard deviation", "stdev_lat");

        
        public static final int UNIT_TIME = 0;
        public static final int UNIT_REQUESTS = 1;
        public static final int UNIT_THROUGHPUT = 2;
        
        private final String fullName;
        private final String alias;
        private final int unitType;
        
        private ResultType(String fullName, String alias, int unitType) {
            this.fullName = fullName;
            this.alias = alias;
            this.unitType = unitType;
        }
        
        private ResultType(String fullName, String alias) {
            this(fullName, alias, UNIT_TIME);
        }
        
        @Override
        public String toString() {
            return alias;
        }
        
        public String toString(TimeUnit unit) {
            return String.format(getFormatString(), unit);
        }
        
        public String getFullName() {
            return fullName;
        }
        
        public String getFormatString() {
            switch(unitType) {
                case UNIT_THROUGHPUT:
                    return alias + "_req_per_%s";
                case UNIT_REQUESTS:
                    return alias;
                default:
                    return alias + "_%s";
            }
        }
    }

    public final long nanoSeconds;
    public final int measuredRequests;
    public final DistributionStatistics latencyDistribution;
    final Histogram<TransactionType> txnSuccess = new Histogram<TransactionType>(true);
    final Histogram<TransactionType> txnAbort = new Histogram<TransactionType>(true);
    final Histogram<TransactionType> txnRetry = new Histogram<TransactionType>(true);
    final Histogram<TransactionType> txnErrors = new Histogram<TransactionType>(true);
    final Map<TransactionType, Histogram<String>> txnAbortMessages = new HashMap<TransactionType, Histogram<String>>();
    
    public final List<LatencyRecord.Sample> latencySamples;

    public Results(long nanoSeconds, int measuredRequests, DistributionStatistics latencyDistribution, final List<LatencyRecord.Sample> latencySamples) {
        this.nanoSeconds = nanoSeconds;
        this.measuredRequests = measuredRequests;
        this.latencyDistribution = latencyDistribution;

        if (latencyDistribution == null) {
            assert latencySamples == null;
            this.latencySamples = null;
        } else {
            // defensive copy
            this.latencySamples = Collections.unmodifiableList(new ArrayList<LatencyRecord.Sample>(latencySamples));
            assert !this.latencySamples.isEmpty();
        }
    }

    /**
     * Get a histogram of how often each transaction was executed
     */
    public final Histogram<TransactionType> getTransactionSuccessHistogram() {
        return (this.txnSuccess);
    }
    public final Histogram<TransactionType> getTransactionRetryHistogram() {
        return (this.txnRetry);
    }
    public final Histogram<TransactionType> getTransactionAbortHistogram() {
        return (this.txnAbort);
    }
    public final Histogram<TransactionType> getTransactionErrorHistogram() {
        return (this.txnErrors);
    }
    public final Map<TransactionType, Histogram<String>> getTransactionAbortMessageHistogram() {
        return (this.txnAbortMessages);
    }

    public double getRequestsPerSecond() {
        return (double) measuredRequests / (double) nanoSeconds * 1e9;
    }

    @Override
    public String toString() {
        return "Results(nanoSeconds=" + nanoSeconds + ", measuredRequests=" + measuredRequests + ") = " + getRequestsPerSecond() + " requests/sec";
    }

    public void writeCSV(int windowSizeSeconds, PrintStream out) {
        writeCSV(windowSizeSeconds, out, TransactionType.INVALID);
    }
    
    public void writeCSV(int windowSizeSeconds, PrintStream out, TransactionType txType) {
        out.println("time(sec), throughput(req/sec), avg_lat(ms), min_lat(ms), 25th_lat(ms), median_lat(ms), 75th_lat(ms), 90th_lat(ms), 95th_lat(ms), 99th_lat(ms), max_lat(ms), tp (req/s) scaled");
        int i = 0;
        for (DistributionStatistics s : new TimeBucketIterable(latencySamples, windowSizeSeconds, txType)) {
            final double MILLISECONDS_FACTOR = 1e3;
            out.printf("%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f\n", i * windowSizeSeconds, (double) s.getCount() / windowSizeSeconds, s.getAverage() / MILLISECONDS_FACTOR,
                    s.getMinimum() / MILLISECONDS_FACTOR, s.get25thPercentile() / MILLISECONDS_FACTOR, s.getMedian() / MILLISECONDS_FACTOR, s.get75thPercentile() / MILLISECONDS_FACTOR,
                    s.get90thPercentile() / MILLISECONDS_FACTOR, s.get95thPercentile() / MILLISECONDS_FACTOR, s.get99thPercentile() / MILLISECONDS_FACTOR, s.getMaximum() / MILLISECONDS_FACTOR,
                    MILLISECONDS_FACTOR / s.getAverage());
            i += 1;
        }
    }


    public void writeAllCSV(PrintStream out) {
        long startNs = latencySamples.get(0).startNs;
        out.println("transaction type (index in config file), start time (microseconds),latency (microseconds)");
        for (Sample s : latencySamples) {
            long startUs = (s.startNs - startNs + 500) / 1000;
            out.println(s.tranType + "," + startUs + "," + s.latencyUs);
        }
    }

    public void writeAllCSVAbsoluteTiming(PrintStream out) {

        // This is needed because nanTime does not guarantee offset... we
        // ground it (and round it) to ms from 1970-01-01 like currentTime
        double x = ((double) System.nanoTime() / (double) 1000000000);
        double y = ((double) System.currentTimeMillis() / (double) 1000);
        double offset = x - y;

        // long startNs = latencySamples.get(0).startNs;
        out.println("transaction type (index in config file), start time (microseconds),latency (microseconds),worker id(start number), phase id(index in config file)");
        for (Sample s : latencySamples) {
            double startUs = ((double) s.startNs / (double) 1000000000);
            out.println(s.tranType + "," + String.format("%10.6f", startUs - offset) + "," + s.latencyUs + "," + s.workerId + "," + s.phaseId);
        }
    }
    
    public List<List<Double>> getAllAbsoluteTiming() {

        // This is needed because nanTime does not guarantee offset... we
        // ground it (and round it) to ms from 1970-01-01 like currentTime
        double x = ((double) System.nanoTime() / (double) 1000000000);
        double y = ((double) System.currentTimeMillis() / (double) 1000);
        double offset = x - y;

        //out.println("transaction type (index in config file), start time (microseconds),latency (microseconds),worker id(start number), phase id(index in config file)");
        List<List<Double>> timings = new ArrayList<List<Double>>();
        for (Sample s : latencySamples) {
            List<Double> timing = new ArrayList<Double>();
            double startUs = ((double) s.startNs / (double) 1000000000);
            //out.println(s.tranType + "," + String.format("%10.6f", startUs - offset) + "," + s.latencyUs + "," + s.workerId + "," + s.phaseId);
            timing.add((double) s.tranType);
            timing.add((startUs - offset) / (double) 1000);
            timing.add(s.latencyUs / (double) 1000);
            timing.add((double) s.workerId);
            timing.add((double) s.phaseId);
            timings.add(timing);
        }
        return timings;
    }
    
    public boolean valid() {
    	return getRequestsPerSecond() != 0.0;
    }
    
    public List<Double> getSummaryResults(PrintStream out) {
        double time = (double) nanoSeconds / 1e9;
        return getResults(out, this.latencyDistribution,
                time, this.measuredRequests,
                this.measuredRequests / time);
    }
    
    public List<List<Double>> getSampleResults(int windowSizeSeconds,
            PrintStream out) {
        return getSampleResults(windowSizeSeconds, out,
                TransactionType.INVALID);
    }
    
    public List<List<Double>> getSampleResults(int windowSizeSeconds,
            PrintStream out, TransactionType txType) {
        List<List<Double>> results = new ArrayList<List<Double>>();
        
        int secondsElapsed = 0;
        for (DistributionStatistics s :new TimeBucketIterable(latencySamples,
                windowSizeSeconds, txType)) {
            results.add(getResults(out, s, secondsElapsed, s.getCount(),
                    s.getCount() / windowSizeSeconds));
            secondsElapsed += windowSizeSeconds;
        }
        return results;
    }
    
    public static List<String> getResultLabels() {
        ResultType[] values = ResultType.values();
        int numValues = values.length;
        List<String> results = new ArrayList<String>(numValues);
        
        for (int i = 0; i < numValues; ++i) {
            ResultType type = values[i];
            switch(type) {
                case TIME:
                case THROUGHPUT:
                    results.add(type.toString(TimeUnit.SECONDS));
                    break;
                case STDEV_LATENCY:
                    break;
                default:
                    results.add(type.toString(TimeUnit.MILLISECONDS));
            }
        }
        return results;
    }

    public static List<Double> getResults(PrintStream out,
            DistributionStatistics stats, double timeSec, int requests,
            double throughput) {
        ResultType[] values = ResultType.values();
        int numValues = values.length;
        double conversionFactor = 1e3;
        
        List<Double> results = new ArrayList<Double>(numValues);
        for (int i = 0; i < numValues; ++i) {
            ResultType resType = values[i];
            switch(resType) {
                case TIME:
                    results.add((double) timeSec);
                    break;
                case REQUESTS:
                    results.add((double) requests);
                    break;
                case THROUGHPUT:
                    results.add(throughput);
                    break;
                case AVG_LATENCY:
                    results.add(stats.getAverage() / conversionFactor);
                    break;
                case MIN_LATENCY:
                    results.add(stats.getMinimum() / conversionFactor);
                    break;
                case P25_LATENCY:
                    results.add(stats.get25thPercentile() / conversionFactor);
                    break;
                case MED_LATENCY:
                    results.add(stats.getMedian() / conversionFactor);
                    break;
                case P75_LATENCY:
                    results.add(stats.get75thPercentile() / conversionFactor);
                    break;
                case P90_LATENCY:
                    results.add(stats.get90thPercentile() / conversionFactor);
                    break;
                case P95_LATENCY:
                    results.add(stats.get95thPercentile() / conversionFactor);
                    break;
                case P99_LATENCY:
                    results.add(stats.get99thPercentile() / conversionFactor);
                    break;
                case MAX_LATENCY:
                    results.add(stats.getMaximum() / conversionFactor);
                    break;
//                case STDEV_LATENCY:
//                    results.add(stats.getStandardDeviation() / conversionFactor);
//                    break;
                default:
                    /* Nothing */
            }
        }
        return results;
    }

}