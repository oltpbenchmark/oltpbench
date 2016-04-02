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
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import com.oltpbenchmark.LatencyRecord.Sample;
import com.oltpbenchmark.ThreadBench.TimeBucketIterable;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.util.Histogram;

public final class Results {
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
    
    public boolean valid() {
    	return getRequestsPerSecond() != 0.0;
    }
    
    public static final class ResultIterable implements Iterable<double[]> {
        
        public static final String LABELS[] = {"time_sec",
                "throughput_req_per_sec", "avg_lat", "min_lat",
                "25th_lat", "median_lat", "75th_lat", "90th_lat",
                "95th_lat", "99th_lat", "max_lat", "stdev_lat", 
                "throughput_req_per_sec_scaled"};
        
        public static final double SECONDS_FACTOR = 1e6;
        public static final double MILLISECONDS_FACTOR = 1e3;
        public static final double MICROSECONDS_FACTOR = 1.0;
        public static final int NO_WINDOW = 0;

        private final Results results;
        private final double windowSizeSeconds;
        private final BitSet mask;
        private final boolean globalStats;

        
        /**
         * The latency samples are given in microseconds. These samples are
         * converted to milliseconds by default, but conversionFactor can
         * optionally be set to convert to a different unit.
         */
        private final double conversionFactor;
        
        public ResultIterable(Results results, int windowSizeSeconds,
                double conversionFactor, String[] ignoreLabels) {
            assert(windowSizeSeconds >= 0);
            
            this.results = results;
            this.conversionFactor = conversionFactor;
            
            if (windowSizeSeconds == NO_WINDOW) {
                this.globalStats = true;
                this.windowSizeSeconds = results.nanoSeconds / 1e6;
            } else {
                this.globalStats = false;
                this.windowSizeSeconds = windowSizeSeconds;
            }

            
            this.mask = getMask();
            if (ignoreLabels.length > 0) {
                int numLabels = LABELS.length;
                for (int i = 0; i < numLabels; ++i) {
                    for (int j = 0; j < ignoreLabels.length; ++j) {
                        if (LABELS[i].equals(ignoreLabels[j])) {
                            this.mask.clear(i);
                            break;
                        }
                    }
                }
            }
        }
        
        public ResultIterable(Results results, int windowSizeSeconds,
                TransactionType txType) {
            this(results, windowSizeSeconds, MILLISECONDS_FACTOR,
                    new String[0]);
        }
        
        public ResultIterable(Results results, double conversionFormat,
                String[] ignoreLabels) {
            this(results, NO_WINDOW, conversionFormat, ignoreLabels);
        }
        
        public ResultIterable(Results results, double conversionFormat,
                int windowSize, String[] ignoreLabels) {
            this(results, windowSize, conversionFormat, ignoreLabels);
        }
        
        @Override
        public Iterator<double[]> iterator() {
            Iterator<DistributionStatistics> iter = null;
            if (this.globalStats) {
                iter = (new TimeBucketIterable(results.latencySamples,
                        (int) windowSizeSeconds, TransactionType.INVALID))
                        .iterator();
            } else {
            
                iter = new Iterator<DistributionStatistics>() {
                    
                            private final DistributionStatistics stats =
                                    ResultIterable.this.results.latencyDistribution;
                            
                            private boolean done = false;
    
                            @Override
                            public boolean hasNext() {
                                return !done;
                            }
    
                            @Override
                            public DistributionStatistics next() {
                                if (!hasNext()) {
                                    throw new NoSuchElementException();
                                }
                                done = true;
                                return stats;
                            }
    
                            @Override
                            public void remove() {
                                throw new UnsupportedOperationException("unsupported");
                            }
                    
                };
            }

            return new ResultIterator(iter, this.windowSizeSeconds,
                    this.conversionFactor, this.mask);
        }
        
        private String getUnitString() {
            String unit = null;
            if (this.conversionFactor == MICROSECONDS_FACTOR) {
                unit = "_us";
            } else if (this.conversionFactor == MILLISECONDS_FACTOR) {
                unit = "_ms";
            } else if (this.conversionFactor == SECONDS_FACTOR) {
                unit = "_sec";
            } else{
                unit = "";
            }
            return unit;
        }
        
        public String[] getResultLabels() {
            String unit = getUnitString();
            int numLabels = LABELS.length;
            
            if (unit.equals("") && mask.cardinality() == numLabels) {
                return Arrays.copyOf(LABELS, numLabels);
            }
            
            String[] result = new String[mask.cardinality()];
            int resIdx = 0;
            for (int i = 0; i < numLabels; ++i) {
                if (mask.get(i)) {
                    String next = LABELS[i];
                    if (next.endsWith("_lat")) {
                        next += unit;
                    }
                    result[resIdx++] = next;
                }
            }
            return result;
        }
        
        private static BitSet getMask() {
            // Returns a bitset with all bits set to true
            BitSet mask = new BitSet(LABELS.length);
            mask.set(0, LABELS.length, true);
            return mask;
        }
    }
    
    public static final class ResultIterator implements Iterator<double[]> {

        private Iterator<DistributionStatistics> statsIter;
        private final BitSet statMask;
        private double windowSizeSeconds;
        private int elapsedSeconds;
        private double conversionFactor;
        
        public ResultIterator(Iterator<DistributionStatistics> statsIter,
                double windowSizeSeconds, double conversionFactor,
                BitSet statMask) {
            assert(conversionFactor > 0);
            this.conversionFactor = conversionFactor;
            this.windowSizeSeconds = windowSizeSeconds;
            this.elapsedSeconds = 0;
            this.statsIter = statsIter;
            this.statMask = statMask;
        }

        @Override
        public boolean hasNext() {
            return statsIter.hasNext();
        }

        @Override
        public double[] next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            DistributionStatistics stat = statsIter.next();
            int numStats = statMask.cardinality();
            int totalStats = statMask.size();

            double[] fullResult = {
                    (double) elapsedSeconds,
                    (double) stat.getCount() / windowSizeSeconds,
                    stat.getAverage() / conversionFactor,
                    stat.getMinimum() / conversionFactor,
                    stat.get25thPercentile() / conversionFactor,
                    stat.getMedian() / conversionFactor,
                    stat.get75thPercentile() / conversionFactor,
                    stat.get90thPercentile() / conversionFactor,
                    stat.get95thPercentile() / conversionFactor,
                    stat.get99thPercentile() / conversionFactor,
                    stat.getMaximum() / conversionFactor,
                    stat.getStandardDeviation() / conversionFactor,
                    conversionFactor / stat.getAverage(),
            };
            assert(totalStats == fullResult.length);
            if (numStats == totalStats) {
                return fullResult;
            }

            int resIdx = 0;
            double[] nextResult = new double[numStats];
            for (int i = 0; i < totalStats; ++i) {
                if (statMask.get(i)) {
                    nextResult[resIdx++] = fullResult[i];
                }
            }
            
            elapsedSeconds += windowSizeSeconds;
            return nextResult;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("unsupported");
        }
    }

}