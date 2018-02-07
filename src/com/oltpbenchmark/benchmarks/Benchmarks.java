package com.oltpbenchmark.benchmarks;

import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkBenchmark;
import com.oltpbenchmark.benchmarks.chbenchmark.CHBenCHmark;
import com.oltpbenchmark.benchmarks.epinions.EpinionsBenchmark;
import com.oltpbenchmark.benchmarks.hyadapt.HYADAPTBenchmark;
import com.oltpbenchmark.benchmarks.jpab.JPABBenchmark;
import com.oltpbenchmark.benchmarks.linkbench.LinkBenchBenchmark;
import com.oltpbenchmark.benchmarks.noop.NoOpBenchmark;
import com.oltpbenchmark.benchmarks.resourcestresser.ResourceStresserBenchmark;
import com.oltpbenchmark.benchmarks.seats.SEATSBenchmark;
import com.oltpbenchmark.benchmarks.sibench.SIBenchmark;
import com.oltpbenchmark.benchmarks.smallbank.SmallBankBenchmark;
import com.oltpbenchmark.benchmarks.tatp.TATPBenchmark;
import com.oltpbenchmark.benchmarks.tpcc.TPCCBenchmark;
import com.oltpbenchmark.benchmarks.tpcds.TPCDSBenchmark;
import com.oltpbenchmark.benchmarks.tpch.TPCHBenchmark;
import com.oltpbenchmark.benchmarks.twitter.TwitterBenchmark;
import com.oltpbenchmark.benchmarks.voter.VoterBenchmark;
import com.oltpbenchmark.benchmarks.wikipedia.WikipediaBenchmark;
import com.oltpbenchmark.benchmarks.ycsb.YCSBBenchmark;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Benchmarks {
    public final Map<String, Class<? extends BenchmarkModule>> workloads;

    public Benchmarks() {
        workloads = new HashMap<String, Class<? extends BenchmarkModule>>();
        workloads.put("auctionmark", AuctionMarkBenchmark.class);
        workloads.put("chbenchmark", CHBenCHmark.class);
        workloads.put("epinions", EpinionsBenchmark.class);
        workloads.put("hyadapt", HYADAPTBenchmark.class);
        workloads.put("jpab", JPABBenchmark.class);
        workloads.put("linkbench", LinkBenchBenchmark.class);
        workloads.put("noop", NoOpBenchmark.class);
        workloads.put("resourcestresser", ResourceStresserBenchmark.class);
        workloads.put("seats", SEATSBenchmark.class);
        workloads.put("sibench", SIBenchmark.class);
        workloads.put("smallbank", SmallBankBenchmark.class);
        workloads.put("tatp", TATPBenchmark.class);
        workloads.put("tpcc", TPCCBenchmark.class);
        // TODO: it was not in config/plugin.xml, how https://github.com/oltpbenchmark/oltpbench/pull/222 worked?
        workloads.put("tpcds", TPCDSBenchmark.class);
        workloads.put("tpch", TPCHBenchmark.class);
        workloads.put("twitter", TwitterBenchmark.class);
        workloads.put("voter", VoterBenchmark.class);
        workloads.put("wikipedia", WikipediaBenchmark.class);
        workloads.put("ycsb", YCSBBenchmark.class);
    }

    public List<String> workloadsAsList() {
        return new ArrayList<String>(workloads.keySet());
    }
}
