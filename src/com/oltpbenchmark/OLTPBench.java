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

import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.util.ClassUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.tree.xpath.XPathExpressionEngine;

import java.util.ArrayList;
import java.util.List;

public class OLTPBench extends OLTPBenchMethods {


    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        OLTPBenchMethods oltpMethods = new OLTPBenchMethods();
        // Initialize log4j
        String log4jPath = System.getProperty("log4j.configuration");
        if (log4jPath != null) {
            org.apache.log4j.PropertyConfigurator.configure(log4jPath);
        } else {
            throw new RuntimeException("Missing log4j.properties file");
        }

        if (ClassUtil.isAssertsEnabled()) {
            LOG.warn("\n" + oltpMethods.getAssertWarning());
        }

        // create the command line parser
        CommandLineParser parser = new PosixParser();
        XMLConfiguration pluginConfig = null;
        try {
            pluginConfig = new XMLConfiguration("config/plugin.xml");
        } catch (ConfigurationException e1) {
            LOG.info("Plugin configuration file config/plugin.xml is missing");
            e1.printStackTrace();
        }
        pluginConfig.setExpressionEngine(new XPathExpressionEngine());
        Options options = new Options();
        options.addOption(
                "b",
                "bench",
                true,
                "[required] Benchmark class. Currently supported: " + pluginConfig.getList("/plugin//@name"));
        options.addOption(
                "c",
                "config",
                true,
                "[required] Workload configuration file");
        options.addOption(
                null,
                "create",
                true,
                "Initialize the database for this benchmark");
        options.addOption(
                null,
                "clear",
                true,
                "Clear all records in the database for this benchmark");
        options.addOption(
                null,
                "load",
                true,
                "Load data using the benchmark's data loader");
        options.addOption(
                null,
                "execute",
                true,
                "Execute the benchmark workload");
        options.addOption(
                null,
                "runscript",
                true,
                "Run an SQL script");
        options.addOption(
                null,
                "upload",
                true,
                "Upload the result");

        options.addOption("v", "verbose", false, "Display Messages");
        options.addOption("h", "help", false, "Print this help");
        options.addOption("s", "sample", true, "Sampling window");
        options.addOption("im", "interval-monitor", true, "Throughput Monitoring Interval in milliseconds");
        options.addOption("ss", false, "Verbose Sampling per Transaction");
        options.addOption("o", "output", true, "Output file (default System.out)");
        options.addOption("d", "directory", true, "Base directory for the result files, default is current directory");
        options.addOption("t", "timestamp", false, "Each result file is prepended with a timestamp for the beginning of the experiment");
        options.addOption("ts", "tracescript", true, "Script of transactions to execute");
        options.addOption(null, "histograms", false, "Print txn histograms");
        options.addOption(null, "dialects-export", true, "Export benchmark SQL to a dialects file");

        // parse the command line arguments
        CommandLine argsLine = parser.parse(options, args);
        if (argsLine.hasOption("h")) {
            oltpMethods.printUsage(options);
            return;
        } else if (argsLine.hasOption("c") == false) {
            LOG.error("Missing Configuration file");
            oltpMethods.printUsage(options);
            return;
        } else if (argsLine.hasOption("b") == false) {
            LOG.fatal("Missing Benchmark Class to load");
            oltpMethods.printUsage(options);
            return;
        }


        // Seconds
        int intervalMonitor = 0;
        if (argsLine.hasOption("im")) {
            intervalMonitor = Integer.parseInt(argsLine.getOptionValue("im"));
        }

        // -------------------------------------------------------------------
        // GET PLUGIN LIST
        // -------------------------------------------------------------------

        String targetBenchmarks = argsLine.getOptionValue("b");

        String[] targetList = targetBenchmarks.split(",");
        List<BenchmarkModule> benchList = new ArrayList<BenchmarkModule>();

        // Use this list for filtering of the output
        List<TransactionType> activeTXTypes = new ArrayList<TransactionType>();

        String configFile = argsLine.getOptionValue("c");
        XMLConfiguration xmlConfig = new XMLConfiguration(configFile);
        xmlConfig.setExpressionEngine(new XPathExpressionEngine());

        // Load the configuration for each benchmark
        System.out.println(oltpMethods.pluginConfigBenchmark(targetList, xmlConfig, argsLine, pluginConfig, configFile, activeTXTypes, benchList));

        assert (benchList.isEmpty() == false);
        assert (benchList.get(0) != null);

        // Export StatementDialects
        if (oltpMethods.isBooleanOptionSet(argsLine, "dialects-export")) {
            BenchmarkModule bench = benchList.get(0);
            if (bench.getStatementDialects() != null) {
                LOG.info("Exporting StatementDialects for " + bench);
                String xml = bench.getStatementDialects().export(bench.getWorkloadConfiguration().getDBType(),
                        bench.getProcedures().values());
                System.out.println(xml);
                System.exit(0);
            }
            throw new RuntimeException("No StatementDialects is available for " + bench);
        }


        @Deprecated
        boolean verbose = argsLine.hasOption("v");

        // Create the Benchmark's Database
        if (oltpMethods.isBooleanOptionSet(argsLine, "create")) {
            for (BenchmarkModule benchmark : benchList) {
                LOG.info("Creating new " + benchmark.getBenchmarkName().toUpperCase() + " database...");
                oltpMethods.runCreator(benchmark, verbose);
                LOG.info("Finished!");
                LOG.info(SINGLE_LINE);
            }
        } else if (LOG.isDebugEnabled()) {
            LOG.debug("Skipping creating benchmark database tables");
            LOG.info(SINGLE_LINE);
        }

        // Clear the Benchmark's Database
        if (oltpMethods.isBooleanOptionSet(argsLine, "clear")) {
            for (BenchmarkModule benchmark : benchList) {
                LOG.info("Resetting " + benchmark.getBenchmarkName().toUpperCase() + " database...");
                benchmark.clearDatabase();
                LOG.info("Finished!");
                LOG.info(SINGLE_LINE);
            }
        } else if (LOG.isDebugEnabled()) {
            LOG.debug("Skipping creating benchmark database tables");
            LOG.info(SINGLE_LINE);
        }

        // Execute Loader
        if (oltpMethods.isBooleanOptionSet(argsLine, "load")) {
            for (BenchmarkModule benchmark : benchList) {
                LOG.info("Loading data into " + benchmark.getBenchmarkName().toUpperCase() + " database...");
                oltpMethods.runLoader(benchmark, verbose);
                LOG.info("Finished!");
                LOG.info(SINGLE_LINE);
            }
        } else if (LOG.isDebugEnabled()) {
            LOG.debug("Skipping loading benchmark database records");
            LOG.info(SINGLE_LINE);
        }

        // Execute a Script
        if (argsLine.hasOption("runscript")) {
            for (BenchmarkModule benchmark : benchList) {
                String script = argsLine.getOptionValue("runscript");
                LOG.info("Running a SQL script: " + script);
                oltpMethods.runScript(benchmark, script);
                LOG.info("Finished!");
                LOG.info(SINGLE_LINE);
            }
        }

        // Execute Workload
        if (oltpMethods.isBooleanOptionSet(argsLine, "execute")) {
            // Bombs away!
            Results r = null;
            try {
                r = oltpMethods.runWorkload(benchList, verbose, intervalMonitor);
            } catch (Throwable ex) {
                LOG.error("Unexpected error when running benchmarks.", ex);
                System.exit(1);
            }
            assert (r != null);

            // WRITE OUTPUT
            oltpMethods.writeOutputs(r, activeTXTypes, argsLine, xmlConfig);

            // WRITE HISTOGRAMS
            if (argsLine.hasOption("histograms")) {
                oltpMethods.writeHistograms(r);
            }


        } else {
            LOG.info("Skipping benchmark workload execution");
        }
    }
}
