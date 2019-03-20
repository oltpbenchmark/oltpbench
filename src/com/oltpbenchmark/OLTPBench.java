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
import org.apache.log4j.PropertyConfigurator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class OLTPBench extends OLTPBenchMethods {
    public OLTPBench() {
    }

    public static void main(String[] args) throws Exception {
        OLTPBenchMethods oltpMethods = new OLTPBenchMethods();
        String log4jPath = System.getProperty("log4j.configuration");
        if (log4jPath == null) {
            throw new RuntimeException("Missing log4j.properties file");
        } else {
            PropertyConfigurator.configure(log4jPath);
            if (ClassUtil.isAssertsEnabled()) {
                LOG.warn("\n" + oltpMethods.getAssertWarning());
            }

            CommandLineParser parser = new PosixParser();
            XMLConfiguration pluginConfig = null;

            try {
                pluginConfig = new XMLConfiguration("config/plugin.xml");
            } catch (ConfigurationException ex) {
                LOG.info("Plugin configuration file config/plugin.xml is missing");
                ex.printStackTrace();
            }

            pluginConfig.setExpressionEngine(new XPathExpressionEngine());
            Options options = new Options();
            options.addOption("b", "bench", true, "[required] Benchmark class. Currently supported: " + pluginConfig.getList("/plugin//@name"));
            options.addOption("c", "config", true, "[required] Workload configuration file");
            options.addOption((String)null, "create", true, "Initialize the database for this benchmark");
            options.addOption((String)null, "clear", true, "Clear all records in the database for this benchmark");
            options.addOption((String)null, "load", true, "Load data using the benchmark's data loader");
            options.addOption((String)null, "execute", true, "Execute the benchmark workload");
            options.addOption((String)null, "runscript", true, "Run an SQL script");
            options.addOption((String)null, "upload", true, "Upload the result");
            options.addOption("v", "verbose", false, "Display Messages");
            options.addOption("h", "help", false, "Print this help");
            options.addOption("s", "sample", true, "Sampling window");
            options.addOption("im", "interval-monitor", true, "Throughput Monitoring Interval in milliseconds");
            options.addOption("ss", false, "Verbose Sampling per Transaction");
            options.addOption("o", "output", true, "Output file (default System.out)");
            options.addOption("d", "directory", true, "Base directory for the result files, default is current directory");
            options.addOption("t", "timestamp", false, "Each result file is prepended with a timestamp for the beginning of the experiment");
            options.addOption("ts", "tracescript", true, "Script of transactions to execute");
            options.addOption((String)null, "histograms", false, "Print txn histograms");
            options.addOption((String)null, "dialects-export", true, "Export benchmark SQL to a dialects file");
            CommandLine argsLine = parser.parse(options, args);
            if (argsLine.hasOption("h")) {
                oltpMethods.printUsage(options);
            } else if (!argsLine.hasOption("c")) {
                LOG.error("Missing Configuration file");
                oltpMethods.printUsage(options);
            } else if (!argsLine.hasOption("b")) {
                LOG.fatal("Missing Benchmark Class to load");
                oltpMethods.printUsage(options);
            } else {
                int intervalMonitor = 0;
                if (argsLine.hasOption("im")) {
                    intervalMonitor = Integer.parseInt(argsLine.getOptionValue("im"));
                }

                String targetBenchmarks = argsLine.getOptionValue("b");
                String[] targetList = targetBenchmarks.split(",");
                List<BenchmarkModule> benchList = new ArrayList();
                List<TransactionType> activeTXTypes = new ArrayList();
                String configFile = argsLine.getOptionValue("c");
                XMLConfiguration xmlConfig = new XMLConfiguration(configFile);
                xmlConfig.setExpressionEngine(new XPathExpressionEngine());
                System.out.println(oltpMethods.pluginConfigBenchmark(targetList, xmlConfig, argsLine, pluginConfig, configFile, activeTXTypes, benchList));

                assert !benchList.isEmpty();

                assert benchList.get(0) != null;

                if (oltpMethods.isBooleanOptionSet(argsLine, "dialects-export")) {
                    BenchmarkModule bench = (BenchmarkModule)benchList.get(0);
                    if (bench.getStatementDialects() != null) {
                        LOG.info("Exporting StatementDialects for " + bench);
                        String xml = bench.getStatementDialects().export(bench.getWorkloadConfiguration().getDBType(), bench.getProcedures().values());
                        System.out.println(xml);
                        System.exit(0);
                    }

                    throw new RuntimeException("No StatementDialects is available for " + bench);
                } else {
                    boolean verbose = argsLine.hasOption("v");
                    Iterator var15;
                    BenchmarkModule benchmark;
                    if (oltpMethods.isBooleanOptionSet(argsLine, "create")) {
                        var15 = benchList.iterator();

                        while(var15.hasNext()) {
                            benchmark = (BenchmarkModule)var15.next();
                            LOG.info("Creating new " + benchmark.getBenchmarkName().toUpperCase() + " database...");
                            oltpMethods.runCreator(benchmark, verbose);
                            LOG.info("Finished!");
                            LOG.info(SINGLE_LINE);
                        }
                    } else if (LOG.isDebugEnabled()) {
                        LOG.debug("Skipping creating benchmark database tables");
                        LOG.info(SINGLE_LINE);
                    }

                    if (oltpMethods.isBooleanOptionSet(argsLine, "clear")) {
                        var15 = benchList.iterator();

                        while(var15.hasNext()) {
                            benchmark = (BenchmarkModule)var15.next();
                            LOG.info("Resetting " + benchmark.getBenchmarkName().toUpperCase() + " database...");
                            benchmark.clearDatabase();
                            LOG.info("Finished!");
                            LOG.info(SINGLE_LINE);
                        }
                    } else if (LOG.isDebugEnabled()) {
                        LOG.debug("Skipping creating benchmark database tables");
                        LOG.info(SINGLE_LINE);
                    }

                    if (oltpMethods.isBooleanOptionSet(argsLine, "load")) {
                        var15 = benchList.iterator();

                        while(var15.hasNext()) {
                            benchmark = (BenchmarkModule)var15.next();
                            LOG.info("Loading data into " + benchmark.getBenchmarkName().toUpperCase() + " database...");
                            oltpMethods.runLoader(benchmark, verbose);
                            LOG.info("Finished!");
                            LOG.info(SINGLE_LINE);
                        }
                    } else if (LOG.isDebugEnabled()) {
                        LOG.debug("Skipping loading benchmark database records");
                        LOG.info(SINGLE_LINE);
                    }

                    if (argsLine.hasOption("runscript")) {
                        var15 = benchList.iterator();

                        while(var15.hasNext()) {
                            benchmark = (BenchmarkModule)var15.next();
                            String script = argsLine.getOptionValue("runscript");
                            LOG.info("Running a SQL script: " + script);
                            oltpMethods.runScript(benchmark, script);
                            LOG.info("Finished!");
                            LOG.info(SINGLE_LINE);
                        }
                    }

                    if (oltpMethods.isBooleanOptionSet(argsLine, "execute")) {
                        Results r = null;

                        try {
                            r = oltpMethods.runWorkload(benchList, verbose, intervalMonitor);
                        } catch (Throwable var18) {
                            LOG.error("Unexpected error when running benchmarks.", var18);
                            System.exit(1);
                        }

                        assert r != null;

                        oltpMethods.writeOutputs(r, activeTXTypes, argsLine, xmlConfig);
                        if (argsLine.hasOption("histograms")) {
                            oltpMethods.writeHistograms(r);
                        }
                    } else {
                        LOG.info("Skipping benchmark workload execution");
                    }

                }
            }
        }
    }
}

