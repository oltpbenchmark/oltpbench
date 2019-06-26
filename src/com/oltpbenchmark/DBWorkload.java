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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.SubnodeConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.tree.xpath.XPathExpressionEngine;
import org.apache.log4j.Logger;

import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.TransactionTypes;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.api.collectors.DBCollector;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.util.ClassUtil;
import com.oltpbenchmark.util.FileUtil;
import com.oltpbenchmark.util.QueueLimitException;
import com.oltpbenchmark.util.ResultUploader;
import com.oltpbenchmark.util.StringBoxUtil;
import com.oltpbenchmark.util.StringUtil;
import com.oltpbenchmark.util.TimeUtil;

public class DBWorkload {
    private static final Logger LOG = Logger.getLogger(DBWorkload.class);
    
    private static final String SINGLE_LINE = StringUtil.repeat("=", 70);
    
    private static final String RATE_DISABLED = "disabled";
    private static final String RATE_UNLIMITED = "unlimited";
    
    private static final String DEFAULT_RESULT_DIRECTORY = "results";
    private static final String DEFAULT_RESULT_FILENAME = "oltpbench";
    private static final int DEFAULT_SAMPLING_WINDOW = 1;

    /**
     * @param args
     * @throws Exception 
     */
    public static void main(String[] args) throws Exception {
        // Initialize log4j
        String log4jPath = System.getProperty("log4j.configuration");
        if (log4jPath != null) {
            org.apache.log4j.PropertyConfigurator.configure(log4jPath);
        } else {
            throw new RuntimeException("Missing log4j.properties file");
        }
        
        if (ClassUtil.isAssertsEnabled()) {
            LOG.warn("\n" + getAssertWarning());
        }
        
        // create the command line parser
        CommandLineParser parser = new PosixParser();
        XMLConfiguration pluginConfig=null;
        try {
            pluginConfig = new XMLConfiguration("config/plugin.xml");
        } catch (ConfigurationException e1) {
            LOG.info("Plugin configuration file config/plugin.xml is missing");
            e1.printStackTrace();
        }
        pluginConfig.setExpressionEngine(new XPathExpressionEngine());

        Options options = new Options();
        options.addOption("v", "verbose", false, "Display Messages");
        options.addOption("h", "help", false, "Print this help");
        options.addOption(
                "b",
                "bench",
                true,
                "[required] Benchmark class. Currently supported: "+ pluginConfig.getList("/plugin//@name"));
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
                "interval-monitor",
                true,
                "Throughput Monitoring Interval in milliseconds");
        options.addOption(
                null,
                "tracescript",
                true,
                "Script of transactions to execute");
        options.addOption(
                null,
                "dialects-export",
                true,
                "Export benchmark SQL to a dialects file");
        options.addOption(
                null,
                "histograms",
                false,
                "Print transaction histograms");
        options.addOption(
                null,
                "upload",
                false,
                "Upload the result");
        options.addOption(
                null,
                "uploadHash",
                true,
                "Git hash to be associated with the upload");
        options.addOption(
                "d",
                "directory",
                true,
                "Base directory for the result files [default: '" + DEFAULT_RESULT_DIRECTORY + "']");
        options.addOption(
                "f",
                "file",
                true,
                "Output filename [default: '" + DEFAULT_RESULT_FILENAME + "']");
        options.addOption(
                "t",
                "timestamp",
                false,
                "Prepend each result files with a timestamp");
        options.addOption(
                "w",
                "window",
                true,
                "Transaction sampling window [default: " + DEFAULT_SAMPLING_WINDOW + " sec]");
        options.addOption(
                "s",
                null,
                false,
                "Output sample data");
        options.addOption(
                "x",
                null,
                false,
                "Output verbose sampling per txn");
        options.addOption(
                "r",
                null,
                false,
                "Output raw transaction data");
        options.addOption(
                "g",
                null,
                false,
                "Output the benchmark configuration");
        options.addOption(
                "o",
                null,
                false,
                "Output the performance summary");
        options.addOption(
                "k",
                null,
                false,
                "Output the DBMS's configuration settings");
        options.addOption(
                "m",
                null,
                false,
                "Output the DBMS's runtime metrics");
        options.addOption(
                null,
                "output-minimal",
                false,
                "Alias for output options '-so'");
        options.addOption(
                null,
                "output-uploader",
                false,
                "Alias for output options '-srgokm' (same as results uploaded by the ResultUploader)");
        options.addOption(
                null,
                "output-all",
                false,
                "Alias for all output options");

        // parse the command line arguments
        CommandLine argsLine = parser.parse(options, args);
        if (argsLine.hasOption("h")) {
            printUsage(options);
            return;
        } else if (argsLine.hasOption("c") == false) {
            LOG.error("Missing Configuration file");
            printUsage(options);
            return;
        } else if (argsLine.hasOption("b") == false) {
            LOG.fatal("Missing Benchmark Class to load");
            printUsage(options);
            return;
        }

        // Output options
        String allOpts = "sxrgokm";
        Set<Character> outputOpts = new HashSet<Character>();
        if (argsLine.hasOption("output-minimal")) {
            outputOpts.add('s');
            outputOpts.add('o');
        } else if (argsLine.hasOption("output-uploader")) {
            String uploaderOpts = "srgokm";
            for (int i = 0; i < uploaderOpts.length(); ++i)
                outputOpts.add(uploaderOpts.charAt(i));
        } else if (argsLine.hasOption("output-all")) {
            for (int i = 0; i < allOpts.length(); ++i)
                outputOpts.add(allOpts.charAt(i));
        }
        for (int i = 0; i < allOpts.length(); ++i) {
            char opt = allOpts.charAt(i);
            if (argsLine.hasOption(opt))
                outputOpts.add(opt);
        }

        Map<String, Object> outputSettings = null;
        if (!outputOpts.isEmpty()) {
            outputSettings = new HashMap<String, Object>();
            outputSettings.put("options", outputOpts);

            // Output directory
            outputSettings.put("directory", argsLine.getOptionValue("d", DEFAULT_RESULT_DIRECTORY));

            // Output filename
            String filename = argsLine.getOptionValue("f", DEFAULT_RESULT_FILENAME);

            // Timestamp file prefix
            String prefix = "";
            if (argsLine.hasOption("t")) {
                prefix = String.valueOf(TimeUtil.getCurrentTime().getTime()) + "_";
            }
            outputSettings.put("filename", prefix + filename);

            // Sampling window
            int window = DEFAULT_SAMPLING_WINDOW;
            if (argsLine.hasOption("w")) {
                try {
                    window = Integer.parseInt(argsLine.getOptionValue("w"));
                } catch (NumberFormatException ex) {
                    LOG.warn(String.format("Invalid sampling window: %s, using default: %s",
                            argsLine.getOptionValue("w"), DEFAULT_SAMPLING_WINDOW));
                }
            }
            outputSettings.put("window", window);
        }

        // Seconds
        int intervalMonitor = 0;
        if (argsLine.hasOption("interval-monitor")) {
            intervalMonitor = Integer.parseInt(argsLine.getOptionValue("interval-monitor"));
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

        // Uploader options
        Map<String, String> uploaderSettings = null;
        if (argsLine.hasOption("upload")) {
            uploaderSettings = new HashMap<String, String>();
            String uploadUrl = xmlConfig.getString("uploadUrl");
            String uploadCode = xmlConfig.getString("uploadCode");

            if (uploadUrl == null || uploadCode == null) {
                LOG.fatal(String.format("The upload URL/CODE cannot be null. URL: %s, CODE: %s", uploadUrl, uploadCode));
                System.exit(-1);
            }
            String uploadHash = argsLine.getOptionValue("uploadHash", "");

            uploaderSettings.put("uploadUrl", uploadUrl);
            uploaderSettings.put("uploadCode", uploadCode);
            uploaderSettings.put("uploadHash", uploadHash);
        }

        DatabaseType dbType = DatabaseType.get(xmlConfig.getString("dbtype"));
        Database database = new Database(dbType, xmlConfig.getString("DBName"), xmlConfig.getString("driver"),
                xmlConfig.getString("DBUrl"), xmlConfig.getString("username"), xmlConfig.getString("password"));

        // Load the configuration for each benchmark
        int lastTxnId = 0;
        for (String plugin : targetList) {
            String pluginTest = "[@bench='" + plugin + "']";

            // ----------------------------------------------------------------
            // BEGIN LOADING WORKLOAD CONFIGURATION
            // ----------------------------------------------------------------
            
            WorkloadConfiguration wrkld = new WorkloadConfiguration();
            wrkld.setBenchmarkName(plugin);
            wrkld.setXmlConfig(xmlConfig);
            boolean scriptRun = false;
            if (argsLine.hasOption("tracescript")) {
                scriptRun = true;
                String traceFile = argsLine.getOptionValue("tracescript");
                wrkld.setTraceReader(new TraceReader(traceFile));
                if (LOG.isDebugEnabled()) LOG.debug(wrkld.getTraceReader().toString());
            }

            // Pull in database configuration
            wrkld.setDBType(dbType);
            wrkld.setDB(database);
            
            int terminals = xmlConfig.getInt("terminals[not(@bench)]", 0);
            terminals = xmlConfig.getInt("terminals" + pluginTest, terminals);
            wrkld.setTerminals(terminals);
            
            if (xmlConfig.containsKey("loaderThreads")) {
                int loaderThreads = xmlConfig.getInt("loaderThreads");
                wrkld.setLoaderThreads(loaderThreads);
            }
            
            String isolationMode = xmlConfig.getString("isolation[not(@bench)]", "TRANSACTION_SERIALIZABLE");
            wrkld.setIsolationMode(xmlConfig.getString("isolation" + pluginTest, isolationMode));
            wrkld.setScaleFactor(xmlConfig.getDouble("scalefactor", 1.0));
            wrkld.setRecordAbortMessages(xmlConfig.getBoolean("recordabortmessages", false));
            wrkld.setDataDir(xmlConfig.getString("datadir", "."));

            double selectivity = -1;
            try {
                selectivity = xmlConfig.getDouble("selectivity");
                wrkld.setSelectivity(selectivity);
            }
            catch(NoSuchElementException nse) {  
                // Nothing to do here !
            }

            // ----------------------------------------------------------------
            // CREATE BENCHMARK MODULE
            // ----------------------------------------------------------------

            String classname = pluginConfig.getString("/plugin[@name='" + plugin + "']");

            if (classname == null)
                throw new ParseException("Plugin " + plugin + " is undefined in config/plugin.xml");
            BenchmarkModule bench = ClassUtil.newInstance(classname,
                                                          new Object[] { wrkld },
                                                          new Class<?>[] { WorkloadConfiguration.class });
            Map<String, Object> initDebug = new ListOrderedMap<String, Object>();
            initDebug.put("Benchmark", String.format("%s {%s}", plugin.toUpperCase(), classname));
            initDebug.put("Configuration", configFile);
            initDebug.put("Type", wrkld.getDBType());
            initDebug.put("Driver", wrkld.getDB().getDriver());
            initDebug.put("URL", wrkld.getDB().getUrl());
            initDebug.put("Isolation", wrkld.getIsolationString());
            initDebug.put("Scale Factor", wrkld.getScaleFactor());
            
            if(selectivity != -1)
                initDebug.put("Selectivity", selectivity);

            LOG.info(SINGLE_LINE + "\n\n" + StringUtil.formatMaps(initDebug));
            LOG.info(SINGLE_LINE);

            // ----------------------------------------------------------------
            // LOAD TRANSACTION DESCRIPTIONS
            // ----------------------------------------------------------------
            int numTxnTypes = xmlConfig.configurationsAt("transactiontypes" + pluginTest + "/transactiontype").size();
            if (numTxnTypes == 0 && targetList.length == 1) {
                //if it is a single workload run, <transactiontypes /> w/o attribute is used
                pluginTest = "[not(@bench)]";
                numTxnTypes = xmlConfig.configurationsAt("transactiontypes" + pluginTest + "/transactiontype").size();
            }
            wrkld.setNumTxnTypes(numTxnTypes);

            List<TransactionType> ttypes = new ArrayList<TransactionType>();
            ttypes.add(TransactionType.INVALID);
            int txnIdOffset = lastTxnId;
            for (int i = 1; i <= wrkld.getNumTxnTypes(); i++) {
                String key = "transactiontypes" + pluginTest + "/transactiontype[" + i + "]";
                String txnName = xmlConfig.getString(key + "/name");

                // Get ID if specified; else increment from last one.
                int txnId = i;
                if (xmlConfig.containsKey(key + "/id")) {
                    txnId = xmlConfig.getInt(key + "/id");
                }

                TransactionType tmpType = bench.initTransactionType(txnName, txnId + txnIdOffset);

                // Keep a reference for filtering
                activeTXTypes.add(tmpType);

                // Add a ref for the active TTypes in this benchmark
                ttypes.add(tmpType);
                lastTxnId = i;
            } // FOR

            // Wrap the list of transactions and save them
            TransactionTypes tt = new TransactionTypes(ttypes);
            wrkld.setTransTypes(tt);
            LOG.debug("Using the following transaction types: " + tt);

            // Read in the groupings of transactions (if any) defined for this
            // benchmark
            HashMap<String,List<String>> groupings = new HashMap<String,List<String>>();
            int numGroupings = xmlConfig.configurationsAt("transactiontypes" + pluginTest + "/groupings/grouping").size();
            LOG.debug("Num groupings: " + numGroupings);
            for (int i = 1; i < numGroupings + 1; i++) {
                String key = "transactiontypes" + pluginTest + "/groupings/grouping[" + i + "]";

                // Get the name for the grouping and make sure it's valid.
                String groupingName = xmlConfig.getString(key + "/name").toLowerCase();
                if (!groupingName.matches("^[a-z]\\w*$")) {
                    LOG.fatal(String.format("Grouping name \"%s\" is invalid."
                                + " Must begin with a letter and contain only"
                                + " alphanumeric characters.", groupingName));
                    System.exit(-1);
                }
                else if (groupingName.equals("all")) {
                    LOG.fatal("Grouping name \"all\" is reserved."
                              + " Please pick a different name.");
                    System.exit(-1);
                }

                // Get the weights for this grouping and make sure that there
                // is an appropriate number of them.
                List<String> groupingWeights = xmlConfig.getList(key + "/weights");
                if (groupingWeights.size() != numTxnTypes) {
                    LOG.fatal(String.format("Grouping \"%s\" has %d weights,"
                                + " but there are %d transactions in this"
                                + " benchmark.", groupingName,
                                groupingWeights.size(), numTxnTypes));
                    System.exit(-1);
                }

                LOG.debug("Creating grouping with name, weights: " + groupingName + ", " + groupingWeights);
                groupings.put(groupingName, groupingWeights);
            }

            // All benchmarks should also have an "all" grouping that gives
            // even weight to all transactions in the benchmark.
            List<String> weightAll = new ArrayList<String>();
            for (int i = 0; i < numTxnTypes; ++i)
                weightAll.add("1");
            groupings.put("all", weightAll);
            benchList.add(bench);

            // ----------------------------------------------------------------
            // WORKLOAD CONFIGURATION
            // ----------------------------------------------------------------
            
            int size = xmlConfig.configurationsAt("/works/work").size();
            for (int i = 1; i < size + 1; i++) {
                SubnodeConfiguration work = xmlConfig.configurationAt("works/work[" + i + "]");
                List<String> weight_strings;
                
                // use a workaround if there multiple workloads or single
                // attributed workload
                if (targetList.length > 1 || work.containsKey("weights[@bench]")) {
                    String weightKey = work.getString("weights" + pluginTest).toLowerCase();
                    if (groupings.containsKey(weightKey))
                        weight_strings = groupings.get(weightKey);
                    else
                    weight_strings = getWeights(plugin, work);
                } else {
                    String weightKey = work.getString("weights[not(@bench)]").toLowerCase();
                    if (groupings.containsKey(weightKey))
                        weight_strings = groupings.get(weightKey);
                    else
                    weight_strings = work.getList("weights[not(@bench)]"); 
                }
                int rate = 1;
                boolean rateLimited = true;
                boolean disabled = false;
                boolean serial = false;
                boolean timed = false;

                // can be "disabled", "unlimited" or a number
                String rate_string;
                rate_string = work.getString("rate[not(@bench)]", "");
                rate_string = work.getString("rate" + pluginTest, rate_string);
                if (rate_string.equals(RATE_DISABLED)) {
                    disabled = true;
                } else if (rate_string.equals(RATE_UNLIMITED)) {
                    rateLimited = false;
                } else if (rate_string.isEmpty()) {
                    LOG.fatal(String.format("Please specify the rate for phase %d and workload %s", i, plugin));
                    System.exit(-1);
                } else {
                    try {
                        rate = Integer.parseInt(rate_string);
                        if (rate < 1) {
                            LOG.fatal("Rate limit must be at least 1. Use unlimited or disabled values instead.");
                            System.exit(-1);
                        }
                    } catch (NumberFormatException e) {
                        LOG.fatal(String.format("Rate string must be '%s', '%s' or a number", RATE_DISABLED, RATE_UNLIMITED));
                        System.exit(-1);
                    }
                }
                Phase.Arrival arrival=Phase.Arrival.REGULAR;
                String arrive=work.getString("@arrival","regular");
                if(arrive.toUpperCase().equals("POISSON"))
                    arrival=Phase.Arrival.POISSON;
                
                // We now have the option to run all queries exactly once in
                // a serial (rather than random) order.
                String serial_string;
                serial_string = work.getString("serial", "false");
                if (serial_string.equals("true")) {
                    serial = true;
                }
                else if (serial_string.equals("false")) {
                    serial = false;
                }
                else {
                    LOG.fatal("Serial string should be either 'true' or 'false'.");
                    System.exit(-1);
                }

                // We're not actually serial if we're running a script, so make
                // sure to suppress the serial flag in this case.
                serial = serial && (wrkld.getTraceReader() == null);

                int activeTerminals;
                activeTerminals = work.getInt("active_terminals[not(@bench)]", terminals);
                activeTerminals = work.getInt("active_terminals" + pluginTest, activeTerminals);
                // If using serial, we should have only one terminal
                if (serial && activeTerminals != 1) {
                    LOG.warn("Serial ordering is enabled, so # of active terminals is clamped to 1.");
                    activeTerminals = 1;
                }
                if (activeTerminals > terminals) {
                    LOG.error(String.format("Configuration error in work %d: " +
                                            "Number of active terminals is bigger than the total number of terminals",
                              i));
                    System.exit(-1);
                }

                int time = work.getInt("/time", 0);
                int warmup = work.getInt("/warmup", 0);
                timed = (time > 0);
                if (scriptRun) {
                    LOG.info("Running a script; ignoring timer, serial, and weight settings.");
                }
                else if (!timed) {
                    if (serial)
                        LOG.info("Timer disabled for serial run; will execute"
                                 + " all queries exactly once.");
                    else {
                        LOG.fatal("Must provide positive time bound for"
                                  + " non-serial executions. Either provide"
                                  + " a valid time or enable serial mode.");
                        System.exit(-1);
                    }
                }
                else if (serial)
                    LOG.info("Timer enabled for serial run; will run queries"
                             + " serially in a loop until the timer expires.");
                if (warmup < 0) {
                    LOG.fatal("Must provide nonnegative time bound for"
                            + " warmup.");
                    System.exit(-1);
                }

                wrkld.addWork(time,
                              warmup,
                              rate,
                              weight_strings,
                              rateLimited,
                              disabled,
                        serial,
                        timed,
                              activeTerminals,
                              arrival);
            } // FOR
    
            // CHECKING INPUT PHASES
            int j = 0;
            for (Phase p : wrkld.getAllPhases()) {
                j++;
                if (p.getWeightCount() != wrkld.getNumTxnTypes()) {
                    LOG.fatal(String.format("Configuration files is inconsistent, phase %d contains %d weights but you defined %d transaction types",
                                            j, p.getWeightCount(), wrkld.getNumTxnTypes()));
                    if (p.isSerial()) {
                        LOG.fatal("However, note that since this a serial phase, the weights are irrelevant (but still must be included---sorry).");
                    }
                    System.exit(-1);
                }
            } // FOR
    
            // Generate the dialect map
            wrkld.init();
    
            assert (wrkld.getNumTxnTypes() >= 0);
            assert (xmlConfig != null);
        }
        assert(benchList.isEmpty() == false);
        assert(benchList.get(0) != null);
        
        // Export StatementDialects
        if (isBooleanOptionSet(argsLine, "dialects-export")) {
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
        if (isBooleanOptionSet(argsLine, "create")) {
            for (BenchmarkModule benchmark : benchList) {
                LOG.info("Creating new " + benchmark.getBenchmarkName().toUpperCase() + " database...");
                runCreator(benchmark, verbose);
                LOG.info("Finished!");
                LOG.info(SINGLE_LINE);
            }
        } else if (LOG.isDebugEnabled()) {
            LOG.debug("Skipping creating benchmark database tables");
            LOG.info(SINGLE_LINE);
        }

        // Clear the Benchmark's Database
        if (isBooleanOptionSet(argsLine, "clear")) {
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
        if (isBooleanOptionSet(argsLine, "load")) {
            for (BenchmarkModule benchmark : benchList) {
                LOG.info(String.format("Loading data into %s database with %d threads...",
                                       benchmark.getBenchmarkName().toUpperCase(),
                                       benchmark.getWorkloadConfiguration().getLoaderThreads()));
                runLoader(benchmark, verbose);
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
                LOG.info("Running a SQL script: "+script);
                runScript(benchmark, script);
                LOG.info("Finished!");
                LOG.info(SINGLE_LINE);
            }
        }

        // Execute Workload
        if (isBooleanOptionSet(argsLine, "execute")) {
            // Bombs away!
            Results r = null;
            try {
                r = runWorkload(benchList, verbose, intervalMonitor);
            } catch (Throwable ex) {
                LOG.error("Unexpected error when running benchmarks.", ex);
                System.exit(1);
            }
            assert(r != null);

            WorkloadConfiguration workConf = benchList.get(0).getWorkloadConfiguration();

            // WRITE OUTPUT
            Map<String, String> outputFiles = null;
            if (outputSettings != null) {
                outputFiles = writeOutputs(outputSettings, r, activeTXTypes, workConf);
            } else {
                outputFiles = new HashMap<String, String>();
            }

            // UPLOAD RESULTS
            if (uploaderSettings != null) {
                ResultUploader ru = new ResultUploader(uploaderSettings.get("uploadUrl"),
                        uploaderSettings.get("uploadCode"), uploaderSettings.get("uploadHash"));
                ru.uploadResult(activeTXTypes, r, workConf, outputFiles);
            }

            // WRITE HISTOGRAMS
            if (argsLine.hasOption("histograms")) {
                writeHistograms(r);
            }

            // PRINT EXECUTION SUMMARY
            printSummary(workConf, r);

        } else {
            LOG.info("Skipping benchmark workload execution");
        }
    }

    private static void printSummary(WorkloadConfiguration wrkld, Results results) {
        Map<String, Object> summary = new ListOrderedMap<String, Object>();
        DistributionStatistics latencyDist = results.getLatencyDistribution();

        summary.put("Benchmark", wrkld.getBenchmarkName().toUpperCase());
        summary.put("DBMS", String.format("%s %s", wrkld.getDBType(), wrkld.getDB().getVersion()));
        summary.put("Isolation", wrkld.getIsolationString());
        summary.put("Scalefactor", wrkld.getScaleFactor() + "\n");

        summary.put("Time", String.format("%.0f sec", results.getRuntimeSeconds()));
        summary.put("Requests", results.getRequests());
        summary.put("Throughput", String.format("%.2f requests/sec\n", results.getRequestsPerSecond()));

        String f = "%10.2f ms";
        double scale = 1e3;
        summary.put("Latency Distribution", "");
        summary.put(" - Average", String.format(f, latencyDist.getAverage() / scale));
        summary.put(" - Minimum", String.format(f, latencyDist.getMinimum() / scale));
        summary.put(" - 25th %-tile", String.format(f, latencyDist.get25thPercentile() / scale));
        summary.put(" - 50th %-tile", String.format(f, latencyDist.getMedian() / scale));
        summary.put(" - 75th %-tile", String.format(f, latencyDist.get75thPercentile() / scale));
        summary.put(" - 90th %-tile", String.format(f, latencyDist.get90thPercentile() / scale));
        summary.put(" - 95th %-tile", String.format(f, latencyDist.get95thPercentile() / scale));
        summary.put(" - 99th %-tile", String.format(f, latencyDist.get99thPercentile() / scale));
        summary.put(" - Maximum", String.format(f, latencyDist.getMaximum() / scale));

        String summaryStr = StringUtil.prefix(StringUtil.formatMaps(summary), "  ");
        String title = "*** EXECUTION SUMMARY ***";
        LOG.info(String.format("%s\n\n%s\n\n%s", SINGLE_LINE, title, summaryStr));
        LOG.info(SINGLE_LINE);
    }

    private static void writeHistograms(Results r) {
        StringBuilder sb = new StringBuilder();
        
        sb.append(StringUtil.bold("Completed Transactions:"))
          .append("\n")
          .append(r.getTransactionSuccessHistogram())
          .append("\n\n");
        
        sb.append(StringUtil.bold("Aborted Transactions:"))
          .append("\n")
          .append(r.getTransactionAbortHistogram())
          .append("\n\n");
        
        sb.append(StringUtil.bold("Rejected Transactions (Server Retry):"))
          .append("\n")
          .append(r.getTransactionRetryHistogram())
          .append("\n\n");
        
        sb.append(StringUtil.bold("Unexpected Errors:"))
          .append("\n")
          .append(r.getTransactionErrorHistogram());
        
        if (r.getTransactionAbortMessageHistogram().isEmpty() == false)
            sb.append("\n\n")
              .append(StringUtil.bold("User Aborts:"))
              .append("\n")
              .append(r.getTransactionAbortMessageHistogram());
        
        LOG.info(SINGLE_LINE);
        LOG.info("Workload Histograms:\n" + sb.toString());
        LOG.info(SINGLE_LINE);
    }

    /**
     * Write out the results for a benchmark run to a bunch of files
     *
     * @param outputSettings
     * @param results
     * @param activeTXTypes
     * @param workConf
     * @throws Exception
     */
    private static Map<String, String> writeOutputs(Map<String, Object> outputSettings, Results results,
            List<TransactionType> activeTXTypes,
            WorkloadConfiguration workConf) throws Exception {

        String directory = (String) outputSettings.get("directory");
        FileUtil.makeDirIfNotExists(directory.split("/"));
        String filename = (String) outputSettings.get("filename");
        String basePath = FileUtil.joinPath(directory, filename);
        LOG.debug("Output path: " + basePath);

        // Keep track of the results we output so that the result uploader
        // class can reuse them
        Map<String, String> uploaderFiles = new HashMap<String, String>();

        PrintStream ps;
        String nextName = null;
        int window = (int) outputSettings.get("window");

        @SuppressWarnings("unchecked")
        Set<Character> outputOptions = (Set<Character>) outputSettings.get("options");

        if (outputOptions.contains('s')) {
            nextName = FileUtil.getNextFilename(basePath + ".samples");
            LOG.info(String.format("Saving timeseries results to: %s [window=%ds]", nextName, window));
            ps = new PrintStream(new File(nextName));
            results.writeCSV2(window, ps);
            ps.close();

            if (window == 1)
                uploaderFiles.put("samples", nextName);
        }

        if (outputOptions.contains('x')) {
            String txnName = null;
            for (TransactionType t : activeTXTypes) {
                txnName = t.getName();
                nextName = FileUtil.getNextFilename(basePath + "_" + txnName + ".samples");
                ps = new PrintStream(new File(nextName));
                results.writeCSV2(window, ps, t);
                ps.close();
            }
            if (txnName != null) {
                LOG.info(String.format("Saving transaction results to: %s [window=%ds]",
                        nextName.replace(txnName, "*"), window));
            }
        }

        if (outputOptions.contains('r')) {
            nextName = FileUtil.getNextFilename(basePath + ".csv.gz");
            LOG.info(String.format("Saving raw performance data to: %s", nextName));
            ps = new PrintStream(new GZIPOutputStream(new FileOutputStream(nextName)));
            results.writeAllCSVAbsoluteTiming(activeTXTypes, ps);
            ps.close();
            uploaderFiles.put("csv.gz", nextName);
        }

        if (outputOptions.contains('g')) {
            nextName = FileUtil.getNextFilename(basePath + ".expconfig");
            ps = new PrintStream(new File(nextName));
            LOG.info("Saving benchmark configuration to: " + nextName);
            ResultUploader.writeBenchmarkConf(workConf.getXmlConfig(), ps);
            ps.close();
            uploaderFiles.put("expconfig", nextName);
        }

        if (outputOptions.contains('o')) {
            nextName = FileUtil.getNextFilename(basePath + ".summary");
            ps = new PrintStream(new File(nextName));
            LOG.info("Saving benchmark summary to: " + nextName);
            ResultUploader.writeSummary(workConf, results, ps);
            ps.close();
            uploaderFiles.put("summary", nextName);
        }

        DBCollector dbCollector = null;
        if (outputOptions.contains('k')) {
            nextName = FileUtil.getNextFilename(basePath + ".params");
            ps = new PrintStream(new File(nextName));
            LOG.info("Saving DBMS configuration knobs to: " + nextName);
            if (dbCollector == null)
                dbCollector = DBCollector.createCollector(workConf.getDB());
            ps.println(dbCollector.collectParameters());
            ps.close();
            uploaderFiles.put("params", nextName);
        }

        if (outputOptions.contains('m')) {
            nextName = FileUtil.getNextFilename(basePath + ".metrics");
            ps = new PrintStream(new File(nextName));
            LOG.info("Saving DBMS metrics to: " + nextName);
            if (dbCollector == null)
                dbCollector = DBCollector.createCollector(workConf.getDB());
            ps.println(dbCollector.collectMetrics());
            ps.close();
            uploaderFiles.put("metrics", nextName);
        }

        return uploaderFiles;
    }

    /* buggy piece of shit of Java XPath implementation made me do it 
       replaces good old [@bench="{plugin_name}", which doesn't work in Java XPath with lists
     */
    private static List<String> getWeights(String plugin, SubnodeConfiguration work) {

        List<String> weight_strings = new LinkedList<String>();
        @SuppressWarnings("unchecked")
        List<SubnodeConfiguration> weights = work.configurationsAt("weights");
        boolean weights_started = false;

        for (SubnodeConfiguration weight : weights) {

            // stop if second attributed node encountered
            if (weights_started && weight.getRootNode().getAttributeCount() > 0) {
                break;
            }
            // start adding node values, if node with attribute equal to current
            // plugin encountered
            if (weight.getRootNode().getAttributeCount() > 0 && weight.getRootNode().getAttribute(0).getValue().equals(plugin)) {
                weights_started = true;
            }
            if (weights_started) {
                weight_strings.add(weight.getString(""));
            }

        }
        return weight_strings;
    }
    
    private static void runScript(BenchmarkModule bench, String script) {
        LOG.debug(String.format("Running %s", script));
        bench.runScript(script);
    }

    private static void runCreator(BenchmarkModule bench, boolean verbose) {
        LOG.debug(String.format("Creating %s Database", bench));
        bench.createDatabase();
    }
    
    private static void runLoader(BenchmarkModule bench, boolean verbose) {
        LOG.debug(String.format("Loading %s Database", bench));
        bench.loadDatabase();
    }

    private static Results runWorkload(List<BenchmarkModule> benchList, boolean verbose, int intervalMonitor) throws QueueLimitException, IOException {
        List<Worker<?>> workers = new ArrayList<Worker<?>>();
        List<WorkloadConfiguration> workConfs = new ArrayList<WorkloadConfiguration>();
        for (BenchmarkModule bench : benchList) {
            LOG.info("Creating " + bench.getWorkloadConfiguration().getTerminals() + " virtual terminals...");
            workers.addAll(bench.makeWorkers(verbose));
            // LOG.info("done.");
            
            int num_phases = bench.getWorkloadConfiguration().getNumberOfPhases();
            LOG.info(String.format("Launching the %s Benchmark with %s Phase%s...",
                    bench.getBenchmarkName().toUpperCase(), num_phases, (num_phases > 1 ? "s" : "")));
            workConfs.add(bench.getWorkloadConfiguration());
            
        }
        Results r = ThreadBench.runRateLimitedBenchmark(workers, workConfs, intervalMonitor);
        LOG.info(SINGLE_LINE);
        LOG.info("Rate limited reqs/s: " + r);
        return r;
    }

    private static void printUsage(Options options) {
        HelpFormatter hlpfrmt = new HelpFormatter();
        hlpfrmt.printHelp("oltpbenchmark", options);
    }

    /**
     * Returns true if the given key is in the CommandLine object and is set to
     * true.
     * 
     * @param argsLine
     * @param key
     * @return
     */
    private static boolean isBooleanOptionSet(CommandLine argsLine, String key) {
        if (argsLine.hasOption(key)) {
            LOG.debug("CommandLine has option '" + key + "'. Checking whether set to true");
            String val = argsLine.getOptionValue(key);
            LOG.debug(String.format("CommandLine %s => %s", key, val));
            return (val != null ? val.equalsIgnoreCase("true") : false);
        }
        return (false);
    }
    
    public static String getAssertWarning() {
        String msg = "!!! WARNING !!!\n" +
                     "OLTP-Bench is executing with JVM asserts enabled. This will degrade runtime performance.\n" +
                     "You can disable them by setting the config option 'assertions' to FALSE";
        return StringBoxUtil.heavyBox(msg);
    }
}
