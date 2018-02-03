
package com.oltpbenchmark;

import com.oltpbenchmark.Phase.Arrival;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.TransactionTypes;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.util.*;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.commons.configuration.SubnodeConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.SQLException;
import java.util.*;

public class OLTPBenchMethods {
    private static final String RATE_DISABLED = "disabled";
    private static final String RATE_UNLIMITED = "unlimited";
    public static final Logger LOG = Logger.getLogger(OLTPBench.class);
    public static final String SINGLE_LINE = StringUtil.repeat("=", 70);

    public void writeHistograms(Results r) {
        StringBuilder sb = new StringBuilder();
        sb.append(StringUtil.bold("Completed Transactions:")).append("\n").append(r.getTransactionSuccessHistogram()).append("\n\n");
        sb.append(StringUtil.bold("Aborted Transactions:")).append("\n").append(r.getTransactionAbortHistogram()).append("\n\n");
        sb.append(StringUtil.bold("Rejected Transactions (Server Retry):")).append("\n").append(r.getTransactionRetryHistogram()).append("\n\n");
        sb.append(StringUtil.bold("Unexpected Errors:")).append("\n").append(r.getTransactionErrorHistogram());
        if (!r.getTransactionAbortMessageHistogram().isEmpty()) {
            sb.append("\n\n").append(StringUtil.bold("User Aborts:")).append("\n").append(r.getTransactionAbortMessageHistogram());
        }

        LOG.info(SINGLE_LINE);
        LOG.info("Workload Histograms:\n" + sb.toString());
        LOG.info(SINGLE_LINE);
    }

    public void writeOutputs(Results r, List<TransactionType> activeTXTypes, CommandLine argsLine, XMLConfiguration xmlConfig) throws Exception {
        String outputDirectory = "results";
        if (argsLine.hasOption("d")) {
            outputDirectory = argsLine.getOptionValue("d");
        }

        String filePrefix = "";
        if (argsLine.hasOption("t")) {
            filePrefix = TimeUtil.getCurrentTime().getTime() + "_";
        }

        ResultUploader ru = null;
        if (xmlConfig.containsKey("uploadUrl")) {
            ru = new ResultUploader(r, xmlConfig, argsLine);
            LOG.info("Upload Results URL: " + ru);
        }

        PrintStream ps = null;
        PrintStream rs = null;
        String baseFileName = "oltpbench";
        if (argsLine.hasOption("o")) {
            if (argsLine.getOptionValue("o") == "-") {
                ps = System.out;
                rs = System.out;
                baseFileName = null;
            } else {
                baseFileName = argsLine.getOptionValue("o");
            }
        }

        String baseFile = filePrefix;
        String nextName;
        if (baseFileName != null) {
            if (outputDirectory.length() > 0) {
                FileUtil.makeDirIfNotExists(outputDirectory.split("/"));
            }

            baseFile = filePrefix + baseFileName;
            nextName = FileUtil.getNextFilename(FileUtil.joinPath(new String[]{outputDirectory, baseFile + ".csv"}));
            rs = new PrintStream(new File(nextName));
            LOG.info("Output Raw data into file: " + nextName);
            r.writeAllCSVAbsoluteTiming(activeTXTypes, rs);
            if (ru != null) {
                nextName = FileUtil.getNextFilename(FileUtil.joinPath(new String[]{outputDirectory, baseFile + ".summary"}));
                PrintStream ss = new PrintStream(new File(nextName));
                LOG.info("Output summary data into file: " + nextName);
                ru.writeSummary(ss);
                ss.close();
                nextName = FileUtil.getNextFilename(FileUtil.joinPath(new String[]{outputDirectory, baseFile + ".params"}));
                ss = new PrintStream(new File(nextName));
                LOG.info("Output DBMS parameters into file: " + nextName);
                ru.writeDBParameters(ss);
                ss.close();
                nextName = FileUtil.getNextFilename(FileUtil.joinPath(new String[]{outputDirectory, baseFile + ".metrics"}));
                ss = new PrintStream(new File(nextName));
                LOG.info("Output DBMS metrics into file: " + nextName);
                ru.writeDBMetrics(ss);
                ss.close();
                nextName = FileUtil.getNextFilename(FileUtil.joinPath(new String[]{outputDirectory, baseFile + ".expconfig"}));
                ss = new PrintStream(new File(nextName));
                LOG.info("Output experiment config into file: " + nextName);
                ru.writeBenchmarkConf(ss);
                ss.close();
                nextName = FileUtil.getNextFilename(FileUtil.joinPath(new String[]{outputDirectory, baseFile + ".samples"}));
                ss = new PrintStream(new File(nextName));
                LOG.info("Output samples into file: " + nextName);
                r.writeCSV2(ss);
                ss.close();
            }
        } else if (LOG.isDebugEnabled()) {
            LOG.debug("No output file specified");
        }

        if (this.isBooleanOptionSet(argsLine, "upload") && ru != null) {
            ru.uploadResult(activeTXTypes);
        }

        if (argsLine.hasOption("s")) {
            nextName = FileUtil.getNextFilename(FileUtil.joinPath(new String[]{outputDirectory, baseFile + ".res"}));
            ps = new PrintStream(new File(nextName));
            LOG.info("Output into file: " + nextName);
            int windowSize = Integer.parseInt(argsLine.getOptionValue("s"));
            LOG.info("Grouped into Buckets of " + windowSize + " seconds");
            r.writeCSV(windowSize, ps);
            if (argsLine.hasOption("ss")) {
                Iterator var14 = activeTXTypes.iterator();

                while (var14.hasNext()) {
                    TransactionType t = (TransactionType) var14.next();
                    if (ps != System.out) {
                        baseFile = filePrefix + baseFileName + "_" + t.getName();
                        nextName = FileUtil.getNextFilename(FileUtil.joinPath(new String[]{outputDirectory, baseFile + ".res"}));
                        PrintStream ts = new PrintStream(new File(nextName));
                        r.writeCSV(windowSize, ts, t);
                        ts.close();
                    }
                }
            }
        } else if (LOG.isDebugEnabled()) {
            LOG.warn("No bucket size specified");
        }

        if (ps != null) {
            ps.close();
        }

        if (rs != null) {
            rs.close();
        }

    }

    public List<String> getWeights(String plugin, SubnodeConfiguration work) {
        List<String> weight_strings = new LinkedList();
        List<SubnodeConfiguration> weights = work.configurationsAt("weights");
        boolean weights_started = false;
        Iterator var6 = weights.iterator();

        while (var6.hasNext()) {
            SubnodeConfiguration weight = (SubnodeConfiguration) var6.next();
            if (weights_started && weight.getRootNode().getAttributeCount() > 0) {
                break;
            }

            if (weight.getRootNode().getAttributeCount() > 0 && weight.getRootNode().getAttribute(0).getValue().equals(plugin)) {
                weights_started = true;
            }

            if (weights_started) {
                weight_strings.add(weight.getString(""));
            }
        }

        return weight_strings;
    }

    public void runScript(BenchmarkModule bench, String script) {
        LOG.debug(String.format("Running %s", script));
        bench.runScript(script);
    }

    public void runCreator(BenchmarkModule bench, boolean verbose) {
        LOG.debug(String.format("Creating %s Database", bench));
        bench.createDatabase();
    }

    public void runLoader(BenchmarkModule bench, boolean verbose) {
        LOG.debug(String.format("Loading %s Database", bench));
        bench.loadDatabase();
    }

    public Results runWorkload(List<BenchmarkModule> benchList, boolean verbose, int intervalMonitor) throws QueueLimitException, IOException {
        List<Worker<?>> workers = new ArrayList();
        List<WorkloadConfiguration> workConfs = new ArrayList();
        Iterator var6 = benchList.iterator();

        while (var6.hasNext()) {
            BenchmarkModule bench = (BenchmarkModule) var6.next();
            LOG.info("Creating " + bench.getWorkloadConfiguration().getTerminals() + " virtual terminals...");
            workers.addAll(bench.makeWorkers(verbose));
            int num_phases = bench.getWorkloadConfiguration().getNumberOfPhases();
            LOG.info(String.format("Launching the %s Benchmark with %s Phase%s...", bench.getBenchmarkName().toUpperCase(), num_phases, num_phases > 1 ? "s" : ""));
            workConfs.add(bench.getWorkloadConfiguration());
        }

        Results r = ThreadBench.runRateLimitedBenchmark(workers, workConfs, intervalMonitor);
        LOG.info(SINGLE_LINE);
        LOG.info("Rate limited reqs/s: " + r);
        return r;
    }

    public void printUsage(Options options) {
        HelpFormatter hlpfrmt = new HelpFormatter();
        hlpfrmt.printHelp("oltpbenchmark", options);
    }

    public boolean isBooleanOptionSet(CommandLine argsLine, String key) {
        if (argsLine.hasOption(key)) {
            LOG.debug("CommandLine has option '" + key + "'. Checking whether set to true");
            String val = argsLine.getOptionValue(key);
            LOG.debug(String.format("CommandLine %s => %s", key, val));
            return val != null ? val.equalsIgnoreCase("true") : false;
        } else {
            return false;
        }
    }

    public String getAssertWarning() {
        String msg = "!!! WARNING !!!\nOLTP-Bench is executing with JVM asserts enabled. This will degrade runtime performance.\nYou can disable them by setting the config option 'assertions' to FALSE";
        return StringBoxUtil.heavyBox(msg);
    }

    public String pluginConfigBenchmark(String[] targetList, XMLConfiguration xmlConfig, CommandLine argsLine, XMLConfiguration pluginConfig, String configFile, List<TransactionType> activeTXTypes, List<BenchmarkModule> benchList) throws ParseException, SQLException {
        OLTPBenchMethods oltpMethods = new OLTPBenchMethods();
        int lastTxnId = 0;
        String[] var10 = targetList;
        int var11 = targetList.length;

        for (int var12 = 0; var12 < var11; ++var12) {
            String plugin = var10[var12];
            String pluginTest = "[@bench='" + plugin + "']";
            WorkloadConfiguration wrkld = new WorkloadConfiguration();
            wrkld.setBenchmarkName(plugin);
            wrkld.setXmlConfig(xmlConfig);
            boolean scriptRun = false;
            if (argsLine.hasOption("t")) {
                scriptRun = true;
                String traceFile = argsLine.getOptionValue("t");
                wrkld.setTraceReader(new TraceReader(traceFile));
                if (LOG.isDebugEnabled()) {
                    LOG.debug(wrkld.getTraceReader().toString());
                }
            }

            wrkld.setDBType(DatabaseType.get(xmlConfig.getString("dbtype")));
            wrkld.setDBDriver(xmlConfig.getString("driver"));
            wrkld.setDBConnection(xmlConfig.getString("DBUrl"));
            wrkld.setDBName(xmlConfig.getString("DBName"));
            wrkld.setDBUsername(xmlConfig.getString("username"));
            wrkld.setDBPassword(xmlConfig.getString("password"));
            int terminals = xmlConfig.getInt("terminals[not(@bench)]", 0);
            terminals = xmlConfig.getInt("terminals" + pluginTest, terminals);
            wrkld.setTerminals(terminals);
            if (xmlConfig.containsKey("loaderThreads")) {
                int loaderThreads = xmlConfig.getInt("loaderThreads");
                wrkld.setLoaderThreads(loaderThreads);
            }

            String isolationMode = xmlConfig.getString("isolation[not(@bench)]", "TRANSACTION_SERIALIZABLE");
            wrkld.setIsolationMode(xmlConfig.getString("isolation" + pluginTest, isolationMode));
            wrkld.setScaleFactor(xmlConfig.getDouble("scalefactor", 1.0D));
            wrkld.setRecordAbortMessages(xmlConfig.getBoolean("recordabortmessages", false));
            wrkld.setDataDir(xmlConfig.getString("datadir", "."));
            double selectivity = -1.0D;

            try {
                selectivity = xmlConfig.getDouble("selectivity");
                wrkld.setSelectivity(selectivity);
            } catch (NoSuchElementException var48) {
                ;
            }

            String classname = pluginConfig.getString("/plugin[@name='" + plugin + "']");
            if (classname == null) {
                throw new ParseException("Plugin " + plugin + " is undefined in config/plugin.xml");
            }

            BenchmarkModule bench = (BenchmarkModule) ClassUtil.newInstance(classname, new Object[]{wrkld}, new Class[]{WorkloadConfiguration.class});
            Map<String, Object> initDebug = new ListOrderedMap();
            initDebug.put("Benchmark", String.format("%s {%s}", plugin.toUpperCase(), classname));
            initDebug.put("Configuration", configFile);
            initDebug.put("Type", wrkld.getDBType());
            initDebug.put("Driver", wrkld.getDBDriver());
            initDebug.put("URL", wrkld.getDBConnection());
            initDebug.put("Isolation", wrkld.getIsolationString());
            initDebug.put("Scale Factor", wrkld.getScaleFactor());
            if (selectivity != -1.0D) {
                initDebug.put("Selectivity", selectivity);
            }

            LOG.info(SINGLE_LINE + "\n\n" + StringUtil.formatMaps(new Map[]{initDebug}));
            LOG.info(SINGLE_LINE);
            int numTxnTypes = xmlConfig.configurationsAt("transactiontypes" + pluginTest + "/transactiontype").size();
            if (numTxnTypes == 0 && targetList.length == 1) {
                pluginTest = "[not(@bench)]";
                numTxnTypes = xmlConfig.configurationsAt("transactiontypes" + pluginTest + "/transactiontype").size();
            }

            wrkld.setNumTxnTypes(numTxnTypes);
            List<TransactionType> ttypes = new ArrayList();
            ttypes.add(TransactionType.INVALID);
            int txnIdOffset = lastTxnId;

            /* int i;*/
            for (int i = 1; i <= wrkld.getNumTxnTypes(); lastTxnId = i++) {
                String key = "transactiontypes" + pluginTest + "/transactiontype[" + i + "]";
                String txnName = xmlConfig.getString(key + "/name");
                i = i;
                if (xmlConfig.containsKey(key + "/id")) {
                    i = xmlConfig.getInt(key + "/id");
                }

                TransactionType tmpType = bench.initTransactionType(txnName, i + txnIdOffset);
                activeTXTypes.add(tmpType);
                ttypes.add(tmpType);
            }

            TransactionTypes tt = new TransactionTypes(ttypes);
            wrkld.setTransTypes(tt);
            LOG.debug("Using the following transaction types: " + tt);
            HashMap<String, List<String>> groupings = new HashMap();
            int numGroupings = xmlConfig.configurationsAt("transactiontypes" + pluginTest + "/groupings/grouping").size();
            LOG.debug("Num groupings: " + numGroupings);

            for (int i = 1; i < numGroupings + 1; ++i) {
                String key = "transactiontypes" + pluginTest + "/groupings/grouping[" + i + "]";
                String groupingName = xmlConfig.getString(key + "/name").toLowerCase();
                if (!groupingName.matches("^[a-z]\\w*$")) {
                    LOG.fatal(String.format("Grouping name \"%s\" is invalid. Must begin with a letter and contain only alphanumeric characters.", groupingName));
                    System.exit(-1);
                } else if (groupingName.equals("all")) {
                    LOG.fatal("Grouping name \"all\" is reserved. Please pick a different name.");
                    System.exit(-1);
                }

                List<String> groupingWeights = xmlConfig.getList(key + "/weights");
                if (groupingWeights.size() != numTxnTypes) {
                    LOG.fatal(String.format("Grouping \"%s\" has %d weights, but there are %d transactions in this benchmark.", groupingName, groupingWeights.size(), numTxnTypes));
                    System.exit(-1);
                }

                LOG.debug("Creating grouping with name, weights: " + groupingName + ", " + groupingWeights);
                groupings.put(groupingName, groupingWeights);
            }

            List<String> weightAll = new ArrayList();

            int size;
            for (size = 0; size < numTxnTypes; ++size) {
                weightAll.add("1");
            }

            groupings.put("all", weightAll);
            benchList.add(bench);
            size = xmlConfig.configurationsAt("/works/work").size();

            /*int i;*/
            for (int i = 1; i < size + 1; ++i) {
                SubnodeConfiguration work = xmlConfig.configurationAt("works/work[" + i + "]");
                List weight_strings;
                String weightKey;
                if (targetList.length <= 1 && !work.containsKey("weights[@bench]")) {
                    weightKey = work.getString("weights[not(@bench)]").toLowerCase();
                    if (groupings.containsKey(weightKey)) {
                        weight_strings = (List) groupings.get(weightKey);
                    } else {
                        weight_strings = work.getList("weights[not(@bench)]");
                    }
                } else {
                    weightKey = work.getString("weights" + pluginTest).toLowerCase();
                    if (groupings.containsKey(weightKey)) {
                        weight_strings = (List) groupings.get(weightKey);
                    } else {
                        weight_strings = oltpMethods.getWeights(plugin, work);
                    }
                }

                int rate = 1;
                boolean rateLimited = true;
                boolean disabled = false;
                boolean serial = false;
                boolean timed = false;
                String rate_string = work.getString("rate[not(@bench)]", "");
                rate_string = work.getString("rate" + pluginTest, rate_string);
                if (rate_string.equals("disabled")) {
                    disabled = true;
                } else if (rate_string.equals("unlimited")) {
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
                    } catch (NumberFormatException var47) {
                        LOG.fatal(String.format("Rate string must be '%s', '%s' or a number", "disabled", "unlimited"));
                        System.exit(-1);
                    }
                }

                Arrival arrival = Arrival.REGULAR;
                String arrive = work.getString("@arrival", "regular");
                if (arrive.toUpperCase().equals("POISSON")) {
                    arrival = Arrival.POISSON;
                }

                String serial_string = work.getString("serial", "false");
                if (serial_string.equals("true")) {
                    serial = true;
                } else if (serial_string.equals("false")) {
                    serial = false;
                } else {
                    LOG.fatal("Serial string should be either 'true' or 'false'.");
                    System.exit(-1);
                }

                serial = serial && wrkld.getTraceReader() == null;
                int activeTerminals = work.getInt("active_terminals[not(@bench)]", terminals);
                activeTerminals = work.getInt("active_terminals" + pluginTest, activeTerminals);
                if (serial && activeTerminals != 1) {
                    LOG.warn("Serial ordering is enabled, so # of active terminals is clamped to 1.");
                    activeTerminals = 1;
                }

                if (activeTerminals > terminals) {
                    LOG.error(String.format("Configuration error in work %d: Number of active terminals is bigger than the total number of terminals", i));
                    System.exit(-1);
                }

                int time = work.getInt("/time", 0);
                int warmup = work.getInt("/warmup", 0);
                timed = time > 0;
                if (scriptRun) {
                    LOG.info("Running a script; ignoring timer, serial, and weight settings.");
                } else if (!timed) {
                    if (serial) {
                        LOG.info("Timer disabled for serial run; will execute all queries exactly once.");
                    } else {
                        LOG.fatal("Must provide positive time bound for non-serial executions. Either provide a valid time or enable serial mode.");
                        System.exit(-1);
                    }
                } else if (serial) {
                    LOG.info("Timer enabled for serial run; will run queries serially in a loop until the timer expires.");
                }

                if (warmup < 0) {
                    LOG.fatal("Must provide nonnegative time bound for warmup.");
                    System.exit(-1);
                }

                wrkld.addWork(time, warmup, rate, weight_strings, rateLimited, disabled, serial, timed, activeTerminals, arrival);
            }

            int i = 0;
            Iterator var59 = wrkld.getAllPhases().iterator();

            while (var59.hasNext()) {
                Phase p = (Phase) var59.next();
                ++i;
                if (p.getWeightCount() != wrkld.getNumTxnTypes()) {
                    LOG.fatal(String.format("Configuration files is inconsistent, phase %d contains %d weights but you defined %d transaction types", i, p.getWeightCount(), wrkld.getNumTxnTypes()));
                    if (p.isSerial()) {
                        LOG.fatal("However, note that since this a serial phase, the weights are irrelevant (but still must be included---sorry).");
                    }

                    System.exit(-1);
                }
            }

            wrkld.init();

            assert wrkld.getNumTxnTypes() >= 0;

            assert xmlConfig != null;
        }

        return configFile;
    }
}

