package com.oltpbenchmark.benchpress;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.ParseException;
import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.SubnodeConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.tree.xpath.XPathExpressionEngine;
import org.apache.log4j.Logger;

import com.oltpbenchmark.Phase;
import com.oltpbenchmark.Results;
import com.oltpbenchmark.ThreadBench;
import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.TransactionTypes;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.util.ClassUtil;
import com.oltpbenchmark.util.QueueLimitException;
import com.oltpbenchmark.util.StringUtil;

public class GameThread extends Thread {
    private static final Logger LOG = Logger.getLogger(GameThread.class);
    private static final String SINGLE_LINE = "**********************************************************************************";
    
    private static final String RATE_DISABLED = "disabled";
    private static final String RATE_UNLIMITED = "unlimited";
    public volatile GameThread thread;
    
    private DBConfig data;
    public final List<ServerCallback> callbacks = new ArrayList<ServerCallback>();
    
    public GameThread(DBConfig data) {
        super();
        this.data = data;
        thread = this;
    }
    
    public void registerCallback(ServerCallback callback) {
        callbacks.add(callback);
    }

    public void stopThread() {
        thread = null;
    }
    
    @Override
    public void run() {
        XMLConfiguration pluginConfig=null;
        try {
            pluginConfig = new XMLConfiguration("config/plugin.xml");
        } catch (ConfigurationException e1) {
            LOG.info("Plugin configuration file config/plugin.xml is missing");
            e1.printStackTrace();
        }
        pluginConfig.setExpressionEngine(new XPathExpressionEngine());
        // -------------------------------------------------------------------
        // GET PLUGIN LIST
        // -------------------------------------------------------------------
        
        //String[] targetList = targetBenchmarks.split(",");
        String[] targetList = {data.getBenchmark()};
        List<BenchmarkModule> benchList = new ArrayList<BenchmarkModule>();
        
        // Use this list for filtering of the output
        List<TransactionType> activeTXTypes = new ArrayList<TransactionType>();
        
        //String configFile = argsLine.getOptionValue("c");
        String configFile = "config/benchpress/" + data.getDbms() + "_" + data.getBenchmark() 
                + "_cfg.xml";
        XMLConfiguration xmlConfig = null;
        try {
            xmlConfig = new XMLConfiguration(configFile);
        } catch (ConfigurationException e2) {
            e2.printStackTrace();
            String defaultConfig = "/usr0/home/dvanaken/oltpbench/config/benchpress/mysql_ycsb_cfg.xml";
            LOG.warn("Unrecognized config file: " + configFile + defaultConfig);
            try {
                xmlConfig = new XMLConfiguration(defaultConfig);
            } catch (ConfigurationException e3) {
                LOG.warn("Unrecoverable configuration error");
                e3.printStackTrace();
                return;
            }
        }
        xmlConfig.setExpressionEngine(new XPathExpressionEngine());

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

            wrkld.setDBType(DatabaseType.get(xmlConfig.getString("dbtype")));
            wrkld.setDBDriver(xmlConfig.getString("driver"));
            wrkld.setDBConnection(xmlConfig.getString("DBUrl"));
            wrkld.setDBName(xmlConfig.getString("DBName"));
            wrkld.setDBUsername(xmlConfig.getString("username"));
            wrkld.setDBPassword(xmlConfig.getString("password"));
            int terminals = xmlConfig.getInt("terminals[not(@bench)]", 0);
            terminals = xmlConfig.getInt("terminals" + pluginTest, terminals);
            wrkld.setTerminals(terminals);
            String isolationMode = xmlConfig.getString("isolation[not(@bench)]", "TRANSACTION_SERIALIZABLE");
            wrkld.setIsolationMode(xmlConfig.getString("isolation" + pluginTest, isolationMode));
            wrkld.setScaleFactor(xmlConfig.getDouble("scalefactor", 1.0));
            wrkld.setRecordAbortMessages(xmlConfig.getBoolean("recordabortmessages", false));
            wrkld.setDataDir(xmlConfig.getString("datadir", "."));

            // ----------------------------------------------------------------
            // CREATE BENCHMARK MODULE
            // ----------------------------------------------------------------

            String classname = pluginConfig.getString("/plugin[@name='" + plugin + "']");

            if (classname == null)
                try {
                    throw new ParseException("Plugin " + plugin + " is undefined in config/plugin.xml");
                } catch (ParseException e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
            BenchmarkModule bench = ClassUtil.newInstance(classname,
                                                          new Object[] { wrkld },
                                                          new Class<?>[] { WorkloadConfiguration.class });
            Map<String, Object> initDebug = new ListOrderedMap<String, Object>();
            initDebug.put("Benchmark", String.format("%s {%s}", plugin.toUpperCase(), classname));
            initDebug.put("Configuration", configFile);
            initDebug.put("Type", wrkld.getDBType());
            initDebug.put("Driver", wrkld.getDBDriver());
            initDebug.put("URL", wrkld.getDBConnection());
            initDebug.put("Isolation", wrkld.getIsolationString());
            initDebug.put("Scale Factor", wrkld.getScaleFactor());
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
            for (int i = 1; i < wrkld.getNumTxnTypes() + 1; i++) {
                String key = "transactiontypes" + pluginTest + "/transactiontype[" + i + "]";
                String txnName = xmlConfig.getString(key + "/name");

                // Get ID if specified; else increment from last one.
                int txnId = i + 1;
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
                    weight_strings = get_weights(plugin, work);
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

                wrkld.addWork(time,
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
                    LOG.fatal(String.format("Configuration files is inconsistent, phase %d"
                            + " contains %d weights but you defined %d transaction types",
                                            j, p.getWeightCount(), wrkld.getNumTxnTypes()));
                    if (p.isSerial()) {
                        LOG.fatal("However, note that since this a serial phase, the weights "
                                + "are irrelevant (but still must be included---sorry).");
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

        boolean verbose = true;

        // Create the Benchmark's Database
        for (BenchmarkModule benchmark : benchList) {
            LOG.info("Creating new " + benchmark.getBenchmarkName().toUpperCase() + " database...");
            runCreator(benchmark, verbose);
            LOG.info("Finished!");
            LOG.info(SINGLE_LINE);
        }

        // Execute Loader
        for (BenchmarkModule benchmark : benchList) {
            LOG.info("Loading data into " + benchmark.getBenchmarkName().toUpperCase() + " database...");
            runLoader(benchmark, verbose);
            LOG.info("Finished!");
            LOG.info(SINGLE_LINE);
        }

        // Execute Workload
        //Results r = null;
        int intervalMonitor = 1;
        boolean dynamic = true;
        try {
            //r = runWorkload(benchList, verbose, intervalMonitor, dynamic);
            runWorkload(benchList, verbose, intervalMonitor, dynamic);
        } catch (Throwable ex) {
            LOG.error("Unexpected error when running benchmarks.", ex);
            ex.printStackTrace();
            return;
            //System.exit(1);
        }
        //assert(r != null);
    }
    
    /* buggy piece of shit of Java XPath implementation made me do it 
    replaces good old [@bench="{plugin_name}", which doesn't work in Java XPath with lists
  */
 private static List<String> get_weights(String plugin, SubnodeConfiguration work) {
         
         List<String> weight_strings = new LinkedList<String>();
         @SuppressWarnings("unchecked")
         List<SubnodeConfiguration> weights = work.configurationsAt("weights");
         boolean weights_started = false;
         
         for (SubnodeConfiguration weight : weights) {
             
             // stop if second attributed node encountered
             if (weights_started && weight.getRootNode().getAttributeCount() > 0) {
                 break;
             }
             //start adding node values, if node with attribute equal to current plugin encountered
             if (weight.getRootNode().getAttributeCount() > 0 && weight.getRootNode().getAttribute(0).getValue().equals(plugin)) {
                 weights_started = true;
             }
             if (weights_started) {
                 weight_strings.add(weight.getString(""));
             }
             
         }
         return weight_strings;
 }
 

 private static Results runWorkload(List<BenchmarkModule> benchList, boolean verbose, int intervalMonitor, boolean dynamic) throws QueueLimitException, IOException {
     List<Worker> workers = new ArrayList<Worker>();
     List<WorkloadConfiguration> workConfs = new ArrayList<WorkloadConfiguration>();
     for (BenchmarkModule bench : benchList) {
         LOG.info("Creating " + bench.getWorkloadConfiguration().getTerminals() + " virtual terminals...");
         workers.addAll(bench.makeWorkers(verbose));
         // LOG.info("done.");
         LOG.info(String.format("Launching the %s Benchmark with %s Phases...",
                 bench.getBenchmarkName(), bench.getWorkloadConfiguration().getNumberOfPhases()));
         workConfs.add(bench.getWorkloadConfiguration());
         
     }
     Results r = ThreadBench.runRateLimitedBenchmark(workers, workConfs, intervalMonitor, dynamic);        
     LOG.info(SINGLE_LINE);
     LOG.info("Rate limited reqs/s: " + r);
     return r;
 }
 
 private static void runCreator(BenchmarkModule bench, boolean verbose) {
     LOG.debug(String.format("Creating %s Database", bench));
     bench.createDatabase();
 }
 
 private static void runLoader(BenchmarkModule bench, boolean verbose) {
     LOG.debug(String.format("Loading %s Database", bench));
     bench.loadDatabase();
 }
}

