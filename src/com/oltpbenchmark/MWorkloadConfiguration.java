package com.oltpbenchmark;

import java.util.List;

import org.apache.commons.configuration.XMLConfiguration;

import com.oltpbenchmark.Phase.Arrival;
import com.oltpbenchmark.api.TransactionTypes;
import com.oltpbenchmark.types.DatabaseType;

public class MWorkloadConfiguration extends WorkloadConfiguration {
    /**
     * Flag for simulating Intrusion Detection System (IDS)
     */
    private boolean simIDS = false;
    private WorkloadConfiguration _nwconf;
    
    
    public MWorkloadConfiguration(WorkloadConfiguration nwconf) {
        super();
        _nwconf = nwconf;
    }

    /**
     * @return true if workload is configured to simulate IDS, false otherwise
     */
    
    public boolean simulateIDS(){
       return simIDS; 
    }
    
    /**
     * 
     * @param enabled
     */
    public void setIDSSimulation(boolean enabled){
        simIDS = enabled;
    }

    /**
     * @return
     * @see java.lang.Object#hashCode()
     */
    public int hashCode() {
        return _nwconf.hashCode();
    }

    /**
     * @return
     * @see com.oltpbenchmark.WorkloadConfiguration#getBenchmarkName()
     */
    public String getBenchmarkName() {
        return _nwconf.getBenchmarkName();
    }

    /**
     * @param benchmarkName
     * @see com.oltpbenchmark.WorkloadConfiguration#setBenchmarkName(java.lang.String)
     */
    public void setBenchmarkName(String benchmarkName) {
        _nwconf.setBenchmarkName(benchmarkName);
    }

    /**
     * @return
     * @see com.oltpbenchmark.WorkloadConfiguration#getTraceReader()
     */
    public TraceReader getTraceReader() {
        return _nwconf.getTraceReader();
    }

    /**
     * @param traceReader
     * @see com.oltpbenchmark.WorkloadConfiguration#setTraceReader(com.oltpbenchmark.TraceReader)
     */
    public void setTraceReader(TraceReader traceReader) {
        _nwconf.setTraceReader(traceReader);
    }

    /**
     * @return
     * @see com.oltpbenchmark.WorkloadConfiguration#getWorkloadState()
     */
    public WorkloadState getWorkloadState() {
        return _nwconf.getWorkloadState();
    }

    /**
     * @param benchmarkState
     * @return
     * @see com.oltpbenchmark.WorkloadConfiguration#initializeState(com.oltpbenchmark.BenchmarkState)
     */
    public WorkloadState initializeState(BenchmarkState benchmarkState) {
        return _nwconf.initializeState(benchmarkState);
    }

    /**
     * @param time
     * @param rate
     * @param weights
     * @param rateLimited
     * @param disabled
     * @param serial
     * @param timed
     * @param active_terminals
     * @param arrival
     * @see com.oltpbenchmark.WorkloadConfiguration#addWork(int, int, java.util.List, boolean, boolean, boolean, boolean, int, com.oltpbenchmark.Phase.Arrival)
     */
    public void addWork(int time, int rate, List<String> weights,
            boolean rateLimited, boolean disabled, boolean serial,
            boolean timed, int active_terminals, Arrival arrival) {
        _nwconf.addWork(time, rate, weights, rateLimited, disabled, serial,
                timed, active_terminals, arrival);
    }

    /**
     * @param obj
     * @return
     * @see java.lang.Object#equals(java.lang.Object)
     */
    public boolean equals(Object obj) {
        return _nwconf.equals(obj);
    }

    /**
     * @param dbType
     * @see com.oltpbenchmark.WorkloadConfiguration#setDBType(com.oltpbenchmark.types.DatabaseType)
     */
    public void setDBType(DatabaseType dbType) {
        _nwconf.setDBType(dbType);
    }

    /**
     * @return
     * @see com.oltpbenchmark.WorkloadConfiguration#getDBType()
     */
    public DatabaseType getDBType() {
        return _nwconf.getDBType();
    }

    /**
     * @param database
     * @see com.oltpbenchmark.WorkloadConfiguration#setDBConnection(java.lang.String)
     */
    public void setDBConnection(String database) {
        _nwconf.setDBConnection(database);
    }

    /**
     * @return
     * @see com.oltpbenchmark.WorkloadConfiguration#getDBConnection()
     */
    public String getDBConnection() {
        return _nwconf.getDBConnection();
    }

    /**
     * @param dbname
     * @see com.oltpbenchmark.WorkloadConfiguration#setDBName(java.lang.String)
     */
    public void setDBName(String dbname) {
        _nwconf.setDBName(dbname);
    }

    /**
     * @return
     * @see com.oltpbenchmark.WorkloadConfiguration#getNumTxnTypes()
     */
    public int getNumTxnTypes() {
        return _nwconf.getNumTxnTypes();
    }

    /**
     * @param numTxnTypes
     * @see com.oltpbenchmark.WorkloadConfiguration#setNumTxnTypes(int)
     */
    public void setNumTxnTypes(int numTxnTypes) {
        _nwconf.setNumTxnTypes(numTxnTypes);
    }

    /**
     * @return
     * @see com.oltpbenchmark.WorkloadConfiguration#getDBName()
     */
    public String getDBName() {
        return _nwconf.getDBName();
    }

    /**
     * @param username
     * @see com.oltpbenchmark.WorkloadConfiguration#setDBUsername(java.lang.String)
     */
    public void setDBUsername(String username) {
        _nwconf.setDBUsername(username);
    }

    /**
     * @return
     * @see com.oltpbenchmark.WorkloadConfiguration#getDBUsername()
     */
    public String getDBUsername() {
        return _nwconf.getDBUsername();
    }

    /**
     * @param password
     * @see com.oltpbenchmark.WorkloadConfiguration#setDBPassword(java.lang.String)
     */
    public void setDBPassword(String password) {
        _nwconf.setDBPassword(password);
    }

    /**
     * @return
     * @see com.oltpbenchmark.WorkloadConfiguration#getDBPassword()
     */
    public String getDBPassword() {
        return _nwconf.getDBPassword();
    }

    /**
     * @param driver
     * @see com.oltpbenchmark.WorkloadConfiguration#setDBDriver(java.lang.String)
     */
    public void setDBDriver(String driver) {
        _nwconf.setDBDriver(driver);
    }

    /**
     * @return
     * @see com.oltpbenchmark.WorkloadConfiguration#getDBDriver()
     */
    public String getDBDriver() {
        return _nwconf.getDBDriver();
    }

    /**
     * @param recordAbortMessages
     * @see com.oltpbenchmark.WorkloadConfiguration#setRecordAbortMessages(boolean)
     */
    public void setRecordAbortMessages(boolean recordAbortMessages) {
        _nwconf.setRecordAbortMessages(recordAbortMessages);
    }

    /**
     * @return
     * @see com.oltpbenchmark.WorkloadConfiguration#getRecordAbortMessages()
     */
    public boolean getRecordAbortMessages() {
        return _nwconf.getRecordAbortMessages();
    }

    /**
     * @param scaleFactor
     * @see com.oltpbenchmark.WorkloadConfiguration#setScaleFactor(double)
     */
    public void setScaleFactor(double scaleFactor) {
        _nwconf.setScaleFactor(scaleFactor);
    }

    /**
     * @return
     * @see com.oltpbenchmark.WorkloadConfiguration#getScaleFactor()
     */
    public double getScaleFactor() {
        return _nwconf.getScaleFactor();
    }

    /**
     * @return
     * @see com.oltpbenchmark.WorkloadConfiguration#getNumberOfPhases()
     */
    public int getNumberOfPhases() {
        return _nwconf.getNumberOfPhases();
    }

    /**
     * @param dir
     * @see com.oltpbenchmark.WorkloadConfiguration#setDataDir(java.lang.String)
     */
    public void setDataDir(String dir) {
        _nwconf.setDataDir(dir);
    }

    /**
     * @return
     * @see com.oltpbenchmark.WorkloadConfiguration#getDataDir()
     */
    public String getDataDir() {
        return _nwconf.getDataDir();
    }

    /**
     * 
     * @see com.oltpbenchmark.WorkloadConfiguration#init()
     */
    public void init() {
        _nwconf.init();
    }

    /**
     * @param terminals
     * @see com.oltpbenchmark.WorkloadConfiguration#setTerminals(int)
     */
    public void setTerminals(int terminals) {
        _nwconf.setTerminals(terminals);
    }

    /**
     * @return
     * @see com.oltpbenchmark.WorkloadConfiguration#getTerminals()
     */
    public int getTerminals() {
        return _nwconf.getTerminals();
    }

    /**
     * @return
     * @see com.oltpbenchmark.WorkloadConfiguration#getTransTypes()
     */
    public TransactionTypes getTransTypes() {
        return _nwconf.getTransTypes();
    }

    /**
     * @param transTypes
     * @see com.oltpbenchmark.WorkloadConfiguration#setTransTypes(com.oltpbenchmark.api.TransactionTypes)
     */
    public void setTransTypes(TransactionTypes transTypes) {
        _nwconf.setTransTypes(transTypes);
    }

    /**
     * @return
     * @see com.oltpbenchmark.WorkloadConfiguration#getAllPhases()
     */
    public List<Phase> getAllPhases() {
        return _nwconf.getAllPhases();
    }

    /**
     * @param xmlConfig
     * @see com.oltpbenchmark.WorkloadConfiguration#setXmlConfig(org.apache.commons.configuration.XMLConfiguration)
     */
    public void setXmlConfig(XMLConfiguration xmlConfig) {
        _nwconf.setXmlConfig(xmlConfig);
    }

    /**
     * @return
     * @see com.oltpbenchmark.WorkloadConfiguration#getXmlConfig()
     */
    public XMLConfiguration getXmlConfig() {
        return _nwconf.getXmlConfig();
    }

    /**
     * @return
     * @see com.oltpbenchmark.WorkloadConfiguration#getIsolationMode()
     */
    public int getIsolationMode() {
        return _nwconf.getIsolationMode();
    }

    /**
     * @return
     * @see com.oltpbenchmark.WorkloadConfiguration#getIsolationString()
     */
    public String getIsolationString() {
        return _nwconf.getIsolationString();
    }

    /**
     * @param mode
     * @see com.oltpbenchmark.WorkloadConfiguration#setIsolationMode(java.lang.String)
     */
    public void setIsolationMode(String mode) {
        _nwconf.setIsolationMode(mode);
    }

    /**
     * @return
     * @see com.oltpbenchmark.WorkloadConfiguration#toString()
     */
    public String toString() {
        return _nwconf.toString();
    }
    
    
}
