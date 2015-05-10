package com.oltpbenchmark.benchpress;
public class DBConfig {

    private String dbms;
    private String benchmark;

    public DBConfig() {
    }

    public DBConfig(String dbms, String benchmark) {
        super();
        this.dbms = dbms;
        this.benchmark = benchmark;
    }

    public String getDbms() {
        return dbms;
    }
    
    public void setDbms(String dbms) {
        this.dbms = dbms;
    }

    public String getBenchmark() {
        return benchmark;
    }
    
    public void setBenchmark(String benchmark) {
        this.benchmark = benchmark;
    }
    
    public boolean isValidDbms(String dbms) {
        return benchmark.equals("mysql") || benchmark.equals("postgres");
    }
    
    public boolean isValidBenchmark(String benchmark) {
        return benchmark.equals("ycsb");
    }
    
    public boolean isValid() {
        return isValidBenchmark(benchmark) && isValidDbms(dbms);
    }
    
    public void setDefaults() {
        benchmark = "ycsb";
        dbms = "mysql";
    }

    @Override
    public String toString() {
        return "\tDBMS: " + dbms + "\n\tBenchmark: " + benchmark + "\n\n";
    }
}
