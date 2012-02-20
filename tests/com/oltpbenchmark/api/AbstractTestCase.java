package com.oltpbenchmark.api;

import java.io.File;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import junit.framework.TestCase;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.catalog.Catalog;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.util.ClassUtil;

public abstract class AbstractTestCase<T extends BenchmarkModule> extends TestCase {
    
    // HACK
    static {
//      org.apache.log4j.PropertyConfigurator.configure("/home/pavlo/Documents/OLTPBenchmark/OLTPBenchmark/log4j.properties");
    }
    
    // -----------------------------------------------------------------
    
    // HSQLDB
    public static final String DB_CONNECTION = "jdbc:hsqldb:mem:";
    public static final String DB_JDBC = "org.hsqldb.jdbcDriver";
    public static final DatabaseType DB_TYPE = DatabaseType.HSQLDB;
    
    // H2
    // public static final String DB_CONNECTION = "jdbc:h2:mem:";
    // public static final String DB_JDBC = "org.h2.Driver";
    // public static final DatabaseType DB_TYPE = DatabaseType.H2;
    
    // SQLITE
    // public static final String DB_CONNECTION = "jdbc:sqlite:/tmp/";
    // public static final String DB_JDBC = "org.sqlite.JDBC";
    // public static final DatabaseType DB_TYPE = DatabaseType.SQLITE;
    
    // -----------------------------------------------------------------
    
    protected static final double DB_SCALE_FACTOR = 0.01;

    protected String dbName;
    protected WorkloadConfiguration workConf;
    protected T benchmark;
    protected Catalog catalog;
    protected Connection conn;
    protected List<Class<? extends Procedure>> procClasses = new ArrayList<Class<? extends Procedure>>();

    @SuppressWarnings({ "rawtypes", "unchecked" })
    protected void setUp(Class<T> clazz, Class...procClasses) throws Exception {
        super.setUp();
        Class.forName(DB_JDBC);
        
        this.workConf = new WorkloadConfiguration();
        TransactionTypes txnTypes = new TransactionTypes();
        for (int i = 0; i < procClasses.length; i++) {
            assertFalse("Duplicate Procedure '" + procClasses[i] + "'",
                        this.procClasses.contains(procClasses[i]));
            this.procClasses.add(procClasses[i]);
            TransactionType tt = new TransactionType(procClasses[i], i);
            txnTypes.add(tt);
        } // FOR
        
        this.dbName = String.format("%s-%d.db", clazz.getSimpleName(), new Random().nextInt());
        this.workConf.setTransTypes(txnTypes);
        this.workConf.setDBType(DB_TYPE);
        this.workConf.setDBConnection(DB_CONNECTION + this.dbName);
        this.workConf.setScaleFactor(DB_SCALE_FACTOR);
        
        this.benchmark = (T) ClassUtil.newInstance(clazz,
                                                   new Object[] { this.workConf },
                                                   new Class<?>[] { WorkloadConfiguration.class });
        assertNotNull(this.benchmark);
        System.err.println(this.benchmark + " -> " + this.dbName);
        
        this.catalog = this.benchmark.getCatalog();
        assertNotNull(this.catalog);
        this.conn = this.benchmark.makeConnection();
        assertNotNull(this.conn);
    }
    
    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        File f = new File(this.dbName);
        if (f.exists()) {
            f.delete();
        }
    }
}
