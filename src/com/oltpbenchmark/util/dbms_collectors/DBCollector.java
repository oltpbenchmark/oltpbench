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

package com.oltpbenchmark.util.dbms_collectors;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import com.oltpbenchmark.catalog.Catalog;
import com.oltpbenchmark.util.JSONSerializable;
import com.oltpbenchmark.util.ResultObject.DBCollection;
import com.oltpbenchmark.util.ResultObject.DBEntry;

class DBCollector implements DBParameterCollector {
    
    public static final class VersionInfo {
        String version = "";
        String osName = "";
        String architecture = "";
        
        Map<String, String> toMap() {
            Map<String, String> result = new TreeMap<String, String>();
            result.put("version", this.version);
            result.put("os_name", this.osName);
            result.put("architecture", this.architecture);
            return result;
        }
    }
    
    
    public enum MapKeys {
        VARNAMES ("variable_names"),
        VARVALS ("variable_values"),
        GLOBAL ("global"),
        DATABASE ("database"),
        TABLE ("table"),
        INDEX ("index"),
        GLBNAME ("global_name"),
        GLBSTATS ("global_stats"),
        GLBVARS ("global_variables"),
        DATNAME ("database_name"),
        DATSTATS ("database_stats"),
        TABNAME ("table_name"),
        TABSTATS ("table_stats"),
        IDXNAME ("index_name"),
        IDXSTATS ("index_stats");
        
        public String key;
        
        private MapKeys(String key) {
            this.key = key;
        }
        
        @Override
        public String toString() {
            return key;
        }
    }
        
    private static final Logger LOG = Logger.getLogger(DBCollector.class);
    private static final int MAX_ATTEMPTS = 10;
    
    protected final DBCollection dbParams;
    protected final DBCollection dbStats;
    protected final Map<String, Map<String, String>> dbIndexStats;
    
    protected final Set<String> tableNames;
    protected final VersionInfo versionInfo;
    protected String databaseName;
    protected String isolationLevel;
    
    
    public DBCollector() {
        this.dbParams = new DBCollection();
        this.dbStats = new DBCollection();
        this.dbIndexStats = new TreeMap<String, Map<String, String>>();
        this.tableNames = new TreeSet<String>();
        this.versionInfo = new VersionInfo();
        this.databaseName = null;
    }
    
    public void collect(String oriDBUrl, String username, String password) {
        Connection conn = null;
        int failedAttempts = 0;
        while (conn == null) {
            try {
                conn = DriverManager.getConnection(oriDBUrl, username, password);
                Catalog.setSeparator(conn);
            } catch (SQLException e) {
                failedAttempts++;
                if (failedAttempts < MAX_ATTEMPTS) {
                    printSQLException(e, true);
                } else {
                    printSQLException(e, false);
                }
            }
        }
        
        assert(conn != null);
        
        try {
            getDatabaseName(conn);
            
            getIsolationLevel(conn);
            
            getVersionInfo(conn);
            
            getTables(conn);
        
            getGlobalParameters(conn);
            
            getGlobalStats(conn);
            
            getDatabaseStats(conn);
            
            getTableStats(conn);
            
            getIndexStats(conn);
            
        } catch(SQLException e) {
            printSQLException(e, false);
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                printSQLException(e, true);
            }
        }
    }
    
    protected void getDatabaseName(Connection conn) throws SQLException {
        try {
            this.databaseName = conn.getCatalog().toLowerCase();
        } catch(SQLException e) {
            printSQLException(e, true);
            this.databaseName = "";
        }
    }
    
    protected void getVersionInfo(Connection conn) throws SQLException {
        try {
            DatabaseMetaData metadata = conn.getMetaData();
            this.versionInfo.version = metadata.
                    getDatabaseProductVersion().toLowerCase();
        } catch(SQLException e) {
            printSQLException(e, true);
        }
    }
    
    protected void getIsolationLevel(Connection conn) throws SQLException {
        try {
            int isolation = conn.getTransactionIsolation();
            switch(isolation) {
                case Connection.TRANSACTION_SERIALIZABLE:
                    this.isolationLevel = "serializable";
                    break;
                case Connection.TRANSACTION_REPEATABLE_READ:
                    this.isolationLevel = "repeatable_read";
                    break;
                case Connection.TRANSACTION_READ_COMMITTED:
                    this.isolationLevel = "read_committed";
                    break;
                case Connection.TRANSACTION_READ_UNCOMMITTED:
                    this.isolationLevel = "read_uncommitted";
                    break;
                case Connection.TRANSACTION_NONE:
                default:
                    this.isolationLevel = "none";
            }
        } catch(SQLException e) {
            printSQLException(e, true);
            this.isolationLevel = "";
        } 
    }
    
    protected void getTables(Connection conn) throws SQLException {
        try {
            DatabaseMetaData metadata = conn.getMetaData();
            String[] types = {"TABLE"};
            ResultSet rs = metadata.getTables(null, null, "%", types);
            while (rs.next()) {
                this.tableNames.add(rs.getString("table_name").toLowerCase());
            }
            assert(tableNames.size() > 0);
        } catch(SQLException e) {
            printSQLException(e, true);
        }
    }
    
    protected void getDatabaseVersionInfo(Connection conn) throws SQLException { }
    
    protected void getGlobalParameters(Connection conn) throws SQLException {
        dbParams.add(new DBEntry(MapKeys.GLOBAL.toString(), Collections.EMPTY_LIST));
    }
    
    protected void getGlobalStats(Connection conn) throws SQLException {
        dbStats.add(new DBEntry(MapKeys.GLOBAL.toString(), Collections.EMPTY_LIST));
    }
    
    protected void getDatabaseStats(Connection conn) throws SQLException {
        dbStats.add(new DBEntry(MapKeys.DATABASE.toString(), Collections.EMPTY_LIST));
    }
    
    protected void getTableStats(Connection conn) throws SQLException {
        dbStats.add(new DBEntry(MapKeys.TABLE.toString(), Collections.EMPTY_LIST));
    }
    
    protected void getIndexStats(Connection conn) throws SQLException {
        dbStats.add(new DBEntry(MapKeys.INDEX.toString(), Collections.EMPTY_LIST));
    }
    
    @Override
    public JSONSerializable collectConfigParameters() {
        return dbParams;
    }
    
    @Override
    public JSONSerializable collectStats() {
        return dbStats;
    }
    
    @Override
    public String collectVersion() {
        return this.versionInfo.version;
    }

    @Override
    public String collectOSName() {
        return this.versionInfo.osName;
    }

    @Override
    public String collectArchitecture() {
        return this.versionInfo.architecture;
    }
    
    @Override
    public String collectDatabaseName() {
        return this.databaseName;
    }
    
    @Override
    public String collectIsolationLevel() {
        return this.isolationLevel;
    }
    
    protected static void resultHelper(ResultSet out, 
            Map<String, Map<String, String>> destMap, 
            String columnKeyName,
            boolean validateKeys) throws SQLException {
        ResultSetMetaData metadata;
        metadata = out.getMetaData();
        int numColumns = metadata.getColumnCount();
        while(out.next()) {
            String outerKey = out.getString(columnKeyName).toLowerCase();
            Map<String, String> rowMap = new TreeMap<String, String>();
            for (int i = 1; i <= numColumns; ++i) {
                String value = out.getString(i) == null ? "" : out.getString(i);
                String key = metadata.getColumnName(i).toLowerCase();
                String oldValue = rowMap.put(key, value);
                if (validateKeys && oldValue != null && !oldValue.equals(value)) {
                    throw new IllegalArgumentException("Value overwritten."
                            + "Two entries with the same key and different"
                            + "values (" + value + ", " + oldValue + ")");
                }
            }
            destMap.put(outerKey, rowMap);
        }
    }
    
    protected static void resultHelper(ResultSet out, 
            Map<String, String> destMap,
            boolean validateKeys) throws SQLException {

        ResultSetMetaData metadata;
        metadata = out.getMetaData();
        int numColumns = metadata.getColumnCount();
        //while(out.next()) {
        for (int i = 1; i <= numColumns; ++i) {
            String value = out.getString(i) == null ? "" : out.getString(i);
            String key = metadata.getColumnName(i).toLowerCase();
            String oldValue = destMap.put(key, value);
            if (validateKeys && oldValue != null && !oldValue.equals(value)) {
                throw new IllegalArgumentException("Value overwritten."
                        + "Two entries with the same key and different"
                        + "values (" + value + ", " + oldValue + ")");
            }
        }
    }
    
    protected static void getSimpleStats(Connection conn, String query,
            String statTypeKey, String statNameKey, String statName,
            String statKey, DBCollection dst) throws SQLException {
        assert(!conn.isClosed());
        Map<String, String> kvMap = new LinkedHashMap<String, String>();
        Statement s = conn.createStatement();
        ResultSet out = s.executeQuery(query);
        while (out.next()) {
            resultHelper(out, kvMap, false);
        }
        s.close();
        DBCollection singleEntry = new DBCollection()
                   .add(new DBEntry(statNameKey, statName))
                   .add(new DBEntry(statKey, getBaseCollection(kvMap)));
        List<DBCollection> list = new ArrayList<DBCollection>();
        list.add(singleEntry);
        
        dst.add(new DBEntry(statTypeKey,
                list));
    }
    
    protected static void getKVStats(Connection conn, String query,
            String statTypeKey, String statNameKey, String statName,
            String statKey, DBCollection dst) throws SQLException {
        assert(!conn.isClosed());
        Statement s = conn.createStatement();
        Map<String, String> kvMap = new LinkedHashMap<String, String>();
        ResultSet out = s.executeQuery(query);
        while(out.next()) {
            kvMap.put(out.getString(1).toLowerCase(), out.getString(2));
        }
        assert(!kvMap.isEmpty());
        
      DBCollection singleEntry = new DBCollection()
              .add(new DBEntry(statNameKey, statName))
              .add(new DBEntry(statKey, getBaseCollection(kvMap)));
      List<DBCollection> list = new ArrayList<DBCollection>();
      list.add(singleEntry);

      dst.add(new DBEntry(statTypeKey,
              list));
    }
    
    protected static DBCollection getBaseCollection(
            Map<String, String> srcMap) throws SQLException {
        DBCollection dest = new DBCollection();
        DBEntry names = new DBEntry(MapKeys.VARNAMES.toString(), srcMap.keySet());
        DBEntry values = new DBEntry(MapKeys.VARVALS.toString(), srcMap.values());
        dest.add(names).add(values);
        return dest;
    }

    public static void printSQLException(SQLException ex, boolean ignore) {

        for (Throwable e : ex) {
            if (e instanceof SQLException) {
                if (ignore || ignoreSQLException(
                              ((SQLException)e).
                              getSQLState()) == false) {

                    e.printStackTrace(System.err);
                    System.err.println("SQLState: " +
                        ((SQLException)e).getSQLState());

                    System.err.println("Error Code: " +
                        ((SQLException)e).getErrorCode());

                    System.err.println("Message: " + e.getMessage());

                    Throwable t = ex.getCause();
                    while(t != null) {
                        System.out.println("Cause: " + t);
                        t = t.getCause();
                    }
                }
            }
        }
    }

    
    public static boolean ignoreSQLException(String sqlState) {

        if (sqlState == null) {
            System.out.println("The SQL state is not defined!");
            return false;
        }

        // X0Y32: Jar file already exists in schema
        // if (sqlState.equalsIgnoreCase("X0Y32"))
        //     return true;

        // 42Y55: Table already exists in schema
        // if (sqlState.equalsIgnoreCase("42Y55"))
        //     return true;

        return false;
    }

}
