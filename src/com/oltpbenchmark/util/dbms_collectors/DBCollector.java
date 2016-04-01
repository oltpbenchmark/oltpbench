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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import com.oltpbenchmark.catalog.Catalog;
import com.oltpbenchmark.util.ErrorCodes;
import com.oltpbenchmark.util.JSONUtil;
import com.oltpbenchmark.util.json.JSONException;
import com.oltpbenchmark.util.json.JSONStringer;

class DBCollector implements DBParameterCollector {
    
    public class VersionInfo {
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
    
    
    private static final Logger LOG = Logger.getLogger(DBCollector.class);
        
    protected static final int MAX_ATTEMPTS = 10;
    
    protected final Map<String, String> dbParams;
    
    protected final Map<String, String> dbGlobalStats;
    
    protected final Map<String, Map<String, String>> dbDatabaseStats;
    
    protected final Map<String, Map<String, String>> dbTableStats;
    
    protected final Map<String, Map<String, String>> dbIndexStats;

    protected final Set<String> tableNames;

    protected final VersionInfo versionInfo;
    
    protected String databaseName;
    
    protected String isolationLevel;
    
    
    public DBCollector() {
        this.dbParams = new TreeMap<String, String>();
        this.dbGlobalStats = new TreeMap<String, String>();
        this.dbDatabaseStats = new TreeMap<String, Map<String, String>>();
        this.dbTableStats = new TreeMap<String, Map<String, String>>();
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
        this.databaseName = conn.getCatalog().toLowerCase();
    }
    
    protected void getVersionInfo(Connection conn) throws SQLException {
        DatabaseMetaData metadata = conn.getMetaData();
        this.versionInfo.version = metadata.getDatabaseProductVersion().toLowerCase();
    }
    
    protected void getIsolationLevel(Connection conn) throws SQLException {
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
    }
    
    protected void getTables(Connection conn) throws SQLException {
        DatabaseMetaData metadata = conn.getMetaData();
        String[] types = {"TABLE"};
        ResultSet rs = metadata.getTables(null, null, "%", types);
        while (rs.next()) {
            this.tableNames.add(rs.getString("table_name").toLowerCase());
        }
        assert(tableNames.size() > 0);
    }
    
    protected void getDatabaseVersionInfo(Connection conn) throws SQLException { }
    
    protected void getGlobalParameters(Connection conn) throws SQLException { }
    
    protected void getGlobalStats(Connection conn) throws SQLException { }
    
    protected void getDatabaseStats(Connection conn) throws SQLException { }
    
    protected void getTableStats(Connection conn) throws SQLException { }
    
    protected void getIndexStats(Connection conn) throws SQLException { }

    @Override
    public String collectConfigParameters() {
        JSONStringer stringer = new JSONStringer();
        try {
            stringer.object()
                    .key("system_variables")
                    .array();
            for (Map.Entry<String, String> kv : dbParams.entrySet()) {
                stringer.object()
                        .key("variable_name")
                        .value(kv.getKey())
                        .key("variable_value")
                        .value(kv.getValue())
                        .endObject();
            }
            stringer.endArray()
                    .endObject();
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return JSONUtil.format(stringer.toString());
    }
    
    @Override
    public String collectStats() {
        JSONStringer stringer = new JSONStringer();
        try {
            stringer.object()
                    .key("statistics")
                    .object()
                    .key("global")
                    .array();
            for (Map.Entry<String, String> kv : dbGlobalStats.entrySet()) {
                stringer.object()
                        .key("variable_name")
                        .value(kv.getKey())
                        .key("variable_value")
                        .value(kv.getValue())
                        .endObject();
            }
            stringer.endArray()
                    .key("database")
                    .array();
            for (Entry<String, Map<String, String>> dbmap : dbDatabaseStats.entrySet()) {
                stringer.object()
                        .key("dbname")
                        .value(dbmap.getKey())
                        .key("dbstats")
                        .array();
                for (Map.Entry<String, String> kv : dbmap.getValue().entrySet()) {
                    stringer.object()
                            .key("variable_name")
                            .value(kv.getKey())
                            .key("variable_value")
                            .value(kv.getValue())
                            .endObject();
                }
                stringer.endArray()
                .endObject();
            }
            stringer.endArray()
                    .key("table")
                    .array();
            for (Entry<String, Map<String, String>> tablemap : dbTableStats.entrySet()) {
                stringer.object()
                        .key("tablename")
                        .value(tablemap.getKey())
                        .key("tablestats")
                        .array();
                for (Map.Entry<String, String> kv : tablemap.getValue().entrySet()) {
                    stringer.object()
                            .key("variable_name")
                            .value(kv.getKey())
                            .key("variable_value")
                            .value(kv.getValue())
                            .endObject();
                }
                stringer.endArray()
                        .endObject();
            }
            stringer.endArray()
                    .key("index")
                    .array();
            for (Entry<String, Map<String, String>> indexmap : dbIndexStats.entrySet()) {
                stringer.object()
                        .key("indexname")
                        .value(indexmap.getKey())
                        .key("indexstats")
                        .array();
                for (Map.Entry<String, String> kv : indexmap.getValue().entrySet()) {
                    stringer.object()
                            .key("variable_name")
                            .value(kv.getKey())
                            .key("variable_value")
                            .value(kv.getValue())
                            .endObject();
                }
                stringer.endArray()
                        .endObject();
            }
            stringer.endArray()
                    .endObject()
                    .endObject();
        } catch (JSONException e) {
            e.printStackTrace();
        }
        System.out.println(stringer.toString() == null);
        return JSONUtil.format(stringer.toString());
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
    
    protected static void resultHelper(ResultSet out,Map<String, String> destMap,
            boolean validateKeys) throws SQLException {
        ResultSetMetaData metadata;
        metadata = out.getMetaData();
        int numColumns = metadata.getColumnCount();
        while(out.next()) {
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
