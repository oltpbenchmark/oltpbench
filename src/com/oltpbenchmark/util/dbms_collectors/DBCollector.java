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
import java.sql.SQLException;
import java.util.Map;
import java.util.Map.Entry;
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
    private static final Logger LOG = Logger.getLogger(DBCollector.class);
    
    protected static final int MAX_ATTEMPTS = 10;
    
    protected final Map<String, String> dbConf = new TreeMap<String, String>();
    
    protected final Map<String, String> dbGlobalStats = 
            new TreeMap<String, String>();
    
    protected final Map<String, String> dbDatabaseStats = 
            new TreeMap<String, String>();
    
    protected final Map<String, Map<String, String>> dbTableStats = 
            new TreeMap<String, Map<String, String>>();
    
    protected String versionKey;
    
    protected String databaseName;
    
    protected final Set<String> tableNames = new TreeSet<String>();

    @Override
    public String collectConfigParameters() {
        JSONStringer stringer = new JSONStringer();
        try {
            stringer.object()
                    .key("system_variables")
                    .array();
            for (Map.Entry<String, String> kv : dbConf.entrySet()) {
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
            for (Map.Entry<String, String> kv : dbDatabaseStats.entrySet()) {
                stringer.object()
                        .key("variable_name")
                        .value(kv.getKey())
                        .key("variable_value")
                        .value(kv.getValue())
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
        if (dbConf == null || dbConf.isEmpty())
            return "";
        else
            return dbConf.get(versionKey);
    }

    protected Connection connect(String oriDBUrl, String username, String password) {
        Connection conn = null;
        SQLException ex = null;
        int failed_attempts = 0;
        while (conn == null && failed_attempts < MAX_ATTEMPTS) {
            try {
                conn = DriverManager.getConnection(oriDBUrl, username, password);
                Catalog.setSeparator(conn);
                this.databaseName = conn.getCatalog();
                DatabaseMetaData dbmd = conn.getMetaData();
                String[] types = {"TABLE"};
                ResultSet rs = dbmd.getTables(null, null, "%", types);
                while (rs.next()) {
                    this.tableNames.add(rs.getString("TABLE_NAME"));
                }
                assert(tableNames.size() > 0);
            } catch (SQLException e) {
                ex = e;
                LOG.debug("Error while collecting DB parameters: " + e.getMessage());
            }
            failed_attempts++;
        }
        
        if (conn == null) {
            raiseException("Could not connect to database to collect DB parameters.",
                    ex, ErrorCodes.NO_CONNECTION);
        }
        return conn;
    }
    
    protected static void raiseException(String msg, Exception e, int errorCode) {
        LOG.error(msg);
        if (e != null)
            e.printStackTrace();
        System.exit(errorCode);
    }
}
