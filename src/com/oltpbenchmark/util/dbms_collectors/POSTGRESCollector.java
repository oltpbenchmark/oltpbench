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
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.oltpbenchmark.util.ErrorCodes;

class POSTGRESCollector extends DBCollector {
    private static final Logger LOG = Logger.getLogger(POSTGRESCollector.class);

    private static final String[] TABLE_STAT_VIEWS = {
        "pg_stat_user_tables",
        "pg_statio_user_tables",
        "pg_stat_user_indexes",
        "pg_statio_user_indexes"
    };
    private final Map<String, Map<String, String>> pgDbTable = new TreeMap<String, Map<String, String>>();

    public POSTGRESCollector(String oriDBUrl, String username, String password) {
        this.versionKey = "server_version";

        Connection conn = connect(oriDBUrl, username, password);
        assert(conn != null);
        
        getGlobalVars(conn);
        assert(!dbConf.isEmpty());
        
        getGlobalStatus(conn);
        assert(!dbStatus.isEmpty());
        
        getTableInfo(conn);
        assert(!pgDbTable.isEmpty());
        
        try {
            conn.close();
        } catch (SQLException e) {
        }
    }
    
    @Override
    public String collectTableParameters() {
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, Map<String, String>> entry : pgDbTable.entrySet()) {
            builder.append(String.format("[%s]\n", entry.getKey()));
            builder.append(DBCollector.collectMap(entry.getValue())).append("\n");
        }
        System.out.println();
        return builder.toString();
    }
    
    private void getGlobalVars(Connection conn) {
        SQLException ex = null;
        int failed_attempts = 0;
        while (dbConf.isEmpty() && failed_attempts < MAX_ATTEMPTS) {
            Statement s = null;
            try {
                assert(!conn.isClosed());
                s = conn.createStatement();
                ResultSet out = s.executeQuery("SHOW ALL;");
                while(out.next()) {
                    dbConf.put(out.getString("name"), out.getString("setting"));
                }
            } catch (SQLException e) {
                ex = e;
                LOG.debug("Error while collecting DB parameters: " + e.getMessage());
            } finally {
                try {
                    s.close();
                } catch(SQLException e2) { 
                }
            }
            failed_attempts++;
        }
        
        if (dbConf.isEmpty()) {
            raiseException("Error while collecting DB configuration parameters.", ex, ErrorCodes.DB_ERROR);
        }
    }
    
    private void getGlobalStatus(Connection conn) {
        SQLException ex = null;
        int failed_attempts = 0;

        while (dbStatus.isEmpty() && failed_attempts < MAX_ATTEMPTS) {
            Statement s = null;
            ResultSet out = null;
            try {
                assert(!conn.isClosed());
                s = conn.createStatement();
                out = s.executeQuery("SELECT * FROM pg_stat_bgwriter;");
                resultHelper(out, dbStatus, "pg_stat_bgwriter");
                
                out = s.executeQuery("SELECT * FROM pg_stat_database WHERE datname=\'" + databaseName + "\';");
                resultHelper(out, dbStatus, "pg_stat_database");
                s.close();
            } catch (SQLException e) {
                LOG.debug("Error while collecting DB parameters: " + e.getMessage());
                ex = e;
            } finally {
                try {
                    s.close();
                } catch(SQLException e2) { 
                    Thread.dumpStack();
                }
            }
            failed_attempts++;
        }
        
        if (dbStatus.isEmpty()) {
            raiseException("Error while collecting DB status parameters.", ex, ErrorCodes.DB_ERROR);
        }
    }
    
    private void getTableInfo(Connection conn) {
        SQLException ex = null;
        int failed_attempts = 0;

        while (pgDbTable.isEmpty() && failed_attempts < MAX_ATTEMPTS) {
            Statement s = null;
            ResultSet out = null;
            List<String> tables = new LinkedList<String>();
            try {
                assert(!conn.isClosed());
                s = conn.createStatement();
                out = s.executeQuery("SELECT table_name from information_schema.tables WHERE table_schema='public';");
                while (out.next()) {
                    tables.add(out.getString(1));
                }
                System.out.println("Tables: " + tables.toString());
                
                for (String tablename : tables) {
                    Map<String, String> tmap = new TreeMap<String, String>();
                    for (String view : TABLE_STAT_VIEWS) {
                        out = s.executeQuery(String.format("SELECT * FROM %s WHERE relname=\'%s\'", view, tablename));
                        resultHelper(out, tmap, view);
                    }
                    pgDbTable.put(tablename, tmap);
                }
                s.close();
            } catch (SQLException e) {
                LOG.debug("Error while collecting DB parameters: " + e.getMessage());
                ex = e;
            } finally {
                try {
                    s.close();
                } catch(SQLException e2) { 
                    Thread.dumpStack();
                }
            }
            failed_attempts++;
        }
        
        if (pgDbTable.isEmpty()) {
            raiseException("Error while collecting DB status parameters.", ex, ErrorCodes.DB_ERROR);
        }
    }
    
    private void resultHelper(ResultSet out, Map<String, String> destMap, String keyPrefix) {
        try {
            while(out.next()) {
                ResultSetMetaData metadata;
                metadata = out.getMetaData();
                int numColumns = metadata.getColumnCount();
                for (int i = 1; i <= numColumns; ++i) {
                    String value = out.getString(i) == null ? "" : out.getString(i);
                    String key;
                    if (keyPrefix != null) {
                        key = keyPrefix + "__" + metadata.getColumnName(i);
                    } else {
                        key = metadata.getColumnName(i);
                    }
                    destMap.put(key, value);
                }
            }
        } catch (SQLException e) {
            LOG.debug("Error while collecting DB parameters: " + e.getMessage());
        }
    }

}
