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
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.oltpbenchmark.catalog.Catalog;
import com.oltpbenchmark.util.ErrorCodes;

class DBCollector implements DBParameterCollector {
    private static final Logger LOG = Logger.getLogger(DBCollector.class);
    protected static final int MAX_ATTEMPTS = 10;
    protected final Map<String, String> dbConf = new TreeMap<String, String>();
    protected final Map<String, String> dbStatus = new TreeMap<String, String>();
    protected final List<Map<String, String>> dbTable = new LinkedList<Map<String, String>>();
    
    protected String versionKey;
    protected String databaseName;

    @Override
    public String collectConfigParameters() {
        return collectMap(dbConf);
    }
    
    @Override
    public String collectStatusParameters() {
        return collectMap(dbStatus);
    }
    
    @Override
    public String collectTableParameters() {
        return collectList(dbTable);
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
            } catch (SQLException e) {
                ex = e;
                LOG.debug("Error while collecting DB parameters: " + e.getMessage());
            }
            failed_attempts++;
        }
        
        if (conn == null) {
            raiseException("Could not connect to database to collect DB parameters.", ex, ErrorCodes.NO_CONNECTION);
        }
        return conn;
    }
    
    protected static String collectMap(Map<String,String> dbMap) {
    	StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, String> kv : dbMap.entrySet()) {
            builder.append(kv.getKey().toLowerCase())
                       .append("=")
                       .append(kv.getValue().toLowerCase())
                       .append("\n");
        }
        return builder.toString();
    }
    
    private static String collectList(List<Map<String,String>> dbList) {
        StringBuilder builder = new StringBuilder();
        for (Map<String,String> map : dbList) {
            builder.append(collectMap(map)).append("\n");
        }
        return builder.toString();
    }
    
    protected static void raiseException(String msg, Exception e, int errorCode) {
        LOG.error(msg);
        if (e != null)
            e.printStackTrace();
        System.exit(errorCode);
    }
}
