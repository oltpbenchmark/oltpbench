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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.oltpbenchmark.util.ErrorCodes;

class MYSQLCollector extends DBCollector {
    private static final Logger LOG = Logger.getLogger(MYSQLCollector.class);

    public MYSQLCollector(String oriDBUrl, String username, String password) {
        this.versionKey = "VERSION";
        
    	Connection conn = connect(oriDBUrl, username, password);
    	assert(conn != null);
    	
    	getGlobalVars(conn);
    	assert(!dbConf.isEmpty());
    	
    	getGlobalStatus(conn);
    	assert(!dbGlobalStats.isEmpty());
    	
    	getTableInfo(conn);
        assert(!dbTableStats.isEmpty());
        
        try {
            conn.close();
        } catch (SQLException e) {
        }
    }
    
    private void getGlobalVars(Connection conn) {
        SQLException ex = null;
    	int failed_attempts = 0;
    	while (dbConf.isEmpty() && failed_attempts < MAX_ATTEMPTS) {
    	    Statement s = null;
	    	try {
	    		assert(!conn.isClosed());
	    		s = conn.createStatement();
	            ResultSet out = s.executeQuery("SELECT * FROM INFORMATION_SCHEMA.GLOBAL_VARIABLES;");
	            while(out.next()) {
	                dbConf.put(out.getString("VARIABLE_NAME"), out.getString("VARIABLE_VALUE"));
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
        while (dbGlobalStats.isEmpty() && failed_attempts < MAX_ATTEMPTS) {
            Statement s = null;
            try {
                assert(!conn.isClosed());
                s = conn.createStatement();
                ResultSet out = s.executeQuery("SELECT * FROM INFORMATION_SCHEMA.GLOBAL_STATUS;");
                while(out.next()) {
                    dbGlobalStats.put(out.getString("VARIABLE_NAME"), out.getString("VARIABLE_VALUE"));
                }
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
        
        if (dbGlobalStats.isEmpty()) {
            raiseException("Error while collecting DB status parameters.", ex, ErrorCodes.DB_ERROR);
        }
    }
    
    private void getTableInfo(Connection conn) {
        SQLException ex = null;
        int failed_attempts = 0;
        while (dbTableStats.isEmpty() && failed_attempts < MAX_ATTEMPTS) {
            Statement s = null;
            try {
                assert(!conn.isClosed());
                s = conn.createStatement();
                ResultSet out = s.executeQuery("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='" + 
                        databaseName + "' ORDER BY TABLE_NAME;");
                
                List<String> columnNames = new ArrayList<String>();
                ResultSetMetaData metadata = out.getMetaData();
                int numColumns = metadata.getColumnCount();
                int tableNameIndex = -1;
                for (int i = 1; i <= numColumns; ++i) {
                    String columnName = metadata.getColumnName(i);
                    if (columnName.toLowerCase().equals("table_name")) {
                        tableNameIndex = i;
                    }
                    columnNames.add(columnName);
                }
                
                assert(columnNames.size() == tableNames.size());
                while (out.next()) {
                    String tableName = out.getString(tableNameIndex);
                    assert(tableNames.contains(tableName));
                    Map<String, String> map = new TreeMap<String, String>();
                    for (int i = 1; i <= numColumns; ++i) {
                        String columnName = columnNames.get(i);
                        String value = out.getString(i) == null ? "" : out.getString(i);
                        
                        map.put(columnName, value);
                    }
                    
                    dbTableStats.put(tableName, map);
                }
                assert !dbTableStats.isEmpty();
            } catch (SQLException e) {
                ex = e;
                LOG.debug("Error while collecting DB parameters: " + e.getMessage());
            } finally {
                try {
                    s.close();
                } catch(SQLException e2) { }  
            }
            failed_attempts++;
        }
        
        if (dbGlobalStats.isEmpty()) {
            raiseException("Error while collecting DB status parameters.", ex, ErrorCodes.DB_ERROR);
        }
    }

    @Override
    public String collectVersion() {
        String dbVersion = dbConf.get(versionKey);
        int verIdx = dbVersion.indexOf('-');
        if (verIdx >= 0)
	        dbVersion = dbVersion.substring(0, verIdx);
        return dbVersion;
    }
}
