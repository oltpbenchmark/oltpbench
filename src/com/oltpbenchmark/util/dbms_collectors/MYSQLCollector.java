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
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.oltpbenchmark.catalog.Catalog;
import com.oltpbenchmark.util.ErrorCodes;

class MYSQLCollector extends DBCollector {
    private static final Logger LOG = Logger.getLogger(MYSQLCollector.class);
    private static final String VERSION = "VERSION";
    private static final int MAX_ATTEMPTS = 10;

    public MYSQLCollector(String oriDBUrl, String username, String password) {
    	Connection conn = connect(oriDBUrl, username, password);
    	assert(conn != null);
    	
    	getGlobalVars(conn);
    	assert(!dbConf.isEmpty());
    	
    	getGlobalStatus(conn);
    	assert(!dbStatus.isEmpty());
    	
    	getTableInfo(conn);
        assert(!dbTable.isEmpty());
        
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
	            ResultSet out = s.executeQuery("SELECT * FROM GLOBAL_VARIABLES;");
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
        while (dbStatus.isEmpty() && failed_attempts < MAX_ATTEMPTS) {
            Statement s = null;
            try {
                assert(!conn.isClosed());
                s = conn.createStatement();
                ResultSet out = s.executeQuery("SELECT * FROM GLOBAL_STATUS;");
                while(out.next()) {
                    dbStatus.put(out.getString("VARIABLE_NAME"), out.getString("VARIABLE_VALUE"));
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
        
        if (dbStatus.isEmpty()) {
            raiseException("Error while collecting DB status parameters.", ex, ErrorCodes.DB_ERROR);
        }
    }
    
    private void getTableInfo(Connection conn) {
        SQLException ex = null;
        int failed_attempts = 0;
        while (dbTable.isEmpty() && failed_attempts < MAX_ATTEMPTS) {
            Statement s = null;
            try {
                assert(!conn.isClosed());
                s = conn.createStatement();
                ResultSet out = s.executeQuery("SELECT * FROM TABLES WHERE TABLE_SCHEMA='" + dbName + 
                        "' ORDER BY TABLE_NAME;");
                while(out.next()) {
                    Map<String,String> map = new TreeMap<String,String>();
                    ResultSetMetaData metadata = out.getMetaData();
                    int numColumns = metadata.getColumnCount();
                    for (int i = 1; i <= numColumns; ++i) {
                        String value = out.getString(i) == null ? "" : out.getString(i);
                        map.put(metadata.getColumnName(i), value);
                    }
                    dbTable.add(map);
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
        
        if (dbStatus.isEmpty()) {
            raiseException("Error while collecting DB status parameters.", ex, ErrorCodes.DB_ERROR);
        }
    }
    
    private Connection connect(String oriDBUrl, String username, String password) {
        int dbIdx = oriDBUrl.lastIndexOf('/') + 1;
    	String dbUrl = oriDBUrl.substring(0, dbIdx);
    	dbUrl += "information_schema";
    	dbName = oriDBUrl.substring(dbIdx, oriDBUrl.length());
        Connection conn = null;
        SQLException ex = null;
        int failed_attempts = 0;
        while (conn == null && failed_attempts < MAX_ATTEMPTS) {
        	try {
        		conn = DriverManager.getConnection(dbUrl, username, password);
        		Catalog.setSeparator(conn);
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

    @Override
    public String collectVersion() {
        String dbVersion = dbConf.get(VERSION);
        int verIdx = dbVersion.indexOf('-');
        if (verIdx >= 0)
	        dbVersion = dbVersion.substring(0, verIdx);
        return dbVersion;
    }
    
    private static void raiseException(String msg, Exception e, int errorCode) {
        LOG.error(msg);
        if (e != null)
            e.printStackTrace();
        System.exit(errorCode);
    }
}
