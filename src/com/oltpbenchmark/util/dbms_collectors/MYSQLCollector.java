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

import com.oltpbenchmark.catalog.Catalog;
import com.oltpbenchmark.util.ErrorCodes;

import org.apache.log4j.Logger;

import java.sql.*;

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
    }
    
    private void getGlobalVars(Connection conn) {
    	int failed_attempts = 0;
    	while (dbConf.isEmpty() && failed_attempts < MAX_ATTEMPTS) {
	    	try {
	    		assert(!conn.isClosed());
	    		Statement s = conn.createStatement();
	            ResultSet out = s.executeQuery("SELECT * FROM GLOBAL_VARIABLES;");
	            while(out.next()) {
	                dbConf.put(out.getString("VARIABLE_NAME"), out.getString("VARIABLE_VALUE"));
	            }
	    	} catch (SQLException e) {
	    		LOG.debug("Error while collecting DB parameters: " + e.getMessage());
	    	}
	    	failed_attempts++;
    	}
    	
    	if (dbConf.isEmpty()) {
    		LOG.error("Error while collecting DB configuration parameters.");
    		System.exit(ErrorCodes.DB_ERROR);
    	}
    }
    
    private void getGlobalStatus(Connection conn) {
    	int failed_attempts = 0;
    	while (dbStatus.isEmpty() && failed_attempts < MAX_ATTEMPTS) {
	    	try {
	    		assert(!conn.isClosed());
	    		Statement s = conn.createStatement();
	            ResultSet out = s.executeQuery("SELECT * FROM GLOBAL_STATUS;");
	            while(out.next()) {
	                dbStatus.put(out.getString("VARIABLE_NAME"), out.getString("VARIABLE_VALUE"));
	            }
	    	} catch (SQLException e) {
	    		LOG.debug("Error while collecting DB parameters: " + e.getMessage());
	    	}
	    	failed_attempts++;
    	}
    	
    	if (dbStatus.isEmpty()) {
    		LOG.error("Error while collecting DB status parameters.");
    		System.exit(ErrorCodes.DB_ERROR);
    	}
    }
    
    private Connection connect(String oriDBUrl, String username, String password) {
    	String dbUrl = oriDBUrl.substring(0, oriDBUrl.lastIndexOf('/'));
        dbUrl = dbUrl + "/information_schema";
        Connection conn = null;
        int failed_attempts = 0;
        while (conn == null && failed_attempts < MAX_ATTEMPTS) {
        	try {
        		conn = DriverManager.getConnection(dbUrl, username, password);
        		Catalog.setSeparator(conn);
        	} catch (SQLException e) {
        		LOG.debug("Error while collecting DB parameters: " + e.getMessage());
        	}
        	failed_attempts++;
        }
        
        if (conn == null) {
        	LOG.error("Could not connect to database to collect DB parameters.");
        	System.exit(ErrorCodes.NO_CONNECTION);
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
}
