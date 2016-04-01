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
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

class MYSQLCollector extends DBCollector {
    private static final Logger LOG = Logger.getLogger(MYSQLCollector.class);
    
    @Override
    protected void getGlobalParameters(Connection conn) throws SQLException {
		assert(!conn.isClosed());
		Statement s = conn.createStatement();
        ResultSet out = s.executeQuery("SELECT * FROM INFORMATION_SCHEMA.GLOBAL_VARIABLES;");
        while(out.next()) {
            dbParams.put(out.getString(1).toLowerCase(), out.getString(2));
        }
        assert(!dbParams.isEmpty());
    }

    @Override
    protected void getGlobalStats(Connection conn) throws SQLException {
        assert(!conn.isClosed());
        Statement s = conn.createStatement();
        ResultSet out = s.executeQuery("SELECT * FROM INFORMATION_SCHEMA.GLOBAL_STATUS;");
        while(out.next()) {
            dbGlobalStats.put(out.getString(1).toLowerCase(), out.getString(2));
        }
        assert(!dbGlobalStats.isEmpty());
    }
    
    @Override
    protected void getTableStats(Connection conn) throws SQLException {
        assert(!conn.isClosed());
        Statement s = conn.createStatement();
        ResultSet out = s.executeQuery("SELECT * FROM INFORMATION_SCHEMA.TABLES" +
                " WHERE TABLE_SCHEMA='" + databaseName + "' ORDER BY TABLE_NAME;");
        resultHelper(out, dbTableStats, "table_name", false);
        assert(!dbTableStats.isEmpty());
    }
    
    @Override
    protected void getVersionInfo(Connection conn) throws SQLException {
        super.getVersionInfo(conn);
        
        // Include only DBMS version info here
        int verIdx = this.versionInfo.version.indexOf('-');
        if (verIdx >= 0)
            this.versionInfo.version = this.versionInfo.version.substring(0, verIdx);
        
        Statement s = conn.createStatement();
        ResultSet out = s.executeQuery("SELECT * FROM INFORMATION_SCHEMA.GLOBAL_VARIABLES"
                + " WHERE VARIABLE_NAME LIKE 'version_%'");
        while (out.next()) {
            String name = out.getString(1).toLowerCase();
            String value = out.getString(2);
            if (name.equals("version_comment")) {
                value = value.replaceAll("[()]", "");
                this.versionInfo.osName = value.toLowerCase();
            } else if (name.equals("version_compile_machine")) {
                this.versionInfo.architecture = value.toLowerCase();
            } else {
                continue;
            }
        }
        
    }
}
