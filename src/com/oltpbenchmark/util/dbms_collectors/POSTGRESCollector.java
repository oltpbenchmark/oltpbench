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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

class POSTGRESCollector extends DBCollector {
    private static final Logger LOG = Logger.getLogger(POSTGRESCollector.class);

    private static final String[] TABLE_STAT_QUERIES = {
        "SELECT * FROM pg_stat_user_tables;",
        "SELECT * FROM pg_statio_user_tables;",
    };
        
    private static final String[] INDEX_STAT_QUERIES = {
        "SELECT * FROM pg_stat_user_indexes;",
        "SELECT * FROM pg_statio_user_indexes;"
    };
    
    @Override
    protected void getGlobalParameters(Connection conn) throws SQLException {
        assert(!conn.isClosed());
        Statement s = conn.createStatement();
        ResultSet out = s.executeQuery("SELECT name, setting FROM pg_settings;");
        while(out.next()) {
            dbParams.put(out.getString(1).toLowerCase(), out.getString(2));
        }
        assert(!dbParams.isEmpty());
    }
    
    @Override
    protected void getGlobalStats(Connection conn) throws SQLException {
        assert(!conn.isClosed());
        Statement s = conn.createStatement();
        ResultSet out = s.executeQuery("SELECT * FROM pg_stat_bgwriter;");
        resultHelper(out, dbGlobalStats, false);
        s.close();
        assert(!dbGlobalStats.isEmpty());
    }
    
    @Override
    protected void getDatabaseStats(Connection conn) throws SQLException {
        assert(!conn.isClosed());
        Statement s = conn.createStatement();
        ResultSet out = s.executeQuery("SELECT * FROM pg_stat_database WHERE datname=\'" + databaseName + "\';");
        resultHelper(out, dbDatabaseStats, "datname", false);
        s.close();
        
        assert(!dbDatabaseStats.isEmpty());
    }
    
    @Override
    protected void getTableStats(Connection conn) throws SQLException {
        assert(!conn.isClosed());
        Statement s = null;
        ResultSet out = null;
        for (String query : TABLE_STAT_QUERIES) {
            s = conn.createStatement();
            out = s.executeQuery(query);
            resultHelper(out, dbTableStats, "relname", true);
        }
        s.close();
        if (!tableNames.equals(dbTableStats.keySet())) {
            String error = "Table name mismatch.\n  Expected: " +
                    this.tableNames.toString() + "\n  Actual: " +
                    dbTableStats.keySet() + "\n";
            throw new SQLException(error);
        }
        
        assert(!dbTableStats.isEmpty());
    }
    
    @Override
    protected void getIndexStats(Connection conn) throws SQLException {
        assert(!conn.isClosed());
        Statement s = null;
        ResultSet out = null;
        for (String query : INDEX_STAT_QUERIES) {
            s = conn.createStatement();
            out = s.executeQuery(query);
            resultHelper(out, dbIndexStats, "indexrelname", true);
        }
        s.close();
        
        assert(!dbIndexStats.isEmpty());
    }
    
    @Override
    protected void getVersionInfo(Connection conn) throws SQLException {
        super.getVersionInfo(conn);
        
        final Pattern COMPILE_PATTERN = Pattern.compile("\\S+-\\S+-\\S+-[^,]+");
        final Pattern OS_PATTERN = Pattern.compile("(?<=\\()\\S+");
        
        Statement s = conn.createStatement();
        ResultSet out = s.executeQuery("SELECT version();");
        String result = null;
        while (out.next()) {
            result = out.getString(1);
        }
        if (result != null) {
            Matcher m = COMPILE_PATTERN.matcher(result);
            if (m.find()) {
                this.versionInfo.architecture = m.group()
                        .split("-", 2)[0].toLowerCase();
            }
            m = OS_PATTERN.matcher(result);
            if (m.find()) {
                this.versionInfo.osName = m.group().toLowerCase();
            }
        }
    }

}
