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

class POSTGRESCollector extends DBCollector {
    
    private static final String PARAM_QUERY = "SHOW ALL";
            //"SELECT name, setting FROM pg_settings;";
    
    private static final String GLOBAL_QUERY =
            "SELECT * FROM pg_stat_bgwriter;";
    
    private static final String DATABASE_QUERY = 
            "SELECT * FROM pg_stat_database WHERE datname='%s';";
    
    private static final String TABLE_QUERY = 
            "select * from pg_stat_user_tables full outer join "
            + "pg_statio_user_tables on pg_stat_user_tables.relid="
            + "pg_statio_user_tables.relid order by "
            + "pg_statio_user_tables.relname;";
    
    private static final String INDEX_QUERY = 
            "select * from pg_stat_user_indexes full outer join "
            + "pg_statio_user_indexes on pg_stat_user_indexes.indexrelid="
            + "pg_statio_user_indexes.indexrelid order by "
            + "pg_statio_user_indexes.indexrelname;";
    
    @Override
    protected void getGlobalParameters(Connection conn) throws SQLException {
        getSimpleStats(conn, PARAM_QUERY, MapKeys.GLOBAL.toString(),
                dbParams, true);
    }
    
    @Override
    protected void getGlobalStats(Connection conn) throws SQLException {
        getSimpleStats(conn, GLOBAL_QUERY, MapKeys.GLOBAL.toString(),
                dbStats, false);
    }
    
    @Override
    protected void getDatabaseStats(Connection conn) throws SQLException {
        getSimpleStats(conn, String.format(DATABASE_QUERY, databaseName),
                MapKeys.DATABASE.toString(), dbStats, false);
    }
    
    @Override
    protected void getTableStats(Connection conn) throws SQLException {
        getSimpleStats(conn, TABLE_QUERY, MapKeys.TABLE.toString(),
                dbStats, false);
    }
    
    @Override
    protected void getIndexStats(Connection conn) throws SQLException {
        getSimpleStats(conn, INDEX_QUERY, MapKeys.INDEX.toString(),
                dbStats, false);
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
