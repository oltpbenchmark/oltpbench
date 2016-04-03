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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.oltpbenchmark.util.ResultObject.DBCollection;
import com.oltpbenchmark.util.ResultObject.DBEntry;

class MYSQLCollector extends DBCollector {
    private static final Logger LOG = Logger.getLogger(MYSQLCollector.class);
    
    private static final String PARAM_QUERY = "SELECT * FROM "
            + "INFORMATION_SCHEMA.GLOBAL_VARIABLES ORDER BY VARIABLE_NAME;";
    private static final String GLOBAL_QUERY = "SELECT * FROM "
            + "INFORMATION_SCHEMA.GLOBAL_STATUS SORT ORDER BY VARIABLE_NAME;";
    private static final String TABLE_QUERY = "SELECT * FROM "
            + "INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='%s' ORDER "
            + "BY TABLE_NAME;";
    
    @Override
    protected void getGlobalParameters(Connection conn) throws SQLException {
        getKVStats(conn, PARAM_QUERY,
                MapKeys.GLOBAL.toString(), MapKeys.GLBNAME.toString(), "mysql",
                MapKeys.GLBVARS.toString(), dbParams);
    }

    @Override
    protected void getGlobalStats(Connection conn) throws SQLException {
        getKVStats(conn, GLOBAL_QUERY,
                MapKeys.GLOBAL.toString(), MapKeys.GLBNAME.toString(), "mysql",
                MapKeys.GLBSTATS.toString(), dbStats);
    }
    
    @Override
    protected void getTableStats(Connection conn) throws SQLException {
        assert(!conn.isClosed());
        List<DBCollection> tableEntries = new ArrayList<DBCollection>();
        Statement s = null;
        ResultSet out = null;
        s = conn.createStatement();
        out = s.executeQuery(String.format(TABLE_QUERY, databaseName));
        while (out.next()) {
            String tableName = out.getString("table_name");
            assert(tableNames.contains(tableName));
            DBCollection tableEntry = new DBCollection();
            Map<String, String> kvMap = new LinkedHashMap<String, String>();
            tableEntry.add(new DBEntry(MapKeys.TABNAME.toString(),
                    tableName));
            resultHelper(out, kvMap, true);
            assert(!kvMap.isEmpty());
            tableEntry.add(new DBEntry(MapKeys.TABSTATS.toString(),
                    getBaseCollection(kvMap)));
            tableEntries.add(tableEntry);
        }
        s.close();
        assert(tableEntries.size() == tableNames.size());
        dbStats.add(new DBEntry(MapKeys.TABLE.toString(), tableEntries));
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
