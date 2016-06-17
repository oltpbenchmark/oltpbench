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
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.oltpbenchmark.DBWorkload;
import com.oltpbenchmark.util.FileUtil;

class DB2Collector extends DBCollector {
    
    private static final Logger LOG = Logger.getLogger(DBWorkload.class);
    
    private static final String PARAM_QUERY_DB = "SELECT NAME, VALUE "
            + "FROM SYSIBMADM.DBCFG UNION SELECT NAME, VALUE FROM "
            + "SYSIBMADM.DBMCFG ORDER BY NAME";
    
    private static final String SYS_QUERY = "SELECT NAME, VALUE FROM "
            + "SYSIBMADM.ENV_SYS_RESOURCES ORDER BY NAME";
    
    private static final String DATABASE_QUERY = "SELECT * FROM "
            + "DB_DB_EVENT WHERE DISCONN_TIME IN "
            + "(SELECT MAX(DISCONN_TIME) FROM DB_DB_EVENT)";

    private static final String MEM_QUERY = "SELECT * FROM DBMEMUSE_DB_EVENT";
    
    private static final String OS_QUERY = "SELECT * FROM OSMETRICS_STATS_EVENT";
    
    private static final String TABLE_QUERY = "SELECT * FROM TABLE_TABLE_EVENT X, "
            + "SYSCAT.TABLES Y WHERE X.TABLE_SCHEMA='%s' AND X.TABLE_NAME "
            + "NOT LIKE '%%_EVENT' AND X.EVENT_TIME IN (SELECT MAX(EVENT_TIME) FROM "
            + "TABLE_TABLE_EVENT) AND X.TABLE_NAME=Y.TABNAME AND "
            + "X.TABLE_SCHEMA=Y.TABSCHEMA ORDER BY X.TABLE_NAME";

    private static final String INDEX_QUERY = "SELECT * FROM "
            + "TABLE(MON_GET_INDEX('','',-2)) AS X, SYSCAT.INDEXES AS Y "
            + "WHERE X.TABSCHEMA=Y.TABSCHEMA AND X.IID=Y.IID "
            + "AND X.TABNAME=Y.TABNAME AND X.TABSCHEMA='%s' "
            + "AND X.TABNAME NOT LIKE '%%_EVENT' ORDER BY X.TABNAME, X.IID";
    
    private static final boolean HAS_EVENT_MONITORS = true;
    
    private static final Map<String, List<String>> EVENT_MONITORS;
    
    static {
        Map<String, List<String>> tmpMap = new HashMap<String, List<String>>();
        tmpMap.put("DB_EVENT", Arrays.asList("DBMEMUSE_DB_EVENT", "DB_DB_EVENT"));
        tmpMap.put("TABLE_EVENT", Arrays.asList("TABLE_TABLE_EVENT"));
        tmpMap.put("STATS_EVENT", Arrays.asList("OSMETRICS_STATS_EVENT"));
        EVENT_MONITORS = Collections.unmodifiableMap(tmpMap);
    }
    
    private String databaseSchema;
    
    public DB2Collector() {
        super();
        databaseSchema = null;
    }
    
    @Override
    protected void prepareCollectors(Connection conn) throws SQLException {
        super.prepareCollectors(conn);
        CallableStatement callStmt = null;
        ResultSet out = null;

        if (HAS_EVENT_MONITORS) {
            // Calling this function writes the current stats_events to tables
            callStmt = conn.prepareCall("CALL WLM_COLLECT_STATS()");
            callStmt.execute();
            
            // TODO If ALL tables belonging to a particular event are empty then
            // there could be a lingering connection. How to fix this?
            callStmt = conn.prepareCall("FLUSH EVENT MONITOR ?");
            Statement s = conn.createStatement();
            LOG.debug("\n");
            for (Map.Entry<String, List<String>> entry : EVENT_MONITORS.entrySet()) {
                LOG.debug(entry.getKey() + ":");
                for (String monitorTable : entry.getValue()) {
                    out = s.executeQuery("SELECT COUNT(*) FROM " + monitorTable);
                    int count = -1;
                    while (out.next()) {
                        count = out.getInt(1);
                    }
                    LOG.debug("  " + monitorTable + " ROWS: " + count);
                }
            }
            s.close();
            LOG.info("\n");
        }

        // We must invoke runstats on all benchmark tables to collect
        // index stats
        callStmt = conn.prepareCall("CALL SYSPROC.ADMIN_CMD(?)");
        String currentSchema = getDatabaseSchema(conn).toUpperCase();
        assert (currentSchema != null);
        
        DatabaseMetaData md = conn.getMetaData();
        out = md.getTables(null, currentSchema, "%", null);
        List<String> tableNames = new ArrayList<String>();
        while (out.next()) {
            tableNames.add(out.getString(3));
        }

        for (String tname : tableNames) {
            String param = String.format("RUNSTATS ON TABLE %s.%s "
                    + "WITH DISTRIBUTION ON KEY COLUMNS AND DETAILED "
                    + "INDEXES ALL", currentSchema, tname);
            callStmt.setString(1, param);
            callStmt.execute();
        }
        callStmt.close();
    }
    
    private String getDatabaseSchema(Connection conn) throws SQLException {
        if (databaseSchema == null) {
            Statement s = conn.createStatement();
            ResultSet out = s.executeQuery("SELECT CURRENT SCHEMA "
                    + "FROM SYSIBM.SYSDUMMY1");
            while(out.next()) {
                this.databaseSchema = out.getString(1);
                break;
            }
            s.close();
        }
        return databaseSchema;
    }
    
    @Override
    protected String getDatabaseName(Connection conn) throws SQLException {
        if (this.databaseName == null) {
            try {
                Statement s = conn.createStatement();
                ResultSet out = s.executeQuery("SELECT CURRENT SERVER "
                        + "FROM SYSIBM.SYSDUMMY1");
                while(out.next()) {
                    this.databaseName = out.getString(1).toLowerCase();
                    break;
                }
            } catch(Exception e) {
                this.databaseName = FileUtil.basename(
                        conn.getMetaData().getURL());
            }
        }
        return this.databaseName;
    }
    
    @Override
    protected void getDatabaseParameters(Connection conn) throws SQLException {
        getSimpleStats(conn, Arrays.asList(PARAM_QUERY_DB),
                MapKeys.DATABASE.toString(), dbParams,
                Arrays.asList(true), true);
    }

    @Override
    protected void getDatabaseStats(Connection conn) throws SQLException {
        getSimpleStats(conn, Arrays.asList(DATABASE_QUERY, OS_QUERY),
                MapKeys.DATABASE.toString(), dbStats,
                Arrays.asList(false, false), true);
    }
    
    @Override
    protected void getTableStats(Connection conn) throws SQLException {
        String tableQuery = String.format(TABLE_QUERY, getDatabaseSchema(conn));
        getSimpleStats(conn, Arrays.asList(tableQuery), MapKeys.TABLE.toString(),
                dbStats, Arrays.asList(false), false);
    }
    
    @Override
    protected void getIndexStats(Connection conn) throws SQLException {
        String indexQuery = String.format(INDEX_QUERY, getDatabaseSchema(conn));
        getSimpleStats(conn, Arrays.asList(indexQuery), MapKeys.INDEX.toString(),
                dbStats, Arrays.asList(false), false);
    }
  
    @Override
    protected void getOtherStats(Connection conn) throws SQLException {
        getSimpleStats(conn, Arrays.asList(SYS_QUERY, MEM_QUERY),
                MapKeys.OTHER.toString(), dbStats,
                Arrays.asList(true, false), false);
    }
    
    @Override
    protected void getVersionInfo(Connection conn) throws SQLException {
        super.getVersionInfo(conn);
        
        Statement s = conn.createStatement();
        ResultSet out = s.executeQuery("SELECT PROD_RELEASE "
                + "FROM TABLE(SYSPROC.ENV_GET_PROD_INFO()) "
                + "WHERE INSTALLED_PROD='ESE'");
        while(out.next()) {
            this.versionInfo.version = out.getString(1);
            break;
        }

        out = s.executeQuery("SELECT OS_NAME FROM "
                + "TABLE(SYSPROC.ENV_GET_SYS_INFO())");
        while (out.next()) {
            this.versionInfo.osName = out.getString(1).toLowerCase();
            break;
        }
        out = s.executeQuery("SELECT INST_PTR_SIZE FROM "
                + "TABLE(SYSPROC.ENV_GET_INST_INFO())");
        while(out.next()) {
            int ptr_size = out.getInt(1);
            if (ptr_size == 64) {
                this.versionInfo.architecture = "x86_64";
            } else if (ptr_size == 32) {
                this.versionInfo.architecture = "i386";
            }
            break;
        }
    }
}
