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
import java.util.List;

import org.apache.log4j.Logger;

import com.oltpbenchmark.DBWorkload;
import com.oltpbenchmark.util.FileUtil;

class DB2Collector extends DBCollector {
    
    private static final Logger LOG = Logger.getLogger(DBWorkload.class);
    
    private static final String PARAM_QUERY_DB = "SELECT NAME, VALUE "
            + "FROM  SYSIBMADM.DBCFG ORDER BY NAME";
    private static final String PARAM_QUERY_DBM = "SELECT NAME, VALUE "
            + "FROM  SYSIBMADM.DBMCFG ORDER BY NAME";
    private static final String DATABASE_QUERY_BP = "SELECT * FROM "
            + "SYSIBMADM.MON_BP_UTILIZATION A, SYSIBMADM.MON_GET_BUFFERPOOL B "
            + "WHERE A.BP_NAME NOT LIKE 'IBMSYSTEMBP%K' "
            + "AND A.BP_NAME=B.BP_NAME "
            + "AND A.MEMBER=B.MEMBER "
            + "ORDER BY A.BP_NAME";
//    private static final String DATABASE_QUERY_CONN = "SELECT * FROM "
//            + "SYSIBMADM.MON_CONNECTION_SUMMARY";
    private static final String TABLE_QUERY = "SELECT * FROM "
            + "SYSSTAT.TABLES WHERE TABSCHEMA='%s' ORDER BY TABNAME";
    private static final String INDEX_QUERY = "SELECT * FROM "
            + "SYSSTAT.INDEXES WHERE TABSCHEMA='%s' ORDER BY TABNAME, INDNAME";
    private static final String COLUMN_QUERY = "SELECT A.TABSCHEMA, A.TABNAME, A.COLNAME, "
            + "A.COLCARD, A.HIGH2KEY, A.LOW2KEY, A.AVGCOLLEN, A.NUMNULLS, A.PCTINLINED, "
            + "A.SUB_COUNT, A.SUB_DELIM_LENGTH, A.AVGCOLLENCHAR, A.PCTENCODED, B.TYPE, "
            + "B.SEQNO, B.COLVALUE, B.VALCOUNT, B.DISTCOUNT "
            + "FROM SYSSTAT.COLUMNS A, SYSSTAT.COLDIST B "
            + "WHERE A.TABSCHEMA='%s' "
            + "AND A.TABSCHEMA=B.TABSCHEMA "
            + "AND A.TABNAME=B.TABNAME "
            + "AND A.COLNAME=B.COLNAME "
            + "ORDER BY A.TABNAME, A.COLNAME";
    
    private String databaseSchema;
    
    public DB2Collector() {
        super();
        databaseSchema = null;
    }
    
    @Override
    protected void prepareCollectors(Connection conn) throws SQLException {
        super.prepareCollectors(conn);

        String currentSchema = getDatabaseSchema(conn);
        assert (currentSchema != null);
        LOG.info("CURRENT SCHEMA:" + currentSchema);
        
        DatabaseMetaData md = conn.getMetaData();
        ResultSet out = md.getTables(null, currentSchema.toUpperCase(), "%", null);
        List<String> tableNames = new ArrayList<String>();
        while (out.next()) {
            tableNames.add(out.getString(2) + "." + out.getString(3));
        }
        out.close();

        CallableStatement callStmt = conn.prepareCall("CALL SYSPROC.ADMIN_CMD(?)");
        for (String tname : tableNames) {
            callStmt.setString(1, "RUNSTATS ON TABLE " + tname 
                    + " WITH DISTRIBUTION ON ALL COLUMNS AND DETAILED INDEXES ALL");
            callStmt.execute();
        }
        callStmt.close();
    }
    
    private String getDatabaseSchema(Connection conn) throws SQLException {
        if (databaseSchema == null) {
            Statement s = conn.createStatement();
            ResultSet out = s.executeQuery("SELECT CURRENT SCHEMA FROM SYSIBM.SYSDUMMY1");
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
                ResultSet out = s.executeQuery("SELECT CURRENT SERVER FROM SYSIBM.SYSDUMMY1");
                while(out.next()) {
                    this.databaseName = out.getString(1).toLowerCase();
                    break;
                }
            } catch(Exception e) {
                this.databaseName = FileUtil.basename(conn.getMetaData().getURL());
            }
        }
        return this.databaseName;
    }
    
    @Override
    protected void getGlobalParameters(Connection conn) throws SQLException {
        getSimpleStats(conn, wrap(PARAM_QUERY_DBM), MapKeys.GLOBAL.toString(),
                dbParams, true);
    }
    
    @Override
    protected void getDatabaseParameters(Connection conn) throws SQLException {
        getSimpleStats(conn, wrap(PARAM_QUERY_DB), MapKeys.DATABASE.toString(),
                dbParams, true);
    }

    @Override
    protected void getDatabaseStats(Connection conn) throws SQLException {
        String[] queries = {DATABASE_QUERY_BP};//, DATABASE_QUERY_CONN};
        getSimpleStats(conn, queries, MapKeys.DATABASE.toString(),
                dbStats, false);
    }
    
    @Override
    protected void getTableStats(Connection conn) throws SQLException {
        getSimpleStats(conn, wrap(String.format(TABLE_QUERY, getDatabaseSchema(conn))),
                MapKeys.TABLE.toString(), dbStats, false);
    }
    
    @Override
    protected void getIndexStats(Connection conn) throws SQLException {
        getSimpleStats(conn, wrap(String.format(INDEX_QUERY, getDatabaseSchema(conn))),
                MapKeys.INDEX.toString(), dbStats, false);
    }
    
    @Override
    protected void getColumnStats(Connection conn) throws SQLException {
        getSimpleStats(conn, wrap(String.format(COLUMN_QUERY, getDatabaseSchema(conn))),
                MapKeys.COLUMN.toString(), dbStats, false);
    }
    
    @Override
    protected void getVersionInfo(Connection conn) throws SQLException {
        super.getVersionInfo(conn);
        
        Statement s = conn.createStatement();
        ResultSet out = s.executeQuery("SELECT PROD_RELEASE FROM "
                + "TABLE(SYSPROC.ENV_GET_PROD_INFO()) where INSTALLED_PROD='ESE'");
        while(out.next()) {
            this.versionInfo.version = out.getString(1);
            break;
        }

        out = s.executeQuery("SELECT OS_NAME FROM TABLE(SYSPROC.ENV_GET_SYS_INFO())");
        while (out.next()) {
            this.versionInfo.osName = out.getString(1).toLowerCase();
            break;
        }
        out = s.executeQuery("SELECT INST_PTR_SIZE FROM TABLE(SYSPROC.ENV_GET_INST_INFO())");
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
