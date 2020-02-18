/******************************************************************************
 *  Copyright 2020 by OLTPBenchmark Project                                   *
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

package com.oltpbenchmark.benchmarks.tpcc.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;
import java.text.MessageFormat;

import org.apache.log4j.Logger;

import com.oltpbenchmark.DBWorkload;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;

public class StockLevelLazyMigrationJoin extends TPCCProcedure {

    private static final Logger LOG = Logger.getLogger(StockLevelLazyMigrationJoin.class);

	public SQLStmt stockGetDistOrderIdSQL = new SQLStmt(
	        "SELECT D_NEXT_O_ID " + 
            " FROM " + TPCCConstants.TABLENAME_DISTRICT +
	        " WHERE D_W_ID = ? " +
            " AND D_ID = ?");

    String txnFormat =
            // FIXME: query plan options can be set in postgresql.config
            // SET max_parallel_workers_per_gather = 0;
            // SET enable_hashjoin TO off;
            // SET enable_mergejoin TO off;
            "begin; " +
            "migrate 2 order_line stock " +
            " explain select count(*) from orderline_stock_v " +
            " where ol_w_id = {0,number,#} " +
            " and ol_d_id = {1,number,#} " +
            " and ol_o_id < {2,number,#} " +
            " and ol_o_id >= {3,number,#} " +
            " and s_w_id = {4,number,#} " +
            " and s_quantity < {5,number,#};"
            +
            "migrate insert into orderline_stock(" +
            " ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d, " +
            " ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info, s_w_id, " +
            " s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_data, " +
            " s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, " +
            " s_dist_07, s_dist_08, s_dist_09, s_dist_10) " +
            " (select " +
            "  ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d, " +
            "  ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info, s_w_id, " +
            "  s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_data, " +
            "  s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, " +
            "  s_dist_07, s_dist_08, s_dist_09, s_dist_10 " +
            "  from order_line, stock " +
            "  where ol_i_id = s_i_id); "
            +
            "select count(distinct (s_i_id)) as stock_count " +
            " from orderline_stock " +
            " where ol_w_id = {6,number,#} " +
            " and ol_d_id = {7,number,#} " +
            " and ol_o_id < {8,number,#} " +
            " and ol_o_id >= {9,number,#} " +
            " and s_w_id = {10,number,#} " +
            " and s_quantity < {11,number,#}; " +
            "commit;";

	private PreparedStatement stockGetDistOrderId = null;

    public ResultSet run(Connection conn, Random gen,
            int w_id, int numWarehouses,
            int terminalDistrictLowerID, int terminalDistrictUpperID,
            TPCCWorker w) throws SQLException {

        boolean trace = LOG.isTraceEnabled(); 
        
        stockGetDistOrderId = this.getPreparedStatement(conn, stockGetDistOrderIdSQL);

        int threshold = TPCCUtil.randomNumber(10, 20, gen);
        int d_id = TPCCUtil.randomNumber(terminalDistrictLowerID, terminalDistrictUpperID, gen);

        int o_id = 0;
        int stock_count = 0;

        stockGetDistOrderId.setInt(1, w_id);
        stockGetDistOrderId.setInt(2, d_id);
        if (trace) LOG.trace(String.format("stockGetDistOrderId BEGIN [W_ID=%d, D_ID=%d]", w_id, d_id));
        ResultSet rs = stockGetDistOrderId.executeQuery();
        if (trace) LOG.trace("stockGetDistOrderId END");

        if (!rs.next()) {
            throw new RuntimeException("D_W_ID="+ w_id +" D_ID="+ d_id+" not found!");
        }
        o_id = rs.getInt("D_NEXT_O_ID");
        rs.close();

        // migration txn
        String migration = MessageFormat.format(txnFormat,
            w_id, d_id, o_id, o_id - 20, w_id, threshold,
            w_id, d_id, o_id, o_id - 20, w_id, threshold);
        LOG.info(migration);
        String[] command = {"/bin/sh", "-c",
            "echo '" + migration + "' | " +
            DBWorkload.DB_BINARY_PATH + "/psql -qS -1 -p " +
            DBWorkload.DB_PORT_NUMBER + " tpcc"};
        execCommands(command);

        if (trace) LOG.trace(String.format("stockGetCountStock BEGIN [W_ID=%d, D_ID=%d, O_ID=%d]", w_id, d_id, o_id));
        if (trace) LOG.trace("stockGetCountStock END");

        // stock_count = rs.getInt("STOCK_COUNT");
        if (trace) LOG.trace("stockGetCountStock RESULT=" + stock_count);

        conn.commit();

        if (trace) {
            StringBuilder terminalMessage = new StringBuilder();
            terminalMessage.append("\n+-------------------------- STOCK-LEVEL --------------------------+");
            terminalMessage.append("\n Warehouse: ");
            terminalMessage.append(w_id);
            terminalMessage.append("\n District:  ");
            terminalMessage.append(d_id);
            terminalMessage.append("\n\n Stock Level Threshold: ");
            terminalMessage.append(threshold);
            terminalMessage.append("\n Low Stock Count:       ");
            terminalMessage.append(stock_count);
            terminalMessage.append("\n+-----------------------------------------------------------------+\n\n");
            LOG.trace(terminalMessage.toString());
        }
        return null;
	 }
}
