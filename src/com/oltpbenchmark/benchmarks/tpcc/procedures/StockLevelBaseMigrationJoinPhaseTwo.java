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

public class StockLevelBaseMigrationJoinPhaseTwo extends TPCCProcedure {

    private static final Logger LOG = Logger.getLogger(StockLevelBaseMigrationJoinPhaseTwo.class);

	public SQLStmt stockGetDistOrderIdSQL = new SQLStmt(
	        "SELECT D_NEXT_O_ID " + 
            " FROM " + TPCCConstants.TABLENAME_DISTRICT +
	        " WHERE D_W_ID = ? " +
            " AND D_ID = ?");

    public static final String queryFormat =
            "select count(distinct (s_i_id)) as stock_count " +
            " from orderline_stock " +
            " where ol_w_id = {0,number,#} " +
            " and ol_d_id = {1,number,#} " +
            " and ol_o_id < {2,number,#} " +
            " and ol_o_id >= {3,number,#} " +
            " and s_w_id = {4,number,#} " +
            " and s_quantity < {5,number,#};";

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

        // query
        String query = MessageFormat.format(queryFormat,
            w_id, d_id, o_id, o_id - 20, w_id, threshold);

        String[] command = {"/bin/sh", "-c",
            "echo '" + query + "' | " +
            DBWorkload.DB_BINARY_PATH + "/psql -qS -1 -p " +
            DBWorkload.DB_PORT_NUMBER + " tpcc"};
        execCommands(command);

        if (trace) LOG.trace("[baseline] migration join phase two - query done!");

        conn.commit();

        return null;
	 }
}
