package com.oltpbenchmark.benchmarks.smallworldbank.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.smallworldbank.SWBankConstants;

public class ReportActiveCustomers extends Procedure {
    
    public final String sqlstr1 = "update "+ SWBankConstants.TABLENAME_CUSTOMER
            + " "
            + "set cust_total_tx_count = cust_total_tx_count + cust_curr_tx_count,"
            + "cust_rts_total_tx_count = ?";
    public final String sqlstr2 = "update "+ SWBankConstants.TABLENAME_CUSTOMER
            + " "
            + "set cust_curr_tx_count = 0";
    
    public final SQLStmt stmtUpdateCustTotalsSQL = new SQLStmt(sqlstr1);
    public final SQLStmt stmtUpdateCustCurrentsSQL = new SQLStmt(sqlstr2);
    
    
    
    public ResultSet run(Connection conn) throws SQLException {
     
        PreparedStatement ps1 = this.getPreparedStatement(conn, stmtUpdateCustTotalsSQL);
        PreparedStatement ps2 = this.getPreparedStatement(conn, stmtUpdateCustCurrentsSQL);
        int tmp = 0;
        
        ps1.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        tmp = ps1.executeUpdate();
        if (tmp == 0){
            throw new RuntimeException("Cannot update totals in Customer Table");
        }
        
        tmp = ps2.executeUpdate();
        
        if (tmp == 0){
            throw new RuntimeException("Cannot update rest tx counts in Customer Table");
        }
        
        return null;
    }
    
}
