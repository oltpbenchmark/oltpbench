package com.oltpbenchmark.benchmarks.smallworldbank.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.smallworldbank.SWBankConstants;
import com.oltpbenchmark.util.AIMSLogger;

public class DepositChecking extends Procedure {
    
    public final SQLStmt incChkBalanceSQL = new SQLStmt("UPDATE " 
            + SWBankConstants.TABLENAME_CHECKING 
            + " SET CHK_BALANCE = CHK_BALANCE + ?"
            + " WHERE CHK_A_ID = ?");
    
    public ResultSet run(Connection conn, long d_a_id, float amount) throws SQLException {
//        long txnid = AIMSLogger.getTransactionId(conn, this);
        int tmp = 0;
        
        PreparedStatement incChkBalance = this.getPreparedStatement(conn, incChkBalanceSQL);
        incChkBalance.setFloat(1,amount);
        incChkBalance.setLong(2, d_a_id);
        
        tmp = incChkBalance.executeUpdate();
        if (tmp == 0){
            throw new RuntimeException(String.format("d_a_id = %d not found",d_a_id));
        }
//        AIMSLogger.logReadOperation(txnid, String.format("%s,%d",SWBankConstants.TABLENAME_CHECKING,d_a_id));
//        AIMSLogger.logWriteOperation(txnid, String.format("%s,%d",SWBankConstants.TABLENAME_CHECKING,d_a_id));
        
        
        return null;
    }
    
}
