package com.oltpbenchmark.benchmarks.smallworldbank.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.smallworldbank.SWBankConstants;
import com.oltpbenchmark.util.AIMSLogger;

public class Collect extends Procedure {
    public final SQLStmt getChkBalanceSQL = new SQLStmt("SELECT * FROM " 
            + SWBankConstants.TABLENAME_CHECKING 
            + " WHERE CHK_A_ID = ? for share");
    
    public final SQLStmt updateChkBalanceSQL = new SQLStmt("UPDATE " 
            + SWBankConstants.TABLENAME_CHECKING 
            + " SET CHK_BALANCE = ?"
            + " WHERE CHK_A_ID = ?");
    
    public final SQLStmt incChkBalanceSQL = new SQLStmt("UPDATE " 
            + SWBankConstants.TABLENAME_CHECKING 
            + " SET CHK_BALANCE = CHK_BALANCE + ?"
            + " WHERE CHK_A_ID = ?");
    
    public final SQLStmt decChkBalanceSQL = new SQLStmt("UPDATE " 
            + SWBankConstants.TABLENAME_CHECKING 
            + " SET CHK_BALANCE = CHK_BALANCE - ?"
            + " WHERE CHK_A_ID = ?");
    
    public long run(Connection conn, long[] s_a_ids ,  long d_a_id, double[] amounts, boolean isM) throws SQLException {
        
//        long txnid = AIMSLogger.getTransactionId(conn, this);
        int tmp = 0;
        PreparedStatement decSrcBalance = this.getPreparedStatement(conn, decChkBalanceSQL);
        PreparedStatement incChkBalance = this.getPreparedStatement(conn, incChkBalanceSQL);
        
        
        float destInc = 0;
        for (double d : amounts) {
            destInc += d;
        }
        
        
        assert(amounts.length == s_a_ids.length);
        
        for (int i = 0; i < amounts.length; i++) {
            decSrcBalance.setFloat(1, (float)amounts[i]);
            decSrcBalance.setLong(2, s_a_ids[i]);
            
            
            tmp = decSrcBalance.executeUpdate();
            
            if (tmp == 0){
                throw new RuntimeException(String.format("s_a_id = %d not found",s_a_ids[i]));
            }
            
//            AIMSLogger.logReadOperation(txnid, String.format("%s,%d",SWBankConstants.TABLENAME_CHECKING,s_a_ids[i]));
//            AIMSLogger.logWriteOperation(txnid, String.format("%s,%d",SWBankConstants.TABLENAME_CHECKING,s_a_ids[i]));
        }
        
        
        incChkBalance.setFloat(1,destInc);
        incChkBalance.setLong(2, d_a_id);
        
        tmp = incChkBalance.executeUpdate();
        if (tmp == 0){
            throw new RuntimeException(String.format("d_a_id = %d not found",d_a_id));
        }
//        AIMSLogger.logReadOperation(txnid, String.format("%s,%d",SWBankConstants.TABLENAME_CHECKING,d_a_id));
//        AIMSLogger.logWriteOperation(txnid, String.format("%s,%d",SWBankConstants.TABLENAME_CHECKING,d_a_id));
        if (isM){
            return AIMSLogger.getTransactionId(conn, this);
        }
        else {
            return -1;
        }
        
    }
}
