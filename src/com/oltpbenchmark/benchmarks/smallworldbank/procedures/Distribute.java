package com.oltpbenchmark.benchmarks.smallworldbank.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.smallworldbank.SWBankConstants;
import com.oltpbenchmark.util.AIMSLogger;

public class Distribute extends Procedure {
    private static final Logger LOG = Logger.getLogger(Distribute.class);
    
    public final SQLStmt getChkBalanceSQL = new SQLStmt("SELECT CHK_BALANCE FROM " 
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
    
    public final SQLStmt spGetCustIdSQL = new SQLStmt(
            "SELECT A_CUST_ID "
            + "  FROM " + SWBankConstants.TABLENAME_ACCOUNT 
            + " WHERE A_ID = ? for share");
    
    
    public final SQLStmt spUpdateCustTxnSQL = new SQLStmt(
            "update "+ SWBankConstants.TABLENAME_CUSTOMER
                    +" set cust_curr_tx_count = cust_curr_tx_count +1 where cust_id = ?");
    
//    public final SQLStmt spUpdateCustTxnSQL = new SQLStmt(
//            "UPDATE " + SWBankConstants.TABLENAME_CUSTOMER
//            + "SET cust_current_tx = cust_current_tx + 1 " 
//            + "WHERE CUST_ID = ?");
//    
    public ResultSet run(Connection conn, long s_a_id, long[] d_a_ids, double[] amounts) throws SQLException {
//        long txnid = AIMSLogger.getTransactionId(conn, this);
        int tmp = 0;
        int tmp2 = 0;
        
        PreparedStatement getCustId = this.getPreparedStatement(conn, spGetCustIdSQL);
        PreparedStatement updateCustTxn = this.getPreparedStatement(conn, spUpdateCustTxnSQL);
        PreparedStatement getSrcBalance = this.getPreparedStatement(conn, getChkBalanceSQL);
        PreparedStatement updateSrcBalance = this.getPreparedStatement(conn, updateChkBalanceSQL);
        PreparedStatement incChkBalance = this.getPreparedStatement(conn, incChkBalanceSQL);
        getCustId.setLong(1, s_a_id);
        
        ResultSet rs = getCustId.executeQuery();
        if (!rs.next()){
            throw new RuntimeException(String.format("s_a_id = %d not found",s_a_id));
        }
        
        long s_cust_id = rs.getLong(1);
        
//        AIMSLogger.logReadOperation(txnid, String.format("%s,%d",SWBankConstants.TABLENAME_CUSTOMER,s_cust_id));
        
        updateCustTxn.setLong(1, s_cust_id);
        tmp = updateCustTxn.executeUpdate();
        if (tmp == 0){
            throw new RuntimeException(String.format("s_cust_id = %d not found",s_cust_id));
        }
        
        long[] d_cust_ids = new long[d_a_ids.length];
        for (int i = 0; i < d_cust_ids.length; i++) {
            getCustId.setLong(1, d_a_ids[i]);
            rs = getCustId.executeQuery();
            if (!rs.next()){
                throw new RuntimeException(String.format("d_a_id = %d not found",d_a_ids[i]));
            }
            d_cust_ids[i] = rs.getLong(1);
//            AIMSLogger.logReadOperation(txnid, String.format("%s,%d",SWBankConstants.TABLENAME_CUSTOMER,d_cust_ids[i]));
        }
        
        
        
        getSrcBalance.setLong(1, s_a_id);
        
        rs = getSrcBalance.executeQuery();
        if (!rs.next()){
            throw new RuntimeException(String.format("s_a_id = %d not found",s_a_id));
        }
        
        float srcBal = rs.getFloat(1);
//        AIMSLogger.logReadOperation(txnid, String.format("%s,%d",SWBankConstants.TABLENAME_CHECKING,s_a_id));
        
        float srcDedcution = 0;
        for (double d : amounts) {
            srcDedcution += d;
        }
        
        float newSrcBal = srcBal - srcDedcution;
        
        
        updateSrcBalance.setFloat(1, newSrcBal);
        updateSrcBalance.setLong(2, s_a_id);
        tmp = updateSrcBalance.executeUpdate();
        
        if (tmp == 0){
            throw new RuntimeException(String.format("s_a_id = %d not found",s_a_id));
        }
//        AIMSLogger.logWriteOperation(txnid, String.format("%s,%d",SWBankConstants.TABLENAME_CHECKING,s_a_id));
        
        assert(amounts.length == d_a_ids.length);
        
        for (int i = 0; i < amounts.length; i++) {
            incChkBalance.setFloat(1, (float)amounts[i]);
            incChkBalance.setLong(2, d_a_ids[i]);
            updateCustTxn.setLong(1, d_cust_ids[i]);
            
            tmp = incChkBalance.executeUpdate();
            
            
            if (tmp == 0){
                throw new RuntimeException(String.format("d_a_id = %d not found",d_a_ids[i]));
            }
            
//            AIMSLogger.logWriteOperation(txnid, String.format("%s,%d",SWBankConstants.TABLENAME_CHECKING,d_a_ids[i]));
            
            tmp2 = updateCustTxn.executeUpdate();
            if (tmp2 == 0){
                throw new RuntimeException(String.format("d_cust_id = %d not found",d_cust_ids[i]));
            }
            
//            AIMSLogger.logWriteOperation(txnid, String.format("%s,%d",SWBankConstants.TABLENAME_CUSTOMER,d_cust_ids[i]));
            
            
        }
        
        
        return null;
    }
    
}
