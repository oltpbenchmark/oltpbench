package com.oltpbenchmark.benchmarks.smallworldbank.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.smallworldbank.SWBankConstants;
import com.oltpbenchmark.util.AIMSLogger;

public class SendPayment extends Procedure {
    
    private static final Logger LOG = Logger.getLogger(SendPayment.class);
    
    public final SQLStmt spGetAccIdSQL = new SQLStmt(
            "SELECT A_ID "
            + "  FROM " + SWBankConstants.TABLENAME_ACCOUNT 
            + " WHERE A_CUST_ID = ? for share");
    
    public final SQLStmt spUpdateSrcChkBalance = new SQLStmt("UPDATE " 
            + SWBankConstants.TABLENAME_CHECKING 
            + " SET CHK_BALANCE = CHK_BALANCE - ?"
            + " WHERE CHK_A_ID = ?");
    
    public final SQLStmt spUpdateDestChkBalance = new SQLStmt("UPDATE " 
            + SWBankConstants.TABLENAME_CHECKING 
            + " SET CHK_BALANCE = CHK_BALANCE + ?"
            + " WHERE CHK_A_ID = ?");
    
//    public final SQLStmt spUpdateCustTxnSQL = new SQLStmt(
//            "UPDATE " + SWBankConstants.TABLENAME_CUSTOMER
//            + "SET cust_current_tx = cust_current_tx + 1 " 
//            + "WHERE CUST_ID = ?");
    
    public final SQLStmt spUpdateCustTxnSQL = new SQLStmt(
            "update "+ SWBankConstants.TABLENAME_CUSTOMER
                    +" set cust_curr_tx_count = cust_curr_tx_count +1 where cust_id = ?");
    
    public ResultSet run(Connection conn, long s_cust_id, long d_cust_id, float amount) throws SQLException {
        
//        long txnid = AIMSLogger.getTransactionId(conn, this);
        long src_a_id = 0;
        long dest_a_id = 0;
        int utuples = 0;
        ResultSet tmprs = null;
        
        
        PreparedStatement spGetDestAcctId = this.getPreparedStatement(conn, spGetAccIdSQL);
        PreparedStatement spUpdateSrcAcct = this.getPreparedStatement(conn, spUpdateSrcChkBalance);
        PreparedStatement spUpdateDestAcct = this.getPreparedStatement(conn, spUpdateDestChkBalance);
        PreparedStatement spUpdateCustTxn = this.getPreparedStatement(conn, spUpdateCustTxnSQL);
        
        
        // get account ids for source and destinations
        
        PreparedStatement spGetSrcAcctId = this.getPreparedStatement(conn, spGetAccIdSQL);
        spGetSrcAcctId.setLong(1, s_cust_id);
        
        tmprs = spGetSrcAcctId.executeQuery();
        if (!tmprs.next()){
            throw new RuntimeException(String.format("Cannot find customer with id = %d", s_cust_id));
        }
        src_a_id = tmprs.getLong(1);
        tmprs.close();
        
//        AIMSLogger.logReadOperation(txnid, String.format("%s,%d", SWBankConstants.TABLENAME_CUSTOMER,s_cust_id));
        
        spGetSrcAcctId.setLong(1, d_cust_id);
        
        tmprs = spGetDestAcctId.executeQuery();
        if (!tmprs.next()){
            throw new RuntimeException(String.format("Cannot find customer with id = %d", d_cust_id));
        }
        dest_a_id = tmprs.getLong(1);
        tmprs.close();
        
//        AIMSLogger.logReadOperation(txnid, String.format("%s,%d", SWBankConstants.TABLENAME_CUSTOMER,d_cust_id));
        
        // do transfer amount
        spUpdateSrcAcct.setFloat(1, amount);
        spUpdateSrcAcct.setLong(2, src_a_id);
        utuples = spUpdateSrcAcct.executeUpdate();
        assert(utuples == 1);
//        AIMSLogger.logWriteOperation(txnid, String.format("%s,%d",SWBankConstants.TABLENAME_CHECKING,src_a_id));
        ;
        spUpdateDestAcct.setFloat(1, amount);
        spUpdateDestAcct.setLong(2, dest_a_id);
        utuples = spUpdateDestAcct.executeUpdate();
        assert(utuples == 1);
//        AIMSLogger.logWriteOperation(txnid, String.format("%s,%d",SWBankConstants.TABLENAME_CHECKING,dest_a_id));
        
        spUpdateCustTxn.setLong(1, s_cust_id);
        utuples = spUpdateCustTxn.executeUpdate();
        assert(utuples == 1);
//        AIMSLogger.logWriteOperation(txnid, String.format("%s,%d",SWBankConstants.TABLENAME_CUSTOMER,d_cust_id));
        
        spUpdateCustTxn.setLong(1, d_cust_id);
        utuples = spUpdateCustTxn.executeUpdate();
        assert(utuples == 1);
        
        return null;
    }
}
