package com.oltpbenchmark.benchmarks.smallworldbank.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.smallworldbank.SWBankConstants;
import com.oltpbenchmark.benchmarks.smallworldbank.SWBankUtil;
import com.oltpbenchmark.benchmarks.smallworldbank.SWBankWorker;
import com.oltpbenchmark.util.AIMSLogger;

public class Balance extends Procedure {
    
    private static final Logger LOG = Logger.getLogger(Balance.class);
    
    public final SQLStmt stmtGetAccIdSQL = new SQLStmt(
            "SELECT * "
            + "  FROM " + SWBankConstants.TABLENAME_ACCOUNT 
            + " WHERE A_CUST_ID = ?");
    
    public final SQLStmt stmtGetCheckingBalSQL = new SQLStmt(
            "SELECT *"
            + "  FROM " + SWBankConstants.TABLENAME_CHECKING 
            + " WHERE CHK_A_ID = ?");
    
    public final SQLStmt stmtGetSavingBalSQL = new SQLStmt(
            "SELECT *"
            + "  FROM " + SWBankConstants.TABLENAME_SAVING 
            + " WHERE SAV_A_ID = ?");
    
    private PreparedStatement stmtGetAccId = null;
    private PreparedStatement stmtGetCheckingBal = null;
    private PreparedStatement stmtGetSavingBal = null;
    
    private PreparedStatement stmtGetTxnId = null;

    public ResultSet run(Connection conn, long a_id) throws SQLException {
        
        // initialize queries
        stmtGetCheckingBal = this.getPreparedStatement(conn, stmtGetCheckingBalSQL);
        stmtGetSavingBal = this.getPreparedStatement(conn, stmtGetSavingBalSQL);
        
        stmtGetCheckingBal.setLong(1, a_id);
        stmtGetSavingBal.setLong(1, a_id);
        
        // get transaction id
//        long txnid = AIMSLogger.getTransactionId(conn, this);
        
        // execute queries
        float  bal = 0f;
        ResultSet rsChk = stmtGetCheckingBal.executeQuery();
        
        if (!rsChk.next()){
            throw new RuntimeException("CHK_A_ID =" + a_id + " not found! in Checking");
        }
        
        bal += rsChk.getFloat(1);
        rsChk.close();
//        AIMSLogger.logReadOperation(txnid, String.format("%s,%d", SWBankConstants.TABLENAME_CHECKING, a_id));
        
        
//        LOG.info(String.format("CHECKING,%d, %.2f", a_id,bal));
        
        ResultSet rsSav = stmtGetSavingBal.executeQuery();
        
        if (!rsSav.next()){
            throw new RuntimeException("A_ID=" + a_id + " not found! in Saving");
        }
        
        bal += rsSav.getFloat(1);
        rsSav.close();
//        AIMSLogger.logReadOperation(txnid, String.format("%s,%d", SWBankConstants.TABLENAME_SAVING, a_id));
        
//        LOG.info(String.format("SAVING,%d", a_id));
        
//        LOG.info(String.format("TOTALBALCNCE,%5.2f", bal));
        return null;
    }

}
