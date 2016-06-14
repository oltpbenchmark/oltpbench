package com.oltpbenchmark.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.smallworldbank.SWBankConstants;


public class AIMSLogger {
    
    public static final int READ_OPCODE = 1;
    public static final int UPDATE_OPCODE = 3;
    
    private static final Logger LOG_ACCESS = Logger.getLogger("accessLogger");
    private static final Logger LOG_TRACE = Logger.getLogger("traceLogger");
    
public final static SQLStmt stmtGetTxnIdSQL = new SQLStmt("select txid_current()"); 
    
    public static final SQLStmt stmtGetAccIdSQL = new SQLStmt(
            "SELECT A_ID "
            + "  FROM " + SWBankConstants.TABLENAME_ACCOUNT 
            + " WHERE A_CUST_ID = ?");
    
    
    public static long getTransactionId(Connection conn, Procedure ctx) throws SQLException{
        PreparedStatement stmtGetTxnId = ctx.getPreparedStatement(conn,
                stmtGetTxnIdSQL);
        ResultSet rs_tmp = stmtGetTxnId.executeQuery();
        if (!rs_tmp.next()){
            throw new RuntimeException("Cannot get transaction id");
        }
        long res = rs_tmp.getLong(1);
        rs_tmp.close();
        return res;
    }
    
    public static void logReadOperation(long txnid, String oid){
        LOG_ACCESS.info(String.format("%d,%d,%s,%d", System.nanoTime(),txnid,oid,READ_OPCODE));
    }
    
    public static void logWriteOperation(long txnid,String oid){
        LOG_ACCESS.info(String.format("%d,%d,%s,%d", System.nanoTime(),txnid,oid,UPDATE_OPCODE));
    }
    
    public static void logTransactionSpecs(int txType, String specs){
        LOG_TRACE.info(String.format("%d,%s",  txType, specs));
    }
    
    private String createCSVLogEntry(ArrayList<String> loge) {
        int i = 0;
        StringBuilder sb = new StringBuilder();
        for (String c : loge) {
            if (i != 0)
                sb.append(",");
            sb.append(c);
        }
        return sb.toString();
    }
    
}
