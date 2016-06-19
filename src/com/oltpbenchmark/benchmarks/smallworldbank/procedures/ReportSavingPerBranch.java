package com.oltpbenchmark.benchmarks.smallworldbank.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.smallworldbank.SWBankConstants;

public class ReportSavingPerBranch extends Procedure {
    
    
    String sql1 = "select * from "+SWBankConstants.TABLENAME_SAVING;
    String sql2 = "select * from "+SWBankConstants.TABLENAME_ACCOUNT;
    String sql3 = "select * from "+SWBankConstants.TABLENAME_BRANCH;
    
    String sqls = "select sum(sav_balance), b_id" + " from "
            + SWBankConstants.TABLENAME_SAVING + " s, "
            + SWBankConstants.TABLENAME_ACCOUNT + " a, "
            + SWBankConstants.TABLENAME_BRANCH + " b "
            + "where a.a_b_id = b.b_id and a.a_id = s.sav_a_id "
            + "group by b_id";
    
    public final SQLStmt stmtReportCheckingSQL = new SQLStmt(sqls);
    
    public final SQLStmt stmtr1 = new SQLStmt(sql1);
    public final SQLStmt stmtr2 = new SQLStmt(sql2);
    public final SQLStmt stmtr3 = new SQLStmt(sql3);
    
    
    public final SQLStmt stmtUpdateBranchInfoSQL = new SQLStmt("UPDATE "
            + SWBankConstants.TABLENAME_BRANCH
            + " SET b_rtotal_saving = ?, b_rts_saving = ? "
            + "where b_id = ?");
    
    public ResultSet run(Connection conn, long b_count) throws SQLException {
        
        PreparedStatement aggq = this.getPreparedStatement(conn,
                stmtReportCheckingSQL);
        PreparedStatement updateBranch = this.getPreparedStatement(conn,
                stmtUpdateBranchInfoSQL);
        
        PreparedStatement sql1ps = this.getPreparedStatement(conn, stmtr1);
        PreparedStatement sql2ps = this.getPreparedStatement(conn, stmtr2);
        PreparedStatement sql3ps = this.getPreparedStatement(conn, stmtr3);
        
//        ResultSet tmp = 
        ResultSet tmp1 = sql1ps.executeQuery();
        ResultSet tmp2 = sql2ps.executeQuery();
        ResultSet tmp3 = sql3ps.executeQuery();
        
        tmp1.close();
        tmp2.close();
        tmp3.close();
        
        ResultSet rs = aggq.executeQuery();
        ArrayList<Long> bids = new ArrayList<Long>();
            
        while (rs.next()) {
            double total = rs.getDouble(1);
            long bid = rs.getLong(2);
            updateBranch.setDouble(1, total);
            updateBranch.setTimestamp(2,
                    new Timestamp(System.currentTimeMillis()));
            updateBranch.setLong(3, bid);
            updateBranch.addBatch();
            bids.add(bid);
        }
        
        if (bids.size() != b_count) {
            throw new RuntimeException("Branch counts does not match");
        }
        
        int[] urs = updateBranch.executeBatch();
        
        for (int i = 0; i < urs.length; i++) {
            if (urs[i] == 0) {
                throw new RuntimeException("Cannot update branch with b_id = "
                        + bids.get(i));
            }
        }
        
        return null;
    }
}
