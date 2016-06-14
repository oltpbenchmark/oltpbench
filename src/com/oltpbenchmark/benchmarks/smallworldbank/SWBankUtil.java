package com.oltpbenchmark.benchmarks.smallworldbank;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

public class SWBankUtil {
    
    private static final String getCustIdMaxSql = "select max(cust_id) from customer";
    private static final String getAcctIdMaxSql = "select max(a_id) from account";
    
    public static long getCustIdMax(Connection conn) throws SQLException {
        PreparedStatement ps = conn.prepareStatement(getCustIdMaxSql);
        ResultSet rs = ps.executeQuery();
        if (!rs.next()){
            throw new RuntimeException("Could not get customer id max");            
        }
        long res = rs.getLong(1);
        rs.close();
        return res;
    }

    public static long getAcctIdMax(Connection conn) throws SQLException {
        PreparedStatement ps = conn.prepareStatement(getAcctIdMaxSql);
        ResultSet rs = ps.executeQuery();
        if (!rs.next()){
            throw new RuntimeException("Could not get account id max");            
        }
        long res = rs.getLong(1);
        rs.close();
        return res;
    }
}
