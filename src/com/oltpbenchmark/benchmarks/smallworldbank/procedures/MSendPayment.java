package com.oltpbenchmark.benchmarks.smallworldbank.procedures;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MSendPayment extends SendPayment {
    private long mtxid = 0L; 
    
    
    public void setMTxId(long mtxid){
        this.mtxid = mtxid;
    }
    
    public long getMTxId(){
        return this.mtxid;
    }
    
    public ResultSet run(Connection conn, long s_cust_id, long d_cust_id, float amount) throws SQLException {
        super.run(conn, s_cust_id, d_cust_id, amount);
        
        return null;
    }
}
