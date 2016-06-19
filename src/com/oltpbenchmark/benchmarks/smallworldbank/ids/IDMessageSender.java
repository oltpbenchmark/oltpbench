package com.oltpbenchmark.benchmarks.smallworldbank.ids;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.log4j.Logger;

public class IDMessageSender implements Runnable {

    private static final Logger LOG = Logger.getLogger(IDMessageSender.class);
    
    private long txid;
    private Connection conn;
    
    private PreparedStatement ps1;
    private PreparedStatement ps2;
    
    
        
    public IDMessageSender(Connection conn, long txid) throws SQLException {
        super();
        this.txid = txid;
        this.conn = conn;
        
        ps1 = conn.prepareStatement("select blockTxn(?);");
        ps1.setLong(1, txid);
        ps2 = conn.prepareStatement("select alertMTxn(?);");
        ps2.setLong(1, txid);
         
    }



    @Override
    public void run() {
        
        try {
            ps1.execute();
            conn.commit();
            ps2.execute();
            conn.commit();
        } catch (SQLException e) {
            LOG.info(String.format("Transaction # %d is malicious but we could not alert DBMS",txid));
            e.printStackTrace();
        }
        
        LOG.info(String.format("Transaction # %d is malicious",txid));
    }

}
