package com.oltpbenchmark.benchmarks.smallworldbank.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.smallworldbank.SWBankConstants;

public class ListCountries extends Procedure {
    
    public final String sqlstr0 = "select * from "+ SWBankConstants.TABLENAME_COUNTRY;
    public final SQLStmt stmtr0 = new SQLStmt(sqlstr0);
    
    public ResultSet run(Connection conn) throws SQLException {
        
        // make sure reads were logged.
        PreparedStatement rq = this.getPreparedStatement(conn, stmtr0);
        ResultSet tmp1 = rq.executeQuery();
        tmp1.close();
        
        return null;
    }
}
