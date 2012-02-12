package com.oltpbenchmark.benchmarks.resourcestresser.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.resourcestresser.ResourceStresserWorker;

public class CPU1 extends Procedure {

    public final SQLStmt cpuSelect;
    { 
        String complexClause = "passwd";
        for (int i = 1; i <= ResourceStresserWorker.CPU1_nestedLevel; ++i) {
            complexClause = "md5(concat(" + complexClause +",?))";
        } // FOR
        cpuSelect = new SQLStmt(
            "SELECT count(*) FROM (SELECT " + complexClause + " FROM cputable WHERE empid >= 1 AND empid <= 100)"
        );
    }
    
    public void run(Connection conn) throws SQLException {
        final int howManyPerTrasaction = ResourceStresserWorker.CPU1_howManyPerTrasaction;
        final int sleepLength = ResourceStresserWorker.CPU1_sleep;
        final int nestedLevel = ResourceStresserWorker.CPU1_nestedLevel;

        PreparedStatement stmt = this.getPreparedStatement(conn, cpuSelect);

        for (int tranIdx = 0; tranIdx < howManyPerTrasaction; ++tranIdx) {
            double randNoise = ResourceStresserWorker.gen.nextDouble();

            for (int i = 1; i <= nestedLevel; ++i) {
                stmt.setString(i, Double.toString(randNoise));
            } // FOR

            ResultSet rs = stmt.executeQuery();
            try {
                Thread.sleep(sleepLength);
            } catch (InterruptedException e) {
                throw new SQLException("Unexpected interupt while sleeping!");
            }
            rs.close();
        } // FOR
    }
    
}
