package com.oltpbenchmark.benchmarks.ycsb.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Map.Entry;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

public class ReadModifyWriteRecord extends Procedure{
    public final SQLStmt selectStmt = new SQLStmt(
            "Select * from usertable where YCSB_KEY=?"
        );
    public final SQLStmt updateAllStmt = new SQLStmt(
            "UPDATE USERTABLE SET FIELD1=?,FIELD2=?,FIELD3=?,FIELD4=?,FIELD5=?," +
            "FIELD6=?,FIELD7=?,FIELD8=?,FIELD9=?,FIELD10=? WHERE YCSB_KEY=?"
        );
    	//FIXME: The value in ysqb is a byteiterator
        public void run(Connection conn, int keyname, Map<Integer,String> results) throws SQLException {
            PreparedStatement stmt = this.getPreparedStatement(conn, selectStmt);
            stmt.setInt(1, keyname);          
            ResultSet r=stmt.executeQuery();
            while(!r.next())
            {
            	for(int i=1;i<11;i++)
            		results.put(i, r.getString(i));
            }
            stmt= this.getPreparedStatement(conn, updateAllStmt);
            stmt.setInt(11, keyname);
            for(int i=1;i<11;i++)
            	stmt.setString(i, results.get(i)+"\\updated");
            stmt.executeUpdate();
            
            conn.commit();
        }

}
