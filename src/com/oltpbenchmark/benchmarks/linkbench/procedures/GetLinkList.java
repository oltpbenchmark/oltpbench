package com.oltpbenchmark.benchmarks.linkbench.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.oltpbenchmark.benchmarks.linkbench.LinkBenchConstants;
import com.oltpbenchmark.benchmarks.linkbench.pojo.Link;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

public class GetLinkList extends Procedure{

    private static final Logger LOG = Logger.getLogger(GetLinkList.class);

    public final SQLStmt getLinkListsStmt = new SQLStmt(
            "select id1, id2, link_type," +
                    " visibility, data, time," +
                    " version from  linktable "+
                    " where id1 = ? and link_type = ?" +
                    " and time >= ?" +
                    " and time <= ?" +
                    " and visibility = ?" +
                    " order by time desc " +
                    " limit ?, ?"
    );

    public Link[] run(Connection conn, long id1, long link_type,
            long minTimestamp, long maxTimestamp,
            int offset, int limit) throws SQLException {
        PreparedStatement stmt = this.getPreparedStatement(conn, getLinkListsStmt);
        stmt.setLong(1, id1);          
        stmt.setLong(2, link_type);          
        stmt.setLong(3, minTimestamp);          
        stmt.setLong(4, maxTimestamp);                   
        stmt.setLong(5, LinkBenchConstants.VISIBILITY_DEFAULT);
        stmt.setInt(6, offset);
        stmt.setInt(7, limit);
        
        if (LOG.isTraceEnabled()) {
            LOG.trace("Query is " + stmt);
        }
        
        ResultSet rs = stmt.executeQuery();

        // Find result set size
        // be sure we fast forward to find result set size
        assert(rs.getType() != ResultSet.TYPE_FORWARD_ONLY);
        rs.last();
        int count = rs.getRow();
        rs.beforeFirst();

        if (LOG.isTraceEnabled()) {
          LOG.trace("Range lookup result: " + id1 + "," + link_type +
                             " is " + count);
        }
        if (count == 0) {
          return null;
        }

        // Fetch the link data
        Link links[] = new Link[count];
        int i = 0;
        while (rs.next()) {
          Link l = createLinkFromRow(rs);
          links[i] = l;
          i++;
        }
        assert(i == count);
        return links;
    }
    // lookup using just id1, type

    public Link[] run(Connection conn, long id1, long link_type)
      throws SQLException {
      // Retry logic in getLinkList
      return run(conn, id1, link_type, 0, Long.MAX_VALUE, 0, LinkBenchConstants.DEFAULT_LIMIT);
    }
    private Link createLinkFromRow(ResultSet rs) throws SQLException {
        Link l = new Link();
        l.id1 = rs.getLong(1);
        l.id2 = rs.getLong(2);
        l.link_type = rs.getLong(3);
        l.visibility = rs.getByte(4);
        l.data = rs.getBytes(5);
        l.time = rs.getLong(6);
        l.version = rs.getInt(7);
        return l;
      }

}
