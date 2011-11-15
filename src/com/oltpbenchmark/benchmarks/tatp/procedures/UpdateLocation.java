/***************************************************************************
 *  Copyright (C) 2009 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Original Version:                                                      *
 *  Zhe Zhang (zhe@cs.brown.edu)                                           *
 *                                                                         *
 *  Modifications by:                                                      *
 *  Andy Pavlo (pavlo@cs.brown.edu)                                        *
 *  http://www.cs.brown.edu/~pavlo/                                        *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
package com.oltpbenchmark.benchmarks.tatp.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tatp.TATPConstants;

public class UpdateLocation extends Procedure {

    public final SQLStmt getSubscriber = new SQLStmt(
        "SELECT s_id FROM " + TATPConstants.TABLENAME_SUBSCRIBER + " WHERE sub_nbr = ?"
    );
    
    public final SQLStmt updateSubscriber = new SQLStmt(
        "UPDATE " + TATPConstants.TABLENAME_SUBSCRIBER + " SET vlr_location = ? WHERE s_id = ?"
    );
    
//    public final String updateSubscriber = new String(
//        "UPDATE " + TATPConstants.TABLENAME_SUBSCRIBER + " SET vlr_location = ? WHERE sub_nbr = ?"
//    );

    public long run(Connection conn, int location, String sub_nbr) throws SQLException {
    	PreparedStatement stmt = this.getPreparedStatement(conn, getSubscriber);
    	stmt.setString(1, sub_nbr);
    	ResultSet results = stmt.executeQuery();
    	assert(results != null);
    	
    	if (results.next()) {
    		long s_id = results.getLong(1);
    		stmt = this.getPreparedStatement(conn, updateSubscriber);
    		stmt.setInt(1, location);
    		stmt.setLong(2, s_id);
    		return stmt.executeUpdate();
        }
        return 0;
    }
}