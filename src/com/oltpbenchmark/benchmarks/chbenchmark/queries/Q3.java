package com.oltpbenchmark.benchmarks.chbenchmark.queries;

import com.oltpbenchmark.api.SQLStmt;

public class Q3 extends GenericQuery {

	@Override
	protected SQLStmt getStmtSQL() {
		return new SQLStmt(
"select    ol_o_id, ol_w_id, ol_d_id,"
 + "	  sum(ol_amount)  as  revenue, o_entry_d"
 + " from  	 customer, new_order, oorder, order_line"
 + " where  	 c_state  like   'A%' "
 + "	  and  c_id = o_c_id"
 + "	  and  c_w_id = o_w_id"
 + "	  and  c_d_id = o_d_id"
 + "	  and  no_w_id = o_w_id"
 + "	  and  no_d_id = o_d_id"
 + "	  and  no_o_id = o_id"
 + "	  and  ol_w_id = o_w_id"
 + "	  and  ol_d_id = o_d_id"
 + "	  and  ol_o_id = o_id"
 + "	  and  o_entry_d >  '2007-01-02 00:00:00.000000' "
 + " group   by  ol_o_id, ol_w_id, ol_d_id, o_entry_d"
 + " order   by  revenue  desc , o_entry_d"
                                  );
	}
}
