package com.oltpbenchmark.benchmarks.chbenchmark.queries;

import com.oltpbenchmark.api.SQLStmt;

public class Q21 extends GenericQuery {

	@Override
	protected SQLStmt getStmtSQL() {
		return new SQLStmt(
"select 	 su_name,  count (*)  as  numwait"
 + " from 	 supplier, orderline l1, orders, stock, nation"
 + " where 	 ol_o_id = o_id"
 + "	  and  ol_w_id = o_w_id"
 + "	  and  ol_d_id = o_d_id"
 + "	  and  ol_w_id = s_w_id"
 + "	  and  ol_i_id = s_i_id"
 + "	  and   mod ((s_w_id * s_i_id), 10000 ) = su_suppkey"
 + "	  and  l1.ol_delivery_d > o_entry_d"
 + "	  and   not   exists  ( select  *"
 + "			  from 	orderline l2"
 + "			  where   l2.ol_o_id = l1.ol_o_id"
 + "				 and  l2.ol_w_id = l1.ol_w_id"
 + "				 and  l2.ol_d_id = l1.ol_d_id"
 + "				 and  l2.ol_delivery_d > l1.ol_delivery_d)"
 + "	  and  su_nationkey = n_nationkey"
 + "	  and  n_name =  'Germany' "
 + " group   by  su_name"
 + " order   by  numwait  desc , su_name"
 + ""
 + ""
                                  );
	}
}
