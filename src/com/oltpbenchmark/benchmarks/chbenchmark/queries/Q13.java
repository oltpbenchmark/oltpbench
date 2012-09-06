package com.oltpbenchmark.benchmarks.chbenchmark.queries;

import com.oltpbenchmark.api.SQLStmt;

public class Q13 extends GenericQuery {

	@Override
	protected SQLStmt getStmtSQL() {
		return new SQLStmt(
"select 	 c_count,  count (*)  as  custdist"
 + " from 	 ( select  c_id,  count (o_id)"
 + "	  from  customer  left   outer   join  orders  on  ("
 + "		c_w_id = o_w_id"
 + "		 and  c_d_id = o_d_id"
 + "		 and  c_id = o_c_id"
 + "		 and  o_carrier_id >  8 )"
 + "	  group   by  c_id)  as  c_orders (c_id, c_count)"
 + " group   by  c_count"
 + " order   by  custdist  desc , c_count  desc "
 + ""
 + ""
                                  );
	}
}
