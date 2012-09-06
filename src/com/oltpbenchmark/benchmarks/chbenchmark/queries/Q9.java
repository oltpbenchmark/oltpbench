package com.oltpbenchmark.benchmarks.chbenchmark.queries;

import com.oltpbenchmark.api.SQLStmt;

public class Q9 extends GenericQuery {

	@Override
	protected SQLStmt getStmtSQL() {
		return new SQLStmt(
"select 	 n_name,  extract(year   from  o_entry_d)  as  l_year,  sum(ol_amount) as sum_profit"
 + " from 	 item, stock, supplier, order_line, oorder, nation"
 + " where 	 ol_i_id = s_i_id"
 + "	  and  ol_supply_w_id = s_w_id"
 + "	  and   mod ((s_w_id * s_i_id),  10000 ) = su_suppkey"
 + "	  and  ol_w_id = o_w_id"
 + "	  and  ol_d_id = o_d_id"
 + "	  and  ol_o_id = o_id"
 + "	  and  ol_i_id = i_id"
 + "	  and  su_nationkey = n_nationkey"
 + "	  and  i_data  like   '%BB' "
 + " group   by  n_name,  extract(year   from  o_entry_d)"
 + " order   by  n_name, l_year  desc"
                                  );
	}
}
