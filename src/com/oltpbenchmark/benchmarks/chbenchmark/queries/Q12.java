package com.oltpbenchmark.benchmarks.chbenchmark.queries;

import com.oltpbenchmark.api.SQLStmt;

public class Q12 extends GenericQuery {

	@Override
	protected SQLStmt getStmtSQL() {
		return new SQLStmt(
"select 	 o_ol_cnt,"
 + "	  sum ( case   when  o_carrier_id =  1   or  o_carrier_id =  2   then   1   else   0   end )  as  high_line_count,"
 + "	  sum ( case   when  o_carrier_id <>  1   and  o_carrier_id <>  2   then   1   else   0   end )  as  low_line_count"
 + " from 	 orders, orderline"
 + " where 	 ol_w_id = o_w_id"
 + "	  and  ol_d_id = o_d_id"
 + "	  and  ol_o_id = o_id"
 + "	  and  o_entry_d <= ol_delivery_d"
 + "	  and  ol_delivery_d <  '2020-01-01 00:00:00.000000' "
 + " group   by  o_ol_cnt"
 + " order   by  o_ol_cnt"
 + ""
 + ""
                                  );
	}
}
