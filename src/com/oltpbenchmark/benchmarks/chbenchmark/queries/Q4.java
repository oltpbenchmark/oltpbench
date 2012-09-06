package com.oltpbenchmark.benchmarks.chbenchmark.queries;

import com.oltpbenchmark.api.SQLStmt;

public class Q4 extends GenericQuery {

	@Override
	protected SQLStmt getStmtSQL() {
		String sql = "select o_ol_cnt, count(*) as order_count "
				 + " from oorder "
				 + " where o_entry_d >= '2007-01-02 00:00:00.000000' "
				 + "	 and  o_entry_d < '2012-01-02 00:00:00.000000' "
				 + "	 and  exists( select  * "
				 + "	         from  order_line"
				 + "             where  o_id = ol_o_id"
				 + "             and  o_w_id = ol_w_id"
				 + "             and  o_d_id = ol_d_id"
				 + "             and  ol_delivery_d >= o_entry_d)"
				 + " group by  o_ol_cnt"
				 + " order by  o_ol_cnt";
		return new SQLStmt(sql);
	}
}
