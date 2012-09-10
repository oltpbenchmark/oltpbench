package com.oltpbenchmark.benchmarks.chbenchmark.queries;

import com.oltpbenchmark.api.SQLStmt;

public class Q14 extends GenericQuery {

	@Override
	protected SQLStmt getStmtSQL() {
		return new SQLStmt(
"select (100.00 * sum(case when i_data like 'PR%' then ol_amount else 0 end) / 1 + sum(ol_amount)) as promo_revenue\n" + 
"from order_line, item\n" + 
"where ol_i_id = i_id and ol_delivery_d >= '2007-01-02 00:00:00.000000'\n" + 
"	and ol_delivery_d < '2020-01-02 00:00:00.000000'"
                                  );
	}
}
