package com.oltpbenchmark.benchmarks.chbenchmark.queries;

import com.oltpbenchmark.api.SQLStmt;

public class Q1 extends GenericQuery {

	@Override
	protected SQLStmt getStmtSQL() {
		return new SQLStmt(
				"select ol_number,"
				+ "  sum(ol_quantity) as sum_qty,"
				+ "  sum(ol_amount) as sum_amount,"
				+ "  avg(ol_quantity) as avg_qty,"
				+ "  avg(ol_amount) as avg_amount,"
				+ "  count(*) as count_order"
				+ "  from order_line where ol_delivery_d > '2007-01-02 00:00:00.000000'"
				+ "  group by ol_number order by ol_number");
	}
}
