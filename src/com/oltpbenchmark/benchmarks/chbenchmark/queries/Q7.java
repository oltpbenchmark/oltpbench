package com.oltpbenchmark.benchmarks.chbenchmark.queries;

import com.oltpbenchmark.api.SQLStmt;

public class Q7 extends GenericQuery {

	@Override
	protected SQLStmt getStmtSQL() {
		return new SQLStmt(
"select 	 su_nationkey  as  supp_nation,"
 + "	 substr(c_state, 1 , 1 )  as  cust_nation,"
 + "	  extract ( year   from  o_entry_d)  as  l_year,"
 + "	  sum (ol_amount)  as  revenue"
 + " from 	 supplier, stock, orderline, orders, customer, nation n1, nation n2"
 + " where 	 ol_supply_w_id = s_w_id"
 + "	  and  ol_i_id = s_i_id"
 + "	  and   mod ((s_w_id * s_i_id),  10000 ) = su_suppkey"
 + "	  and  ol_w_id = o_w_id"
 + "	  and  ol_d_id = o_d_id"
 + "	  and  ol_o_id = o_id"
 + "	  and  c_id = o_c_id"
 + "	  and  c_w_id = o_w_id"
 + "	  and  c_d_id = o_d_id"
 + "	  and  su_nationkey = n1.n_nationkey"
 + "	  and  ascii(substr(c_state, 1 , 1 )) = n2.n_nationkey"
 + "	  and  ("
 + "		(n1.n_name =  'Germany'   and  n2.n_name =  'Cambodia' )"
 + "	      or "
 + "		(n1.n_name =  'Cambodia'   and  n2.n_name =  'Germany' )"
 + "	     )"
 + "	  and  ol_delivery_d  between   '2007-01-02 00:00:00.000000'   and   '2012-01-02 00:00:00.000000' "
 + " group   by  su_nationkey, substr(c_state, 1 , 1 ),  extract ( year   from  o_entry_d)"
 + " order   by  su_nationkey, cust_nation, l_year"
 + ""
 + ""
                                  );
	}
}
