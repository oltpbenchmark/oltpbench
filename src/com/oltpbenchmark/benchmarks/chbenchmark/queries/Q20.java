package com.oltpbenchmark.benchmarks.chbenchmark.queries;

import com.oltpbenchmark.api.SQLStmt;

public class Q20 extends GenericQuery {

	@Override
	protected SQLStmt getStmtSQL() {
		return new SQLStmt(
"select 	 su_name, su_address"
 + " from 	 supplier, nation"
 + " where 	 su_suppkey  in "
 + "		( select    mod (s_i_id * s_w_id,  10000 )"
 + "		 from      stock, orderline"
 + "		 where     s_i_id  in "
 + "				( select  i_id"
 + "				  from  item"
 + "				  where  i_data  like   'co%' )"
 + "			  and  ol_i_id=s_i_id"
 + "			  and  ol_delivery_d >  '2010-05-23 12:00:00' "
 + "		 group   by  s_i_id, s_w_id, s_quantity"
 + "		 having     2 *s_quantity >  sum (ol_quantity))"
 + "	  and  su_nationkey = n_nationkey"
 + "	  and  n_name =  'Germany' "
 + " order   by  su_name"
 + ""
 + ""
                                  );
	}
}
