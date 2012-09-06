package com.oltpbenchmark.benchmarks.chbenchmark.queries;

import com.oltpbenchmark.api.SQLStmt;

public class Q19 extends GenericQuery {

	@Override
	protected SQLStmt getStmtSQL() {
		return new SQLStmt(
"select 	 sum (ol_amount)  as  revenue"
 + " from 	orderline, item"
 + " where 	("
 + "	  ol_i_id = i_id"
 + "           and  i_data  like   '%a' "
 + "           and  ol_quantity >=  1 "
 + "           and  ol_quantity <=  10 "
 + "           and  i_price  between   1   and   400000 "
 + "           and  ol_w_id  in  ( 1 , 2 , 3 )"
 + "	)  or  ("
 + "	  ol_i_id = i_id"
 + "	   and  i_data  like   '%b' "
 + "	   and  ol_quantity >=  1 "
 + "	   and  ol_quantity <=  10 "
 + "	   and  i_price  between   1   and   400000 "
 + "	   and  ol_w_id  in  ( 1 , 2 , 4 )"
 + "	)  or  ("
 + "	  ol_i_id = i_id"
 + "	   and  i_data  like   '%c' "
 + "	   and  ol_quantity >=  1 "
 + "	   and  ol_quantity <=  10 "
 + "	   and  i_price  between   1   and   400000 "
 + "	   and  ol_w_id  in  ( 1 , 5 , 3 )"
 + "	)"
 + ""
 + ""
                                  );
	}
}
