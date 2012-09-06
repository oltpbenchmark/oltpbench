package com.oltpbenchmark.benchmarks.chbenchmark.queries;

import com.oltpbenchmark.api.SQLStmt;

public class Q22 extends GenericQuery {

	@Override
	protected SQLStmt getStmtSQL() {
		return new SQLStmt(
"select 	 substr(c_state, 1 , 1 )  as  country,"
 + "	  count (*)  as  numcust,"
 + "	  sum (c_balance)  as  totacctbal"
 + " from 	 customer"
 + " where 	 substr(c_phone, 1 , 1 )  in  ( '1' , '2' , '3' , '4' , '5' , '6' , '7' )"
 + "	  and  c_balance > ( select   avg (c_BALANCE)"
 + "			   from  	 customer"
 + "			   where   c_balance >  0 . 00 "
 + "			 	  and  substr(c_phone, 1 , 1 )  in  ( '1' , '2' , '3' , '4' , '5' , '6' , '7' ))"
 + "	  and   not   exists  ( select  *"
 + "			  from 	orders"
 + "			  where 	o_c_id = c_id"
 + "			     	 and  o_w_id = c_w_id"
 + "			    	 and  o_d_id = c_d_id)"
 + " group   by  substr(c_state, 1 , 1 )"
 + " order   by  substr(c_state, 1 , 1 )"
 + ""
 + ""
                                  );
	}
}
