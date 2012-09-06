package com.oltpbenchmark.benchmarks.chbenchmark.queries;

import com.oltpbenchmark.api.SQLStmt;

public class Q2 extends GenericQuery {

	@Override
	protected SQLStmt getStmtSQL() {
		return new SQLStmt(
"select  	 su_suppkey, su_name, n_name, i_id, i_name, su_address, su_phone, su_comment"
 + " from 	 item, supplier, stock, nation, region,"
 + "	 ( select  s_i_id  as  m_i_id,"
 + "		  min (s_quantity)  as  m_s_quantity"
 + "	  from 	 stock, supplier, nation, region"
 + "	  where 	  mod ((s_w_id*s_i_id), 10000 )=su_suppkey"
 + "	 	  and  su_nationkey=n_nationkey"
 + "	 	  and  n_regionkey=r_regionkey"
 + "	 	  and  r_name  like   'Europ%' "
 + "	  group   by  s_i_id) m"
 + " where  	 i_id = s_i_id"
 + "	  and   mod ((s_w_id * s_i_id),  10000 ) = su_suppkey"
 + "	  and  su_nationkey = n_nationkey"
 + "	  and  n_regionkey = r_regionkey"
 + "	  and  i_data  like   '%b' "
 + "	  and  r_name  like   'Europ%' "
 + "	  and  i_id=m_i_id"
 + "	  and  s_quantity = m_s_quantity"
 + " order   by  n_name, su_name, i_id"
 + ""
 + ""
                                  );
	}
}
