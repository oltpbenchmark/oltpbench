/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/

package com.oltpbenchmark.benchmarks.tpch.queries;

import com.oltpbenchmark.api.SQLStmt;

public class Q10 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt(
              "select "
            +     "c_custkey, "
            +     "c_name, "
            +     "sum(l_extendedprice * (1 - l_discount)) as revenue, "
            +     "c_acctbal, "
            +     "n_name, "
            +     "c_address, "
            +     "c_phone, "
            +     "c_comment "
            + "from "
            +     "customer, "
            +     "orders, "
            +     "lineitem, "
            +     "nation "
            + "where "
            +     "c_custkey = o_custkey "
            +     "and l_orderkey = o_orderkey "
            +     "and o_orderdate >= to_date ('1994-12-01', 'YYYY-MM-DD') "
            +     "and o_orderdate < add_months (to_date ('1994-12-01', 'YYYY-MM-DD'), 3) "
            +     "and l_returnflag = 'R' "
            +     "and c_nationkey = n_nationkey "
            + "group by "
            +     "c_custkey, "
            +     "c_name, "
            +     "c_acctbal, "
            +     "c_phone, "
            +     "n_name, "
            +     "c_address, "
            +     "c_comment "
            + "order by "
            +     "revenue desc"
        );

    protected SQLStmt get_query() {
        return query_stmt;
    }
}
