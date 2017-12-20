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

public class Q4 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt(
              "select "
            +     "o_orderpriority, "
            +     "count(*) as order_count "
            + "from "
            +     "orders "
            + "where "
            +     "o_orderdate >= to_date ('1994-08-01', 'YYYY-MM-DD') "
            +     "and o_orderdate < add_months (to_date ('1994-08-01', 'YYYY-MM-DD'), 3) "
            +     "and exists ( "
            +         "select "
            +             "* "
            +         "from "
            +             "lineitem "
            +         "where "
            +             "l_orderkey = o_orderkey "
            +             "and l_commitdate < l_receiptdate "
            +     ") "
            + "group by "
            +     "o_orderpriority "
            + "order by "
            +     "o_orderpriority"
        );

    protected SQLStmt get_query() {
        return query_stmt;
    }
}
