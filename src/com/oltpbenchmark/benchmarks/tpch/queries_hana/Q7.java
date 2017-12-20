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

public class Q7 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt(
              "select "
            +     "supp_nation, "
            +     "cust_nation, "
            +     "l_year, "
            +     "sum(volume) as revenue "
            + "from "
            +     "( "
            +         "select "
            +             "n1.n_name as supp_nation, "
            +             "n2.n_name as cust_nation, "
            +             "extract(year from l_shipdate) as l_year, "
            +             "l_extendedprice * (1 - l_discount) as volume "
            +         "from "
            +             "supplier, "
            +             "lineitem, "
            +             "orders, "
            +             "customer, "
            +             "nation n1, "
            +             "nation n2 "
            +         "where "
            +             "s_suppkey = l_suppkey "
            +             "and o_orderkey = l_orderkey "
            +             "and c_custkey = o_custkey "
            +             "and s_nationkey = n1.n_nationkey "
            +             "and c_nationkey = n2.n_nationkey "
            +             "and ( "
            +                 "(n1.n_name = 'MOZAMBIQUE' and n2.n_name = 'CANADA') "
            +                 "or (n1.n_name = 'CANADA' and n2.n_name = 'MOZAMBIQUE') "
            +             ") "
            +             "and l_shipdate between to_date ('1995-01-01', 'YYYY-MM-DD') and to_date ('1996-12-31', 'YYYY-MM-DD') "
            +     ") as shipping "
            + "group by "
            +     "supp_nation, "
            +     "cust_nation, "
            +     "l_year "
            + "order by "
            +     "supp_nation, "
            +     "cust_nation, "
            +     "l_year"
        );

    protected SQLStmt get_query() {
        return query_stmt;
    }
}
