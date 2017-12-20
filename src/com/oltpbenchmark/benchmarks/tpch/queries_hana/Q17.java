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

public class Q17 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt(
              "select "
            +     "sum(l_extendedprice) / 7.0 as avg_yearly "
            + "from "
            +     "lineitem, "
            +     "part "
            + "where "
            +     "p_partkey = l_partkey "
            +     "and p_brand = 'Brand#14' "
            +     "and p_container = 'MED BOX' "
            +     "and l_quantity < ( "
            +         "select "
            +             "0.2 * avg(l_quantity) "
            +         "from "
            +             "lineitem "
            +         "where "
            +             "l_partkey = p_partkey "
            +     ")"
        );

    protected SQLStmt get_query() {
        return query_stmt;
    }
}
