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
import com.oltpbenchmark.types.DatabaseType;

public class Q6 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt(
              "select "
            +     "sum(l_extendedprice * l_discount) as revenue "
            + "from "
            +     "lineitem "
            + "where "
            +     "l_shipdate >= date '1997-01-01' "
            +     "and l_shipdate < date '1997-01-01' + interval '1' year "
            +     "and l_discount between 0.07 - 0.01 and 0.07 + 0.01 "
            +     "and l_quantity < 24"
        );

    public final SQLStmt fb_query_stmt = new SQLStmt(
            "select "
                    + "sum(l_extendedprice * l_discount) as revenue "
                    + "from "
                    + "lineitem "
                    + "where "
                    + "l_shipdate >= date '1997-01-01' "
                    + "and l_shipdate < dateadd(year,1,date '1997-01-01') "
                    + "and l_discount between 0.07 - 0.01 and 0.07 + 0.01 "
                    + "and l_quantity < 24"
    );

    protected SQLStmt get_query() {
        if (getDatabaseType() == DatabaseType.FIREBIRD) {
            return fb_query_stmt;
        }
        return query_stmt;
    }
}
