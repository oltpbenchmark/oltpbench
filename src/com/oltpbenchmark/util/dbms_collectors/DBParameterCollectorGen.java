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

package com.oltpbenchmark.util.dbms_collectors;

public class DBParameterCollectorGen {
    public static DBParameterCollector getCollector(String dbType, 
                                                    String dbUrl,
                                                    String username,
                                                    String password) {
        String db = dbType.toLowerCase();
        DBParameterCollector collector = null;
        if (db.equals("mysql")) {
            collector = new MYSQLCollector();
        } else if (db.equals("postgres")) {
            collector = new POSTGRESCollector();
        } else {
            collector = new DBCollector();
        }
        collector.collect(dbUrl, username, password);
        return collector;
    }
}
