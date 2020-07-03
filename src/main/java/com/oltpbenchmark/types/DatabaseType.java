/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.oltpbenchmark.types;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * List of the database management systems that we support
 * in the framework.
 *
 * @author pavlo
 */
public enum DatabaseType {

    /**
     * Parameters:
     * (1) JDBC Driver String
     * (2) Should SQLUtil.getInsertSQL escape table/col names
     * (3) Should SQLUtil.getInsertSQL include col names
     * (4) Does this DBMS support "real" transactions?
     */
    MYSQL(true, false, true),
    POSTGRES(false, false, true),
    COCKROACHDB(false, false, true),

    DB2(true, false, true),
    MYROCKS(true, false, true),
    ORACLE(true, false, true),
    SQLSERVER(true, false, true),
    SQLITE(true, false, true),
    AMAZONRDS(true, false, true),
    SQLAZURE(true, false, true),
    ASSCLOWN(true, false, true),
    HSQLDB(false, false, true),
    H2(true, false, true),
    MONETDB(false, false, true),
    NUODB(true, false, true),
    TIMESTEN(true, false, true),
    CASSANDRA(true, true, false),
    MEMSQL(true, false, false),
    NOISEPAGE(false, false, true);

    DatabaseType(boolean escapeNames, boolean includeColNames, boolean supportTxns) {
        this.escapeNames = escapeNames;
        this.includeColNames = includeColNames;
        this.supportTxns = supportTxns;
    }

    /**
     * If this flag is set to true, then the framework will escape names in
     * the INSERT queries
     */
    private final boolean escapeNames;

    /**
     * If this flag is set to true, then the framework will include the column names
     * when generating INSERT queries for loading data.
     */
    private final boolean includeColNames;

    /**
     * If this flag is set to true, then the framework will invoke the JDBC transaction
     * api to do various things during execution. This should only be disabled
     * if you know that the DBMS will throw an error when these commands are executed.
     * For example, the Cassandra JDBC driver (as of 2018) throws a "Not Implemented" exception
     * when the framework tries to set the isolation level.
     */
    private boolean supportTxns;

    // ---------------------------------------------------------------
    // ACCESSORS
    // ----------------------------------------------------------------

    /**
     * Returns true if the framework should escape the names of columns/tables when
     * generating SQL to load in data for the target database type.
     *
     * @return
     */
    public boolean shouldEscapeNames() {
        return (this.escapeNames);
    }

    /**
     * Returns true if the framework should include the names of columns when
     * generating SQL to load in data for the target database type.
     *
     * @return
     */
    public boolean shouldIncludeColumnNames() {
        return (this.includeColNames);
    }


    // ----------------------------------------------------------------
    // STATIC METHODS + MEMBERS
    // ----------------------------------------------------------------

    protected static final Map<Integer, DatabaseType> idx_lookup = new HashMap<>();
    protected static final Map<String, DatabaseType> name_lookup = new HashMap<>();

    static {
        for (DatabaseType vt : EnumSet.allOf(DatabaseType.class)) {
            DatabaseType.idx_lookup.put(vt.ordinal(), vt);
            DatabaseType.name_lookup.put(vt.name().toUpperCase(), vt);
        }
    }

    public static DatabaseType get(String name) {
        return (DatabaseType.name_lookup.get(name.toUpperCase()));
    }
}
