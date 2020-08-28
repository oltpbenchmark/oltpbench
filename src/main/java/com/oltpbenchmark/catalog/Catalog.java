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


package com.oltpbenchmark.catalog;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

/**
 * @author pavlo
 */
public final class Catalog {

    private final Map<String, Table> tables;

    private static String separator;

    public Catalog(Map<String, Table> tables) {
        this.tables = tables;
    }

    public Collection<Table> getTables() {
        return (this.tables.values());
    }
    public int getTableCount() {
        return (this.tables.size());
    }
    public Collection<String> getTableNames() {
        return (this.tables.keySet());
    }

    public Table getTable(String tableName) {
        for (Table table : tables.values()) {
            if (table.getName().equalsIgnoreCase(tableName)) {
                return table;
            }
        }
        throw new IllegalArgumentException(String.format("no table found with name [%s]", tableName));
    }

    public static void setSeparator(Connection c) throws SQLException {
        Catalog.separator = c.getMetaData().getIdentifierQuoteString();
    }

    public static void setSeparator(String separator) throws SQLException {
        Catalog.separator = separator;
    }

    public static String getSeparator() {
        return separator;
    }


}
