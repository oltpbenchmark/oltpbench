/******************************************************************************
 * Copyright 2015 by OLTPBenchmark Project                                   *
 * *
 * Licensed under the Apache License, Version 2.0 (the "License");           *
 * you may not use this file except in compliance with the License.          *
 * You may obtain a copy of the License at                                   *
 * *
 * http://www.apache.org/licenses/LICENSE-2.0                              *
 * *
 * Unless required by applicable law or agreed to in writing, software       *
 * distributed under the License is distributed on an "AS IS" BASIS,         *
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 * See the License for the specific language governing permissions and       *
 * limitations under the License.                                            *
 ******************************************************************************/

package com.oltpbenchmark.benchmarks.tpch.util;

import com.oltpbenchmark.WorkloadConfiguration;
import org.apache.log4j.Logger;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.FileInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;

public class CopyUtil {

    // copyDATABASE methods return null if they weren't successful, and return
    // a possibly empty String[] of SQL statements to execute otherwise

    private static String getTablePath(WorkloadConfiguration workConf, String tableName) {
        String fileFormat = workConf.getXmlConfig().getString("fileFormat");
        String fileName = String.format("%s.%s", tableName.toLowerCase(), fileFormat);
        Path p = Paths.get(workConf.getDataDir(), fileName);
        return p.toAbsolutePath().toString();
    }

    private static String[] fillTablePaths(WorkloadConfiguration workConf, String[] copySQL) {
        String[] result = new String[copySQL.length];
        result[0] = String.format(copySQL[0], getTablePath(workConf, "customer"));
        result[1] = String.format(copySQL[1], getTablePath(workConf, "lineitem"));
        result[2] = String.format(copySQL[2], getTablePath(workConf, "nation"));
        result[3] = String.format(copySQL[3], getTablePath(workConf, "orders"));
        result[4] = String.format(copySQL[4], getTablePath(workConf, "part"));
        result[5] = String.format(copySQL[5], getTablePath(workConf, "partsupp"));
        result[6] = String.format(copySQL[6], getTablePath(workConf, "region"));
        result[7] = String.format(copySQL[7], getTablePath(workConf, "supplier"));
        return result;
    }

    public static String[] copyPELOTON(WorkloadConfiguration workConf) {
        return fillTablePaths(workConf, new String[]{
                "COPY customer FROM '%s' WITH (DELIMITER '|')",
                "COPY lineitem FROM '%s' WITH (DELIMITER '|')",
                "COPY nation FROM '%s' WITH (DELIMITER '|')",
                "COPY orders FROM '%s' WITH (DELIMITER '|')",
                "COPY part FROM '%s' WITH (DELIMITER '|')",
                "COPY partsupp FROM '%s' WITH (DELIMITER '|')",
                "COPY region FROM '%s' WITH (DELIMITER '|')",
                "COPY supplier FROM '%s' WITH (DELIMITER '|')"
        });
    }

    public static String[] copyPOSTGRES(WorkloadConfiguration workConf, Connection conn, Logger LOG) {
        String[] tables = new String[8];
        Arrays.fill(tables, "%s");
        tables = fillTablePaths(workConf, tables);

        String[] copySQL = new String[]{
                "COPY customer FROM stdin WITH (DELIMITER '|');",
                "COPY lineitem FROM stdin WITH (DELIMITER '|');",
                "COPY nation FROM stdin WITH (DELIMITER '|');",
                "COPY orders FROM stdin WITH (DELIMITER '|');",
                "COPY part FROM stdin WITH (DELIMITER '|');",
                "COPY partsupp FROM stdin WITH (DELIMITER '|');",
                "COPY region FROM stdin WITH (DELIMITER '|');",
                "COPY supplier FROM stdin WITH (DELIMITER '|');"
        };

        try {
            CopyManager copy = new CopyManager(conn.unwrap(BaseConnection.class));
            for (int i = 0; i < 8; i++) {
                LOG.info("Executing " + copySQL[i]);
                copy.copyIn(copySQL[i], new StripEndInputStream(new FileInputStream(tables[i])));
            }
            LOG.info("Finished loading.");
            return new String[]{};
        } catch (IOException | SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static String[] copyMEMSQL(WorkloadConfiguration workConf) {
        return fillTablePaths(workConf, new String[]{
                "LOAD DATA INFILE \"%s\" INTO TABLE customer FIELDS TERMINATED BY '|' LINES TERMINATED BY '\\n' (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment, @);",
                "LOAD DATA INFILE \"%s\" INTO TABLE lineitem FIELDS TERMINATED BY '|' LINES TERMINATED BY '\\n' (l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment, @);",
                "LOAD DATA INFILE \"%s\" INTO TABLE nation FIELDS TERMINATED BY '|' LINES TERMINATED BY '\\n' (n_nationkey, n_name, n_regionkey, n_comment, @);",
                "LOAD DATA INFILE \"%s\" INTO TABLE orders FIELDS TERMINATED BY '|' LINES TERMINATED BY '\\n' (o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment, @);",
                "LOAD DATA INFILE \"%s\" INTO TABLE part FIELDS TERMINATED BY '|' LINES TERMINATED BY '\\n' (p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment, @);",
                "LOAD DATA INFILE \"%s\" INTO TABLE partsupp FIELDS TERMINATED BY '|' LINES TERMINATED BY '\\n' (ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment, @);",
                "LOAD DATA INFILE \"%s\" INTO TABLE region FIELDS TERMINATED BY '|' LINES TERMINATED BY '\\n' (r_regionkey, r_name, r_comment, @);",
                "LOAD DATA INFILE \"%s\" INTO TABLE supplier FIELDS TERMINATED BY '|' LINES TERMINATED BY '\\n' (s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment, @);",
        });
    }

    public static String[] copyMYSQL(WorkloadConfiguration workConf) {
        return fillTablePaths(workConf, new String[]{
                "LOAD DATA LOCAL INFILE \"%s\" INTO TABLE customer FIELDS TERMINATED BY '|' LINES TERMINATED BY '\\n' (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment, @DUMMY);",
                "LOAD DATA LOCAL INFILE \"%s\" INTO TABLE lineitem FIELDS TERMINATED BY '|' LINES TERMINATED BY '\\n' (l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment, @DUMMY);",
                "LOAD DATA LOCAL INFILE \"%s\" INTO TABLE nation FIELDS TERMINATED BY '|' LINES TERMINATED BY '\\n' (n_nationkey, n_name, n_regionkey, n_comment, @DUMMY);",
                "LOAD DATA LOCAL INFILE \"%s\" INTO TABLE orders FIELDS TERMINATED BY '|' LINES TERMINATED BY '\\n' (o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment, @DUMMY);",
                "LOAD DATA LOCAL INFILE \"%s\" INTO TABLE part FIELDS TERMINATED BY '|' LINES TERMINATED BY '\\n' (p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment, @DUMMY);",
                "LOAD DATA LOCAL INFILE \"%s\" INTO TABLE partsupp FIELDS TERMINATED BY '|' LINES TERMINATED BY '\\n' (ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment, @DUMMY);",
                "LOAD DATA LOCAL INFILE \"%s\" INTO TABLE region FIELDS TERMINATED BY '|' LINES TERMINATED BY '\\n' (r_regionkey, r_name, r_comment, @DUMMY);",
                "LOAD DATA LOCAL INFILE \"%s\" INTO TABLE supplier FIELDS TERMINATED BY '|' LINES TERMINATED BY '\\n' (s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment, @DUMMY);",
        });
    }

    private static class StripEndInputStream extends FilterInputStream {
        /**
         * To remove the | at the end of each line DBGEN creates.
         */

        private Integer curr = null;
        private Integer next = null;

        /**
         * Creates a <code>FilterInputStream</code>
         * by assigning the  argument <code>in</code>
         * to the field <code>this.in</code> so as
         * to remember it for later use.
         *
         * @param in the underlying input stream, or <code>null</code> if
         *           this instance is to be created without an underlying stream.
         */
        protected StripEndInputStream(InputStream in) {
            super(in);
        }

        @Override
        public int read() throws IOException {
            if (next == null) {
                next = super.read();
            }

            curr = next;
            next = super.read();

            if (next == '\n') {
                next = super.read();
                return '\n';
            } else {
                return curr;
            }
        }

        @Override
        public int read(byte b[], int off, int len) throws IOException {
            // identical to InputStream code, just that we want to use our read()
            if (b == null) {
                throw new NullPointerException();
            } else if (off < 0 || len < 0 || len > b.length - off) {
                throw new IndexOutOfBoundsException();
            } else if (len == 0) {
                return 0;
            }

            int c = read();
            if (c == -1) {
                return -1;
            }
            b[off] = (byte) c;

            int i = 1;
            try {
                for (; i < len; i++) {
                    c = read();
                    if (c == -1) {
                        break;
                    }
                    b[off + i] = (byte) c;
                }
            } catch (IOException ee) {
            }
            return i;
        }
    }
}
