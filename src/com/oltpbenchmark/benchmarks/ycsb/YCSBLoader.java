package com.oltpbenchmark.benchmarks.ycsb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderUtil;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.SQLUtil;

public class YCSBLoader extends Loader {
    private static final Logger LOG = Logger.getLogger(YCSBLoader.class);
    private final int num_record;

    public YCSBLoader(YCSBBenchmark benchmark, Connection c) {
        super(benchmark, c);
        this.num_record = (int) Math.round(YCSBConstants.RECORD_COUNT * this.scaleFactor);
        if (LOG.isDebugEnabled()) {
            LOG.debug("# of RECORDS:  " + this.num_record);
        }
    }

    @Override
    public void load() throws SQLException {
        Table catalog_tbl = this.getTableCatalog("USERTABLE");
        assert (catalog_tbl != null);
        
        String sql = SQLUtil.getInsertSQL(catalog_tbl);
        PreparedStatement stmt = this.conn.prepareStatement(sql);
        long total = 0;
        int batch = 0;
        for (int i = 0; i < this.num_record; i++) {
            stmt.setInt(1, i);
            for (int j = 2; j <= 11; j++) {
                stmt.setString(j, LoaderUtil.randomStr(100));
            }
            stmt.addBatch();
            total++;
            if (++batch >= YCSBConstants.configCommitCount) {
                int result[] = stmt.executeBatch();
                assert (result != null);
                conn.commit();
                batch = 0;
                if (LOG.isDebugEnabled())
                    LOG.debug(String.format("Records Loaded %d / %d", total, this.num_record));
            }
        } // FOR
        if (batch > 0) {
            stmt.executeBatch();
            if (LOG.isDebugEnabled())
                LOG.debug(String.format("Records Loaded %d / %d", total, this.num_record));
        }
        if (LOG.isDebugEnabled()) LOG.debug("Finished loading " + catalog_tbl.getName());
    }
}
