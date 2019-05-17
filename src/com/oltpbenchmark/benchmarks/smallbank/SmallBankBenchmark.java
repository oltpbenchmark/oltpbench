package com.oltpbenchmark.benchmarks.smallbank;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.smallbank.procedures.Amalgamate;
import com.oltpbenchmark.catalog.Column;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.SQLUtil;

public class SmallBankBenchmark extends BenchmarkModule {

    protected final long numAccounts;
    
    public SmallBankBenchmark(WorkloadConfiguration workConf) {
        super("smallbank", workConf, true);
        this.numAccounts = (int)Math.round(SmallBankConstants.NUM_ACCOUNTS * workConf.getScaleFactor());
    }

    @Override
    protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl(boolean verbose) throws IOException {
        List<Worker<? extends BenchmarkModule>> workers = new ArrayList<Worker<? extends BenchmarkModule>>();
        for (int i = 0; i < workConf.getTerminals(); ++i) {
            workers.add(new SmallBankWorker(this, i));
        }
        return workers;
    }

    @Override
    protected Loader<SmallBankBenchmark> makeLoaderImpl() throws SQLException {
        return new SmallBankLoader(this);
    }

    @Override
    protected Package getProcedurePackageImpl() {
       return Amalgamate.class.getPackage();
    }
    
    
    /**
     * For the given table, return the length of the first VARCHAR attribute
     * @param acctsTbl
     * @return
     */
    public static int getCustomerNameLength(Table acctsTbl) {
        int acctNameLength = -1;
        for (Column col : acctsTbl.getColumns()) {
            if (SQLUtil.isStringType(col.getType())) {
                acctNameLength = col.getSize();
                break;
            }
        } // FOR
        assert(acctNameLength > 0);
        return (acctNameLength);
    }

}
