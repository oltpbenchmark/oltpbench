/******************************************************************************
 *  Copyright 2016 by OLTPBenchmark Project  
 *  
 *  Author: Thamir Qadah                                 *
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

package com.oltpbenchmark.benchmarks.smallworldbank;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.smallworldbank.procedures.Balance;
import com.oltpbenchmark.benchmarks.voter.VoterLoader;
import com.oltpbenchmark.benchmarks.voter.VoterWorker;

public class SWBankBenchmark extends BenchmarkModule {
    
    private static final Logger LOG = Logger.getLogger(SWBankBenchmark.class);

    public SWBankBenchmark(WorkloadConfiguration workConf) {
        super("swbank", workConf, true);
        // TODO Auto-generated constructor stub        
    }

    @Override
    protected List<Worker> makeWorkersImpl(boolean verbose) throws IOException {
        LOG.info("Creating workers");
        List<Worker> workers = new ArrayList<Worker>();
        for (int i = 0; i < workConf.getTerminals(); ++i) {
            workers.add(new SWBankWorker(this, i));
        }
        return workers;
    }

    @Override
    protected Loader makeLoaderImpl(Connection conn) throws SQLException {
        return new SWBankLoader(this, conn);
    }

    @Override
    protected Package getProcedurePackageImpl() {
        // TODO Auto-generated method stub
        return Balance.class.getPackage();        
    }

}
