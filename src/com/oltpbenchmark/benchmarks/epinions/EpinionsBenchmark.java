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


package com.oltpbenchmark.benchmarks.epinions;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.epinions.procedures.GetAverageRatingByTrustedUser;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.SQLUtil;

public class EpinionsBenchmark extends BenchmarkModule {
    
    private static final Logger LOG = Logger.getLogger(EpinionsBenchmark.class);

    public EpinionsBenchmark(WorkloadConfiguration workConf) {
        super("epinions", workConf, true);
    }

    @Override
    protected Package getProcedurePackageImpl() {
        return GetAverageRatingByTrustedUser.class.getPackage();
    }

    @Override
    protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl(boolean verbose) throws IOException {
        List<Worker<? extends BenchmarkModule>> workers = new ArrayList<Worker<? extends BenchmarkModule>>();

        try {
            Connection metaConn = this.makeConnection();

            // LOADING FROM THE DATABASE IMPORTANT INFORMATION
            // LIST OF USERS

            Table t = this.catalog.getTable("USERACCT");
            assert (t != null) : "Invalid table name '" + t + "' " + this.catalog.getTables();

            String userCount = SQLUtil.selectColValues(this.workConf.getDBType(), t, "u_id");
            Statement stmt = metaConn.createStatement();
            ResultSet res = stmt.executeQuery(userCount);
            ArrayList<String> user_ids = new ArrayList<String>();
            while (res.next()) {
                user_ids.add(res.getString(1));
            }
            res.close();
            if(LOG.isDebugEnabled()) LOG.debug("Loaded: "+user_ids.size()+" User ids");
            // LIST OF ITEMS AND
            t = this.catalog.getTable("ITEM");
            assert (t != null) : "Invalid table name '" + t + "' " + this.catalog.getTables();
            String itemCount = SQLUtil.selectColValues(this.workConf.getDBType(), t, "i_id");
            res = stmt.executeQuery(itemCount);
            ArrayList<String> item_ids = new ArrayList<String>();
            while (res.next()) {
                item_ids.add(res.getString(1));
            }
            res.close();
            if(LOG.isDebugEnabled()) LOG.debug("Loaded: "+item_ids.size()+" Item ids");
            metaConn.close();
            // Now create the workers.
            for (int i = 0; i < workConf.getTerminals(); ++i) {
                workers.add(new EpinionsWorker(this, i, user_ids, item_ids));
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return workers;
    }

    @Override
    protected Loader<EpinionsBenchmark> makeLoaderImpl(Connection conn) throws SQLException {
        return new EpinionsLoader(this, conn);
    }

}
