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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.SubnodeConfiguration;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.log4j.Logger;

import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.smallworldbank.ids.IDMessageSender;
import com.oltpbenchmark.benchmarks.smallworldbank.procedures.Balance;
import com.oltpbenchmark.benchmarks.smallworldbank.procedures.Collect;
import com.oltpbenchmark.benchmarks.smallworldbank.procedures.Distribute;
import com.oltpbenchmark.benchmarks.smallworldbank.procedures.InvalidTransactionTypeException;
import com.oltpbenchmark.benchmarks.smallworldbank.procedures.ListCountries;
import com.oltpbenchmark.benchmarks.smallworldbank.procedures.MCollect;
import com.oltpbenchmark.benchmarks.smallworldbank.procedures.MDistribute;
import com.oltpbenchmark.benchmarks.smallworldbank.procedures.MSendPayment;
import com.oltpbenchmark.benchmarks.smallworldbank.procedures.ReportActiveCustomers;
import com.oltpbenchmark.benchmarks.smallworldbank.procedures.ReportCheckingPerBranch;
import com.oltpbenchmark.benchmarks.smallworldbank.procedures.ReportCheckingPerBranch2;
import com.oltpbenchmark.benchmarks.smallworldbank.procedures.ReportSavingPerBranch;
import com.oltpbenchmark.benchmarks.smallworldbank.procedures.SendPayment;
import com.oltpbenchmark.types.TransactionStatus;
import com.oltpbenchmark.util.AIMSLogger;

public class SWBankWorker extends Worker {

    private static final Logger LOG = Logger.getLogger(SWBankWorker.class);
    private long custIdMax = -1;
    private long custIdMin = -1;
    private long acctIdMax = -1;
    private long acctIdMin = -1;
    private double tdp = 0.0; // transaction dependence probability, the
                              // probability of connecting to (consider last for
                              // now)
                              // previously executed transaction

    private int mddelay = 5; // default detection delay is 5 msec for malicious
                             // txns

    private long lastAcctId = -1;
    private long lastCustId = -1;

    private ArrayList<Long> lastAcctIds = new ArrayList<Long>();
    private ArrayList<Long> lastCustIds = new ArrayList<Long>();

    // private static final RandomDataGenerator rdg = new RandomDataGenerator(
    // new JDKRandomGenerator());
    private RandomDataGenerator rdg = null;

    // Executor service for IDS simulationa
    private ScheduledExecutorService exservice;

    @SuppressWarnings("unchecked")
    public SWBankWorker(BenchmarkModule benchmarkModule, int id) {
        super(benchmarkModule, id);

        exservice = Executors.newScheduledThreadPool(3);

        rdg = new RandomDataGenerator(new JDKRandomGenerator(id));
        // TODO: Get Dataset statistics:
        int noWorkers = benchmarkModule.getWorkloadConfiguration().getTerminals();
        tdp = benchmarkModule.getWorkloadConfiguration().getXmlConfig().getDouble("tdp");

        mddelay = benchmarkModule.getWorkloadConfiguration().getXmlConfig().getInt("mddelay");

        try {
            long _custIdMax = SWBankUtil.getCustIdMax(this.conn);
            long _acctIdMax = SWBankUtil.getAcctIdMax(this.conn);
            long custRange = _custIdMax / noWorkers;
            long acctRange = _acctIdMax / noWorkers;

            custIdMin = (id * custRange);
            custIdMax = custIdMin + custRange;

            acctIdMin = (id * acctRange);
            acctIdMax = acctIdMin + acctRange;

            LOG.info(String.format("workerId=%d, custIdMin=%d, custIdMax=%d, custRange=%d,  " + "acctIdMin=%d, acctIdMax=%d, acctRange=%d, tdp = %f ", id, custIdMin, custIdMax, custRange, acctIdMin,
                    acctIdMax, acctRange, tdp));

        } catch (SQLException e) {
            // TODO: handle exception
            e.printStackTrace();
        }

        // parse specific properties of transaction types

    }

    @SuppressWarnings("unchecked")
    @Override
    protected TransactionStatus executeWork(TransactionType txnType) throws UserAbortException, SQLException {
        // LOG.info("Num Proc. "+Runtime.getRuntime().availableProcessors());
        long txid = -1;
        boolean isMalicous = false;

        try {

            if (txnType.getProcedureClass().equals(Balance.class)) {
                // LOG.info("Executing Balance transaction");
                Balance proc = getProcedure(Balance.class);
                assert (proc != null);

                long acctId = rdg.nextLong(acctIdMin + 1, acctIdMax);
                // LOG.info("Executing "+txnType.getProcedureClass().getName());
                proc.run(conn, acctId);

                AIMSLogger.logTransactionSpecs(1, String.format("%d,%d", acctId, this.getId()));

            } else if (txnType.getProcedureClass().equals(SendPayment.class) || txnType.getProcedureClass().equals(MSendPayment.class)) {

                if (txnType.getProcedureClass().equals(MSendPayment.class)) {
                    isMalicous = true;
                }

                // generate random parameters
                long srcCustId = rdg.nextLong(custIdMin, custIdMax);
                long destCustId = rdg.nextLong(custIdMin, custIdMax);

                // make sure that it is not a self transfer
                while (destCustId == srcCustId) {
                    destCustId = rdg.nextLong(custIdMin, custIdMax);
                }

                if (lastCustId > 0) { // this is not the first transaction
                                      // executed
                    int coinflip = rdg.nextBinomial(1, tdp);
                    if (coinflip == 1) {
                        // connect
                        srcCustId = lastCustId;
                        // LOG.info(String.format("coinflip = %d",coinflip));
                    }
                }

                float balv = (float) rdg.nextUniform(0, 1) * 100;

                SendPayment proc = getProcedure(SendPayment.class);
                assert (proc != null);

                // LOG.info("Executing "+txnType.getProcedureClass().getName());
                txid = proc.run(conn, srcCustId, destCustId, balv, isMalicous);

                // lastCustIds.add(destCustId);
                lastCustId = srcCustId;
                AIMSLogger.logTransactionSpecs(2, String.format("%d,%d,%3f,%d", srcCustId, destCustId, balv, this.getId()));

            } else if (txnType.getProcedureClass().equals(Distribute.class)
                    || txnType.getProcedureClass().equals(MDistribute.class)) {
                List<SubnodeConfiguration> sncs = this.getBenchmarkModule().getWorkloadConfiguration().getXmlConfig().configurationsAt("transactiontypes/transactiontype[name='Distribute']");

                if (txnType.getProcedureClass().equals(MDistribute.class)) {
                    isMalicous = true;
                }
                
                if (sncs.size() != 1) {
                    throw new RuntimeException("Duplicate transaction types: Distribute");
                }

                assert (sncs.size() == 1);
                // LOG.info("size of sncs:"+sncs.size());

                SubnodeConfiguration snc = sncs.get(0);

                int fanout_min = snc.getInt("fanout_min");
                int fanout_max = snc.getInt("fanout_max");

                // int fanout_mu = snc.getInt("fanout_mu");
                // int fanout_sigma = snc.getInt("fanout_sigma");

                int fanout_n = snc.getInt("fanout_n");
                double fanout_p = snc.getDouble("fanout_p");

                // LOG.info("Distribute:" + snc.getInt("fanout_mu"));
                // LOG.info("Distribute:" + snc.getInt("fanout_sigma"));
                // LOG.info(String.format("n = %d, p= %f",fanout_n, fanout_p));
                int fanout_val = rdg.nextBinomial(fanout_n, fanout_p);
                // LOG.info(String.format("max = %d, min= %d", fanout_max,
                // fanout_min));
                // int fanout_val = rdg.nextInt(fanout_min, fanout_max);
                // LOG.info("here");

                if (fanout_val < fanout_min) {
                    fanout_val = fanout_min;
                }

                if (fanout_val > fanout_max) {
                    fanout_val = fanout_max;
                }

                // LOG.info("Random Fanout = " + fanout_val);

                long[] accIds = nextDistinctKLongs(acctIdMin, acctIdMax, fanout_val + 1);
                long[] dest_accids = Arrays.copyOfRange(accIds, 1, accIds.length);
                double[] dest_vals = nextKDoubles(0, 1, 100, fanout_val);

                Distribute proc = getProcedure(Distribute.class);

                txid = proc.run(conn, accIds[0], dest_accids, dest_vals, isMalicous);

                // LOG.info(snc.getString("name"));
                // LOG.info(snc.getInt("id"));
                // if (snc.getString("name").equalsIgnoreCase("Distribute")) {
                // LOG.info("Distribute:" + snc.getInt("fanin"));
                // LOG.info("Distribute:" + snc.getInt("fanout"));
                // }
                StringBuilder sb1 = new StringBuilder();
                StringBuilder sb2 = new StringBuilder();
                for (int i = 0; i < dest_vals.length; i++) {
                    if (i != 0) {
                        sb1.append(':');
                        sb2.append(':');
                    }
                    sb1.append(dest_accids[i]);
                    sb2.append(String.format("%3f", dest_vals[i]));
                }

                AIMSLogger.logTransactionSpecs(3, String.format("%d,%s,%s,%d", accIds[0], sb1.toString(), sb2.toString(), this.getId()));
                
                
            } else if (txnType.getProcedureClass().equals(Collect.class)
                    || txnType.getProcedureClass().equals(MCollect.class)) {

                if (txnType.getProcedureClass().equals(MCollect.class)) {
                    isMalicous = true;
                }
                List<SubnodeConfiguration> sncs = this.getBenchmarkModule().getWorkloadConfiguration().getXmlConfig().configurationsAt("transactiontypes/transactiontype[name='Collect']");

                if (sncs.size() != 1) {
                    throw new RuntimeException("Duplicate transaction types: Collect");
                }
                assert (sncs.size() == 1);
                SubnodeConfiguration snc = sncs.get(0);

                int fanin_min = snc.getInt("fanin_min");
                int fanin_max = snc.getInt("fanin_max");

                int fanin_n = snc.getInt("fanin_n");
                double fanin_p = snc.getDouble("fanin_p");

                int fanin_val = rdg.nextBinomial(fanin_n, fanin_p);

                if (fanin_val < fanin_min) {
                    fanin_val = fanin_min;
                }

                if (fanin_val > fanin_max) {
                    fanin_val = fanin_max;
                }

                // LOG.info("Random Fanin = " + fanin_val);

                long[] accIds = nextDistinctKLongs(acctIdMin, acctIdMax, fanin_val + 1);
                long[] src_accids = Arrays.copyOfRange(accIds, 1, accIds.length);
                double[] src_vals = nextKDoubles(0, 1, 100, fanin_val);

                Collect proc = getProcedure(Collect.class);

                txid = proc.run(conn, src_accids, accIds[0], src_vals, isMalicous);
                
                StringBuilder sb1 = new StringBuilder();
                StringBuilder sb2 = new StringBuilder();
                for (int i = 0; i < src_vals.length; i++) {
                    if (i != 0) {
                        sb1.append(':');
                        sb2.append(':');
                    }
                    sb1.append(src_accids[i]);
                    sb2.append(String.format("%3f", src_vals[i]));
                }

                AIMSLogger.logTransactionSpecs(4, String.format("%s,%d,%s,%d", sb1.toString(), accIds[0], sb2.toString(), this.getId()));

            } else if (txnType.getProcedureClass().equals(ReportCheckingPerBranch.class)) {
                ReportCheckingPerBranch proc = getProcedure(ReportCheckingPerBranch.class);
                long b_count = (Math.round(SWBankConstants.BRANCH_PER_COUNTRY * this.getWorkloadConfiguration().getScaleFactor()) * 100);
                // LOG.info("CHECK: b_count = "+b_count);
                proc.run(conn, b_count);

            } else if (txnType.getProcedureClass().equals(ReportCheckingPerBranch2.class)) {
                ReportCheckingPerBranch2 proc = getProcedure(ReportCheckingPerBranch2.class);
                long b_count = (Math.round(SWBankConstants.BRANCH_PER_COUNTRY * this.getWorkloadConfiguration().getScaleFactor()) * 100);
                // LOG.info("CHECK: b_count = "+b_count);
                proc.run(conn, b_count);

            } else if (txnType.getProcedureClass().equals(ReportSavingPerBranch.class)) {

                ReportSavingPerBranch proc = getProcedure(ReportSavingPerBranch.class);
                long b_count = (Math.round(SWBankConstants.BRANCH_PER_COUNTRY * this.getWorkloadConfiguration().getScaleFactor()) * 100);

                // LOG.info("SAV: b_count = "+b_count);
                proc.run(conn, b_count);

            } else if (txnType.getProcedureClass().equals(ReportActiveCustomers.class)) {
                ReportActiveCustomers proc = getProcedure(ReportActiveCustomers.class);
                proc.run(conn);

            } else if (txnType.getProcedureClass().equals(ListCountries.class)) {
                ListCountries proc = getProcedure(ListCountries.class);
                proc.run(conn);
            } else {

                throw new InvalidTransactionTypeException("Invalid transaction type: " + txnType.getProcedureClass().getName());

            }
        } catch (InvalidTransactionTypeException e) {

            e.printStackTrace();
            return TransactionStatus.RETRY_DIFFERENT;

        } catch (RuntimeException e) {
            conn.rollback();
            // LOG.info("Unknown transaction type");
            e.printStackTrace();
            return TransactionStatus.RETRY;
        }

        conn.commit();

        // send ids message after commiting transaction
        if (isMalicous) {
            LOG.info(String.format("Sending alert in %d msec",mddelay));
            exservice.schedule(new IDMessageSender(conn,txid), mddelay, TimeUnit.MILLISECONDS);
        }
        return TransactionStatus.SUCCESS;

    }

    // returns k distinct long values
    private long[] nextDistinctKLongs(long min, long max, int k) {
        long[] res = new long[k];
        int i = 0;
        res[i] = rdg.nextLong(min, max);
        i++;

        while (i < k) {
            res[i] = rdg.nextLong(min, max);
            while (res[i] == res[i - 1]) {
                res[i] = rdg.nextLong(min, max);
            }
            i++;
        }

        return res;
    }

    // returns random k doubles
    private double[] nextKDoubles(double lower, double upper, double base, int k) {
        double[] res = new double[k];
        int i = 0;

        while (i < k) {
            res[i] = rdg.nextUniform(lower, upper) * base;
            i++;
        }

        return res;
    }

    /*
     * (non-Javadoc)
     * @see com.oltpbenchmark.api.Worker#tearDown(boolean)
     */
    @Override
    public void tearDown(boolean error) {
        super.tearDown(error);

        // gracefully shutdown executor service
        // make sure all IDS are delivered.

        exservice.shutdown();

    }

}
