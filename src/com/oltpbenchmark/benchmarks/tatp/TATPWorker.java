/*******************************************************************************
 * oltpbenchmark.com
 *  
 *  Project Info:  http://oltpbenchmark.com
 *  Project Members:    Carlo Curino <carlo.curino@gmail.com>
 *              Evan Jones <ej@evanjones.ca>
 *              DIFALLAH Djellel Eddine <djelleleddine.difallah@unifr.ch>
 *              Andy Pavlo <pavlo@cs.brown.edu>
 *              CUDRE-MAUROUX Philippe <philippe.cudre-mauroux@unifr.ch>  
 *                  Yang Zhang <yaaang@gmail.com> 
 * 
 *  This library is free software; you can redistribute it and/or modify it under the terms
 *  of the GNU General Public License as published by the Free Software Foundation;
 *  either version 3.0 of the License, or (at your option) any later version.
 * 
 *  This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  See the GNU Lesser General Public License for more details.
 ******************************************************************************/
package com.oltpbenchmark.benchmarks.tatp;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.benchmarks.tatp.procedures.DeleteCallForwarding;
import com.oltpbenchmark.benchmarks.tatp.procedures.GetAccessData;
import com.oltpbenchmark.benchmarks.tatp.procedures.GetNewDestination;
import com.oltpbenchmark.benchmarks.tatp.procedures.GetSubscriberData;
import com.oltpbenchmark.benchmarks.tatp.procedures.InsertCallForwarding;
import com.oltpbenchmark.benchmarks.tatp.procedures.UpdateLocation;
import com.oltpbenchmark.benchmarks.tatp.procedures.UpdateSubscriberData;
import com.oltpbenchmark.types.TransactionStatus;

public class TATPWorker extends Worker {
	private static final Logger LOG = Logger.getLogger(TATPWorker.class);
	
    /**
     * Each Transaction element provides an TransactionInvoker to create the proper
     * arguments used to invoke the stored procedure
     */
    private static interface TransactionInvoker<T extends Procedure> {
        /**
         * Generate the proper arguments used to invoke the given stored procedure
         * @param subscriberSize
         * @return
         */
        public void invoke(Connection conn, Procedure proc, long subscriberSize) throws SQLException;
    }
    
    /**
     * Set of transactions structs with their appropriate parameters
     */
    public static enum Transaction {
    	DeleteCallForwarding(new TransactionInvoker<DeleteCallForwarding>() {
            public void invoke(Connection conn, Procedure proc, long subscriberSize) throws SQLException {
            	long s_id = TATPUtil.getSubscriberId(subscriberSize);
            	((DeleteCallForwarding)proc).run(
            			 conn,
            			 TATPUtil.padWithZero(s_id), // s_id
                         TATPUtil.number(1, 4).byteValue(), // sf_type
                         (byte)(8 * TATPUtil.number(0, 2)) // start_time
                );
            }
        }),
        GetAccessData(new TransactionInvoker<GetAccessData>() {
            public void invoke(Connection conn, Procedure proc, long subscriberSize) throws SQLException {
                long s_id = TATPUtil.getSubscriberId(subscriberSize);
                ((GetAccessData)proc).run(
                		conn,
                        s_id, // s_id
                        TATPUtil.number(1, 4).byteValue() // ai_type
                );
            }
        }),
        GetNewDestination(new TransactionInvoker<GetNewDestination>() {
            public void invoke(Connection conn, Procedure proc, long subscriberSize) throws SQLException {
                long s_id = TATPUtil.getSubscriberId(subscriberSize);
                ((GetNewDestination)proc).run(
                		conn,
                        s_id, // s_id
                        TATPUtil.number(1, 4).byteValue(), // sf_type
                        (byte)(8 * TATPUtil.number(0, 2)), // start_time
                        TATPUtil.number(1, 24).byteValue() // end_time
                );
            }
        }),
        GetSubscriberData(new TransactionInvoker<GetSubscriberData>() {
            public void invoke(Connection conn, Procedure proc, long subscriberSize) throws SQLException {
                long s_id = TATPUtil.getSubscriberId(subscriberSize);
                ((GetSubscriberData)proc).run(
                		conn,
                		s_id // s_id
                );
            }
        }),
        InsertCallForwarding(new TransactionInvoker<InsertCallForwarding>() {
            public void invoke(Connection conn, Procedure proc, long subscriberSize) throws SQLException {
                long s_id = TATPUtil.getSubscriberId(subscriberSize);
                ((InsertCallForwarding)proc).run(
                		conn,
                        TATPUtil.padWithZero(s_id), // sub_nbr
                        TATPUtil.number(1, 4).byteValue(), // sf_type
                        (byte)(8 * TATPUtil.number(0, 2)), // start_time
                        TATPUtil.number(1, 24).byteValue(), // end_time
                        TATPUtil.padWithZero(s_id) // numberx
                );
            }
        }),
        UpdateLocation(new TransactionInvoker<UpdateLocation>() {
            public void invoke(Connection conn, Procedure proc, long subscriberSize) throws SQLException {
                long s_id = TATPUtil.getSubscriberId(subscriberSize);
                ((UpdateLocation)proc).run(
                		conn,
                        TATPUtil.number(0, Integer.MAX_VALUE).intValue(), // vlr_location
                        TATPUtil.padWithZero(s_id) // sub_nbr
                );
            }
        }),
        UpdateSubscriberData(new TransactionInvoker<UpdateSubscriberData>() {
            public void invoke(Connection conn, Procedure proc, long subscriberSize) throws SQLException {
                long s_id = TATPUtil.getSubscriberId(subscriberSize);
                ((UpdateSubscriberData)proc).run(
                		conn,
                        s_id, // s_id
                        TATPUtil.number(0, 1).byteValue(), // bit_1
                        TATPUtil.number(0, 255).shortValue(), // data_a
                        TATPUtil.number(1, 4).byteValue() // sf_type
                );
            }
        }),
        ; // END LIST OF STORED PROCEDURES
        
        /**
         * Constructor
         */
        private Transaction(TransactionInvoker<? extends Procedure> ag) {
            this.generator = ag;
        }
        
        public final TransactionInvoker<? extends Procedure> generator;
        
        protected static final Map<Integer, Transaction> idx_lookup = new HashMap<Integer, Transaction>();
        protected static final Map<String, Transaction> name_lookup = new HashMap<String, Transaction>();
        static {
            for (Transaction vt : EnumSet.allOf(Transaction.class)) {
                Transaction.idx_lookup.put(vt.ordinal(), vt);
                Transaction.name_lookup.put(vt.name().toUpperCase(), vt);
            }
        }
        
        public static Transaction get(String name) {
            Transaction ret = Transaction.name_lookup.get(name.toUpperCase());
            return (ret);
        }
        
        public void invoke(Connection conn, Procedure proc, long subscriberSize) throws SQLException {
        	this.generator.invoke(conn, proc, subscriberSize);
        }
        
    } // TRANSCTION ENUM
    
    private final long subscriberSize;
	
	public TATPWorker(int id, TATPBenchmark benchmarkModule) {
		super(benchmarkModule, id);
		this.subscriberSize = Math.round(TATPConstants.DEFAULT_NUM_SUBSCRIBERS * benchmarkModule.getWorkloadConfiguration().getScaleFactor());
	}
	
	@Override
	protected TransactionStatus executeWork(TransactionType txnType) throws UserAbortException, SQLException {
	    Transaction t = Transaction.get(txnType.getName());
        assert(t != null) : "Unexpected " + txnType;
        
        // Get the Procedure handle
        Procedure proc = this.getProcedure(txnType);
        assert(proc != null) : String.format("Failed to get Procedure handle for %s.%s",
                                             this.getBenchmarkModule().getBenchmarkName(), txnType);
        if (LOG.isDebugEnabled()) LOG.debug("Executing " + proc);
        
        t.invoke(this.conn, proc, subscriberSize);
        conn.commit();
        return (TransactionStatus.SUCCESS);
	}

}
