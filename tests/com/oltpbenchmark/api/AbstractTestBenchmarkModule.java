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
package com.oltpbenchmark.api;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.oltpbenchmark.catalog.Table;

public abstract class AbstractTestBenchmarkModule<T extends BenchmarkModule> extends AbstractTestCase<T> {

    protected static final int NUM_TERMINALS = 10;
    
    /**
     * testGetDatabaseDDL
     */
    public void testGetDatabaseDDL() throws Exception {
        File ddl = this.benchmark.getDatabaseDDL(null);
        assertNotNull(ddl);
        assert (ddl.exists());
    }

    /**
     * testLoadDatabase
     */
    public void testCreateDatabase() throws Exception {
        this.benchmark.createDatabase();

        // Make sure that we get back some tables
        Map<String, Table> tables = this.benchmark.getTables(this.benchmark.getConnection());
        assertNotNull(tables);
        assertFalse(tables.isEmpty());

        // Just make sure that there are no empty tables
        for (Entry<String, Table> e : tables.entrySet()) {
            assert (e.getValue().getColumnCount() > 0) : "Missing columns for " + e.getValue();
            System.err.println(e.getValue());
        } // FOR
    }
    
    /**
     * testGetTransactionType
     */
    public void testGetTransactionType() throws Exception {
        int id = 1;
        for (Class<? extends Procedure> procClass: this.procClasses) {
            assertNotNull(procClass);
            String procName = procClass.getSimpleName();
            TransactionType txnType = this.benchmark.getTransactionType(procName, id++);
            assertNotNull(txnType);
            assertEquals(procClass, txnType.getProcedureClass());
            System.err.println(procClass + " -> " + txnType);
        } // FOR
    }
    
    /**
     * testGetTransactionTypeInvalidId
     */
    public void testGetTransactionTypeInvalidId() throws Exception {
        Class<? extends Procedure> procClass = this.procClasses.get(0);
        assertNotNull(procClass);
        String procName = procClass.getSimpleName();
        TransactionType txnType = null;
        try {
            txnType = this.benchmark.getTransactionType(procName, TransactionType.INVALID_ID);
        } catch (Throwable ex) {
            // Ignore
        }
        assertNull(txnType);
    }
    
    /**
     * testMakeWorkers
     */
//    public void testMakeWorkers() throws Exception {
//        this.workConf.setTerminals(NUM_TERMINALS);
//        List<Worker> workers = this.benchmark.makeWorkers(false);
//        assertNotNull(workers);
//        assertEquals(NUM_TERMINALS, workers.size());
//        assertNotNull(workers.get(0));
//    }
}
