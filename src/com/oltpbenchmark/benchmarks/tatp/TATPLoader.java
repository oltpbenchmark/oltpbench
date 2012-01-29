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
/***************************************************************************
 *  Copyright (C) 2009 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Original Version:                                                      *
 *  Zhe Zhang (zhe@cs.brown.edu)                                           *
 *                                                                         *
 *  Modifications by:                                                      *
 *  Andy Pavlo (pavlo@cs.brown.edu)                                        *
 *  http://www.cs.brown.edu/~pavlo/                                        *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
package com.oltpbenchmark.benchmarks.tatp;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.catalog.*;
import com.oltpbenchmark.util.SQLUtil;

public class TATPLoader extends Loader {
    private static final Logger LOG = Logger.getLogger(TATPLoader.class);
    
    private final long subscriberSize;
    private final int batchSize = 100; // FIXME
    private final boolean blocking = true; // FIXME
    
    public TATPLoader(TATPBenchmark benchmark, Connection c) {
    	super(benchmark, c);
    	this.subscriberSize = Math.round(TATPConstants.DEFAULT_NUM_SUBSCRIBERS * this.scaleFactor);
        if (LOG.isDebugEnabled()) LOG.debug("CONSTRUCTOR: " + TATPLoader.class.getName());
    }

    @Override
    public void load() {
        if (LOG.isDebugEnabled()) LOG.debug("Starting TATPLoader [subscriberSize=" + subscriberSize + ",scaleFactor=" + scaleFactor + "]");
        
        Thread threads[] = new Thread[] {
            new Thread() {
                public void run() {
                    if (LOG.isDebugEnabled()) LOG.debug("Start loading " + TATPConstants.TABLENAME_SUBSCRIBER);
                    Table catalog_tbl = getTableCatalog(TATPConstants.TABLENAME_SUBSCRIBER);
                    try {
                    	genSubscriber(catalog_tbl);
                    } catch (SQLException ex) {
                    	LOG.error("Failed to load data for " + TATPConstants.TABLENAME_SUBSCRIBER, ex);
                    	throw new RuntimeException(ex);
                    }
                    if (LOG.isDebugEnabled()) LOG.debug("Finished loading " + TATPConstants.TABLENAME_SUBSCRIBER);
                }
            },
            new Thread() {
                public void run() {
                    if (LOG.isDebugEnabled()) LOG.debug("Start loading " + TATPConstants.TABLENAME_ACCESS_INFO);
                    Table catalog_tbl = getTableCatalog(TATPConstants.TABLENAME_ACCESS_INFO);
                    try {
                    	genAccessInfo(catalog_tbl);
                    } catch (SQLException ex) {
                    	LOG.error("Failed to load data for " + TATPConstants.TABLENAME_ACCESS_INFO, ex);
                    	throw new RuntimeException(ex);
                    }
                    if (LOG.isDebugEnabled()) LOG.debug("Finished loading " + TATPConstants.TABLENAME_ACCESS_INFO);
                }
            },
            new Thread() {
                public void run() {
                    if (LOG.isDebugEnabled()) LOG.debug("Start loading " + TATPConstants.TABLENAME_SPECIAL_FACILITY + " and " + TATPConstants.TABLENAME_CALL_FORWARDING);
                    Table catalog_spe = getTableCatalog(TATPConstants.TABLENAME_SPECIAL_FACILITY);
                    Table catalog_cal = getTableCatalog(TATPConstants.TABLENAME_CALL_FORWARDING);
                    try {
                    	genSpeAndCal(catalog_spe, catalog_cal);
                    } catch (SQLException ex) {
                    	LOG.error("Failed to load data for " + TATPConstants.TABLENAME_SPECIAL_FACILITY + " and " + TATPConstants.TABLENAME_CALL_FORWARDING, ex);
                    	throw new RuntimeException(ex);
                    }
                    if (LOG.isDebugEnabled()) LOG.debug("Finished loading " + TATPConstants.TABLENAME_SPECIAL_FACILITY + " and " + TATPConstants.TABLENAME_CALL_FORWARDING);
                }
            }
        };

        try {
            for (Thread t : threads) {
                t.start();
                if (blocking)
                    t.join();
            } // FOR
            if (!blocking) {
                for (Thread t : threads)
                    t.join();
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to complete TATP loading phase", e);
        }
        if (LOG.isDebugEnabled()) LOG.debug("TATP loader done. ");
    }

    /**
     * Populate Subscriber table per benchmark spec.
     */
    void genSubscriber(Table catalog_tbl) throws SQLException {
        // Create a prepared statement
        String sql = SQLUtil.getInsertSQL(catalog_tbl);
        PreparedStatement pstmt = this.conn.prepareStatement(sql);

        long s_id = 0;
        long total = 0;
        int batch = 0;
        while (s_id++ < subscriberSize) {
            int col = 0;
            
            pstmt.setLong(++col, s_id);
            pstmt.setString(++col, TATPUtil.padWithZero((Long) s_id));
            
            // BIT_##
            for (int j = 0; j < 10; j++) {
            	pstmt.setByte(++col, TATPUtil.number(0, 1).byteValue());
            }
            // HEX_##
            for (int j = 0; j < 10; j++) {
            	pstmt.setByte(++col, TATPUtil.number(0, 15).byteValue());
            }
            // BYTE2_##
            for (int j = 0; j < 10; j++) {
            	pstmt.setShort(++col, TATPUtil.number(0, 255).shortValue());
            }
            // msc_location + vlr_location
            for (int j = 0; j < 2; j++) {
            	pstmt.setInt(++col, TATPUtil.number(0, Integer.MAX_VALUE).intValue());
            }
            total++;
            pstmt.addBatch();
            
            if (++batch >= this.batchSize) {
                if (LOG.isDebugEnabled()) LOG.debug(String.format("%s: %6d / %d", catalog_tbl.getName(), total, subscriberSize));
                int results[] = pstmt.executeBatch();
                assert(results != null);
                batch = 0;
            }
        } // WHILE
        if (batch > 0) {
        	if (LOG.isDebugEnabled()) LOG.debug(String.format("%s: %6d / %d", catalog_tbl.getName(), total, subscriberSize));
            int results[] = pstmt.executeBatch();
            assert(results != null);
        }
    }

    /**
     * Populate Access_Info table per benchmark spec.
     */
    void genAccessInfo(Table catalog_tbl) throws SQLException {
    	// Create a prepared statement
        String sql = SQLUtil.getInsertSQL(catalog_tbl);
        PreparedStatement pstmt = this.conn.prepareStatement(sql);
    	
        int s_id = 0;
        int[] arr = { 1, 2, 3, 4 };

        int[] ai_types = TATPUtil.subArr(arr, 1, 4);
        long total = 0;
        int batch = 0;
        while (s_id++ < subscriberSize) {
            for (int ai_type : ai_types) {
            	int col = 0;
            	pstmt.setLong(++col, s_id);
                pstmt.setByte(++col, (byte)ai_type);
                pstmt.setShort(++col, TATPUtil.number(0, 255).shortValue());
        		pstmt.setShort(++col, TATPUtil.number(0, 255).shortValue());
				pstmt.setString(++col, TATPUtil.astring(3, 3));
				pstmt.setString(++col, TATPUtil.astring(5, 5));
				pstmt.addBatch();
				batch++;
                total++;
            } // FOR
            if (batch >= batchSize) {
                if (LOG.isDebugEnabled()) LOG.debug(String.format("%s: %6d / %d", TATPConstants.TABLENAME_ACCESS_INFO, total, ai_types.length * subscriberSize));
                int results[] = pstmt.executeBatch();
                assert(results != null);
                conn.commit();
                batch = 0;
            }
        } // WHILE
        if (batch > 0) {
        	if (LOG.isDebugEnabled()) LOG.debug(String.format("%s: %6d / %d", TATPConstants.TABLENAME_ACCESS_INFO, total, ai_types.length * subscriberSize));
            int results[] = pstmt.executeBatch();
            assert(results != null);
            conn.commit();
        }
    }

    /**
     * Populate Special_Facility table and CallForwarding table per benchmark
     * spec.
     */
    void genSpeAndCal(Table catalog_spe, Table catalog_cal) throws SQLException {
    	// Create a prepared statement
        String spe_sql = SQLUtil.getInsertSQL(catalog_spe);
        PreparedStatement spe_pstmt = this.conn.prepareStatement(spe_sql);
        int spe_batch = 0;
        long spe_total = 0;
        
        String cal_sql = SQLUtil.getInsertSQL(catalog_cal);
        PreparedStatement cal_pstmt = this.conn.prepareStatement(cal_sql);
        long cal_total = 0;
        
        int s_id = 0;
        int[] spe_arr = { 1, 2, 3, 4 };
        int[] cal_arr = { 0, 8, 6 };
        if (LOG.isDebugEnabled()) LOG.debug("subscriberSize = " + subscriberSize);
        if (LOG.isDebugEnabled()) LOG.debug("batchSize = " + this.batchSize);
        while (s_id++ < subscriberSize) {
            int[] sf_types = TATPUtil.subArr(spe_arr, 1, 4);
            for (int sf_type : sf_types) {
            	int spe_col = 0;
            	spe_pstmt.setLong(++spe_col, s_id);
            	spe_pstmt.setByte(++spe_col, (byte)sf_type);
            	spe_pstmt.setByte(++spe_col, TATPUtil.isActive());
            	spe_pstmt.setShort(++spe_col, TATPUtil.number(0, 255).shortValue());
            	spe_pstmt.setShort(++spe_col, TATPUtil.number(0, 255).shortValue());
            	spe_pstmt.setString(++spe_col, TATPUtil.astring(5, 5));
            	spe_pstmt.addBatch();
            	spe_batch++;
                spe_total++;

                // now call_forwarding
                int[] start_times = TATPUtil.subArr(cal_arr, 0, 3);
                for (int start_time : start_times) {
                	int cal_col = 0;
                	cal_pstmt.setLong(++cal_col, s_id);
                	cal_pstmt.setByte(++cal_col, (byte)sf_type);
                	cal_pstmt.setByte(++cal_col, (byte)start_time);
                	cal_pstmt.setByte(++cal_col, (byte)(start_time + TATPUtil.number(1, 8)));
                	cal_pstmt.setString(++cal_col, TATPUtil.nstring(15, 15));
                	cal_pstmt.addBatch();
                    cal_total++;
                } // FOR
            } // FOR
            
            if (spe_batch > this.batchSize) {
                if (LOG.isDebugEnabled()) LOG.debug(String.format("%s: %d (%s %d / %d)",
													TATPConstants.TABLENAME_SPECIAL_FACILITY, spe_total,
													TATPConstants.TABLENAME_SUBSCRIBER, s_id, subscriberSize));
                int results[] = spe_pstmt.executeBatch();
                assert(results != null);
                
                
                if (LOG.isDebugEnabled()) LOG.debug(String.format("%s: %d (%s %d / %d)",
                									TATPConstants.TABLENAME_CALL_FORWARDING, cal_total,
                									TATPConstants.TABLENAME_SUBSCRIBER, s_id, subscriberSize));
                results = cal_pstmt.executeBatch();
                assert(results != null);
                
                spe_batch = 0;
                conn.commit();
            }
        } // WHILE
        LOG.debug("spe_batch = " + spe_batch);
        if (spe_batch > 0) {
            if (LOG.isDebugEnabled()) LOG.debug(String.format("%s: %d", TATPConstants.TABLENAME_SPECIAL_FACILITY, spe_total));
            int results[] = spe_pstmt.executeBatch();
            assert(results != null);
            
            if (LOG.isDebugEnabled()) LOG.debug(String.format("%s: %d", TATPConstants.TABLENAME_CALL_FORWARDING, cal_total));
            results = cal_pstmt.executeBatch();
            assert(results != null);
            
            conn.commit();
        }
    }
}