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

import com.oltpbenchmark.api.AbstractTestBenchmarkModule;
import com.oltpbenchmark.benchmarks.tatp.procedures.*;

public class TestTATPBenchmark extends AbstractTestBenchmarkModule<TATPBenchmark> {
	
    public static final Class<?> PROC_CLASSES[] = {
        DeleteCallForwarding.class,
        GetAccessData.class,
        GetNewDestination.class,
        GetSubscriberData.class,
        InsertCallForwarding.class,
        UpdateLocation.class,
        UpdateSubscriberData.class,
    };
    
	@Override
	protected void setUp() throws Exception {
		super.setUp(TATPBenchmark.class, PROC_CLASSES);
		            
	}
	
}
