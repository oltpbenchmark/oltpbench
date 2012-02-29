/*******************************************************************************
 * oltpbenchmark.com
 *  
 *  Project Info:  http://oltpbenchmark.com
 *  Project Members:  	Carlo Curino <carlo.curino@gmail.com>
 * 				Evan Jones <ej@evanjones.ca>
 * 				DIFALLAH Djellel Eddine <djelleleddine.difallah@unifr.ch>
 * 				Andy Pavlo <pavlo@cs.brown.edu>
 * 				CUDRE-MAUROUX Philippe <philippe.cudre-mauroux@unifr.ch>  
 *  				Yang Zhang <yaaang@gmail.com> 
 * 
 *  This library is free software; you can redistribute it and/or modify it under the terms
 *  of the GNU General Public License as published by the Free Software Foundation;
 *  either version 3.0 of the License, or (at your option) any later version.
 * 
 *  This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  See the GNU Lesser General Public License for more details.
 ******************************************************************************/
package com.oltpbenchmark.benchmarks.wikipedia.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import com.oltpbenchmark.api.TransactionTypes;
import com.oltpbenchmark.util.FileUtil;

public class TransactionSelector {

    final Pattern p = Pattern.compile(" "); 
	final File file;
	BufferedReader reader = null;
	TransactionTypes transTypes;
	static final double READ_WRITE_RATIO = 11.8; // from
													// http://www.globule.org/publi/WWADH_comnet2009.html

	public TransactionSelector(File file, TransactionTypes transTypes) throws FileNotFoundException {
	    this.file = file;
	    this.transTypes = transTypes;
		
	    if (this.file == null)
			throw new FileNotFoundException("You must specify a filename to instantiate the TransactionSelector... (probably missing in your workload configuration?)");
		
		BufferedReader r = null;
		try {
		    r = FileUtil.getReader(this.file);
		} catch (IOException ex) {
		    throw new RuntimeException("Failed to open file '" + file + "' for reading", ex);
		}
		assert(r != null);
		this.reader = r;
	}

	public List<WikipediaOperation> readAll() throws IOException {
		ArrayList<WikipediaOperation> transactions = new ArrayList<WikipediaOperation>();
		while (this.reader.ready()) {
		    String line = this.reader.readLine();
	        String[] sa = p.split(line);

	        int user = Integer.parseInt(sa[0]);
	        int namespace = Integer.parseInt(sa[1]);
	        
	        int startIdx = sa[0].length() + sa[1].length() + 2;
	        String title = line.substring(startIdx, startIdx + line.length()-startIdx);
	        // HACK: Check whether they have a " - " at the end of the line
	        // If they do, then that means that they are coming from a real trace and we need
	        // to strip it out
	        if (title.endsWith(" - ")) {
	            title = title.substring(0, title.length()-3);
	        }
	        
	        transactions.add(new WikipediaOperation(user, namespace, title));
		} // WHILE
		this.reader.close();
		return transactions;
	}
	
	public static void writeEntry(OutputStream out, int userId, int pageNamespace, String pageTitle) throws IOException {
	    out.write(String.format("%d %d %s\n", userId, pageNamespace, pageTitle).getBytes());
	}
}
