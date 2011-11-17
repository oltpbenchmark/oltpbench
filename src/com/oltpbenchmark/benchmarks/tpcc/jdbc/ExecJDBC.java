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
package com.oltpbenchmark.benchmarks.tpcc.jdbc;

/*
 * ExecJDBC - Command line program to process SQL DDL statements, from   
 *             a text input file, to any JDBC Data Source
 *
 * Copyright (C) 2004-2006, Denis Lussier
 *
 */

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;


public class ExecJDBC {

	public static void main(String[] args) {

		Connection conn = null;
		Statement stmt = null;
		String rLine = null;
		StringBuffer sql = new StringBuffer();
		java.util.Date now = null;

		now = new java.util.Date();
		System.out.println("------------- ExecJDBC Start Date = " + now
				+ "-------------");

		try {

			String propsPath = System.getProperty("prop", "");
			Properties ini;
			if (propsPath.equals("")) {
				ini = System.getProperties();
			} else {
				ini = new Properties();
				ini.load(new FileInputStream(System.getProperty("prop")));
			}

			// display the values we need
			System.out.println("driver=" + ini.getProperty("driver"));
			System.out.println("conn=" + ini.getProperty("conn"));
			System.out.println("user=" + ini.getProperty("user"));
			System.out.println("password=******");

			// Register jdbcDriver
			Class.forName(ini.getProperty("driver"));

			// make connection
			conn = DriverManager.getConnection(ini.getProperty("conn"),
					ini.getProperty("user"), ini.getProperty("password"));
			conn.setAutoCommit(true);

			// Create Statement
			stmt = conn.createStatement();

			// Open inputFile
			BufferedReader in = new BufferedReader(new FileReader(
					TPCCUtil.getSysProp("commandFile", null)));
			System.out
					.println("-------------------------------------------------\n");

			// loop thru input file and concatenate SQL statement fragments
			while ((rLine = in.readLine()) != null) {

				String line = rLine.trim();

				if (line.length() == 0) {
					System.out.println(""); // print empty line & skip over it
				} else {

					if (line.startsWith("--")) {
						System.out.println(line); // print comment line
					} else {
						sql.append(line);
						if (line.endsWith(";")) {
							execJDBC(stmt, sql);
							sql = new StringBuffer();
						} else {
							sql.append("\n");
						}
					}

				} // end if

			} // end while

			in.close();

		} catch (IOException ie) {
			System.out.println(ie.getMessage());

		} catch (SQLException se) {
			System.out.println(se.getMessage());

		} catch (Exception e) {
			e.printStackTrace();

			// exit Cleanly
		} finally {
			try {
				conn.close();
			} catch (SQLException se) {
				se.printStackTrace();
			} // end finally

		} // end try

		now = new java.util.Date();
		System.out.println("");
		System.out.println("------------- ExecJDBC End Date = " + now
				+ "-------------");

	} // end main

	static void execJDBC(Statement stmt, StringBuffer sql) {

		System.out.println("\n" + sql);

		try {

			long startTimeMS = new java.util.Date().getTime();
			stmt.execute(sql.toString().replace(';', ' '));
			long runTimeMS = (new java.util.Date().getTime()) + 1 - startTimeMS;
			System.out.println("-- SQL Success: Runtime = " + runTimeMS
					+ " ms --");

		} catch (SQLException se) {

			String msg = null;
			msg = se.getMessage();

			System.out
					.println("-- SQL Runtime Exception -----------------------------------------");
			System.out.println("DBMS SqlCode=" + se.getErrorCode()
					+ "  DBMS Msg=");
			System.out
					.println("  "
							+ msg
							+ "\n"
							+ "------------------------------------------------------------------\n");

		} // end try

	} // end execJDBCCommand

} // end ExecJDBC Class
