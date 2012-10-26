package com.oltpbenchmark.benchmarks.chbenchmark;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.benchmarks.chbenchmark.pojo.Nation;
import com.oltpbenchmark.benchmarks.chbenchmark.pojo.Region;
import com.oltpbenchmark.benchmarks.chbenchmark.pojo.Supplier;
import com.oltpbenchmark.benchmarks.tpcc.TPCCBenchmark;
import com.oltpbenchmark.benchmarks.tpcc.TPCCLoader;

public class CHBenCHmarkLoader extends Loader {
	private static final Logger LOG = Logger.getLogger(CHBenCHmarkLoader.class);
	private final static int configCommitCount = 1000; // commit every n records
	private static PreparedStatement regionPrepStmt;
	private static PreparedStatement nationPrepStmt;
	private static PreparedStatement supplierPrepStmt;
	
	private static Date now;
	private static long lastTimeMS;
	private static Connection conn;
	
	public CHBenCHmarkLoader(BenchmarkModule benchmark, Connection c) {
		super(benchmark, c);
		conn =c;
	}

	public void load() throws SQLException {
		try {
			regionPrepStmt = conn.prepareStatement("INSERT INTO region "
					+ " (r_regionkey, r_name, r_comment) "
					+ "VALUES (?, ?, ?)");
			
			nationPrepStmt = conn.prepareStatement("INSERT INTO nation "
					+ " (n_nationkey, n_name, n_regionkey, n_comment) "
					+ "VALUES (?, ?, ?, ?)");
			
			supplierPrepStmt = conn.prepareStatement("INSERT INTO supplier "
					+ " (su_suppkey, su_name, su_address, su_nationkey, su_phone, su_acctbal, su_comment) "
					+ "VALUES (?, ?, ?, ?, ?, ?, ?)");

		} catch (SQLException se) {
			LOG.debug(se.getMessage());
			conn.rollback();

		} catch (Exception e) {
			e.printStackTrace();
			conn.rollback();

		} // end try
		
//		truncateTable("supplier");
//		truncateTable("nation");
//		truncateTable("region");
		loadHelper();
		conn.commit();
	}
	
   static void truncateTable(String strTable) throws SQLException {

        LOG.debug("Truncating '" + strTable + "' ...");
        try {
            conn.createStatement().execute("TRUNCATE TABLE " + strTable + " CASCADE");
            conn.commit();
        } catch (SQLException se) {
            LOG.debug(se.getMessage());
            conn.rollback();
        }
   }
	
	static int loadRegions() throws SQLException {
		
		int k = 0;
		int t = 0;
		
		try {
		    
		    truncateTable("region");
		    truncateTable("nation");
		    truncateTable("supplier");

			now = new java.util.Date();
			LOG.debug("\nStart Region Load @ " + now
					+ " ...");

			Region region = new Region();
			
			File file = new File("src", "com/oltpbenchmark/benchmarks/chbenchmark/region_gen.tbl");
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line = br.readLine();
			while (line != null) {
				StringTokenizer st = new StringTokenizer(line, "|");
				if (!st.hasMoreTokens()) { LOG.error("invalid input file: " + file.getAbsolutePath()); }
				region.r_regionkey = Integer.parseInt(st.nextToken());
				if (!st.hasMoreTokens()) { LOG.error("invalid input file: " + file.getAbsolutePath()); }
				region.r_name = st.nextToken();
				if (!st.hasMoreTokens()) { LOG.error("invalid input file: " + file.getAbsolutePath()); }
				region.r_comment = st.nextToken();
				if (st.hasMoreTokens()) { LOG.error("invalid input file: " + file.getAbsolutePath()); }

				k++;

				regionPrepStmt.setLong(1, region.r_regionkey);
				regionPrepStmt.setString(2, region.r_name);
				regionPrepStmt.setString(3, region.r_comment);
				regionPrepStmt.addBatch();

				long tmpTime = new java.util.Date().getTime();
				String etStr = "  Elasped Time(ms): "
						+ ((tmpTime - lastTimeMS) / 1000.000)
						+ "                    ";
				LOG.debug(etStr.substring(0, 30)
						+ "  Writing record " + k + " of " + t);
				lastTimeMS = tmpTime;
				regionPrepStmt.executeBatch();
				regionPrepStmt.clearBatch();
				conn.commit();
				line = br.readLine();
			}

			long tmpTime = new java.util.Date().getTime();
			String etStr = "  Elasped Time(ms): "
					+ ((tmpTime - lastTimeMS) / 1000.000)
					+ "                    ";
			LOG.debug(etStr.substring(0, 30) + "  Writing record " + k
					+ " of " + t);
			lastTimeMS = tmpTime;

			regionPrepStmt.executeBatch();

			conn.commit();
			now = new java.util.Date();
			LOG.debug("End Region Load @  " + now);

		} catch (SQLException se) {
			LOG.debug(se.getMessage());
			conn.rollback();
		} catch (Exception e) {
			e.printStackTrace();
			conn.rollback();
		}

		return (k);

	} // end loadRegions()
	
	static int loadNations() throws SQLException {
		
		int k = 0;
		int t = 0;
		
		try {

			now = new java.util.Date();
			LOG.debug("\nStart Nation Load @ " + now
					+ " ...");

			Nation nation = new Nation();
			
			File file = new File("src", "com/oltpbenchmark/benchmarks/chbenchmark/nation_gen.tbl");
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line = br.readLine();
			while (line != null) {
				StringTokenizer st = new StringTokenizer(line, "|");
				if (!st.hasMoreTokens()) { LOG.error("invalid input file: " + file.getAbsolutePath()); }
				nation.n_nationkey = Integer.parseInt(st.nextToken());
				if (!st.hasMoreTokens()) { LOG.error("invalid input file: " + file.getAbsolutePath()); }
				nation.n_name = st.nextToken();
				if (!st.hasMoreTokens()) { LOG.error("invalid input file: " + file.getAbsolutePath()); }
				nation.n_regionkey = Integer.parseInt(st.nextToken());
				if (!st.hasMoreTokens()) { LOG.error("invalid input file: " + file.getAbsolutePath()); }
				nation.n_comment = st.nextToken();
				if (st.hasMoreTokens()) { LOG.error("invalid input file: " + file.getAbsolutePath()); }

				k++;

				nationPrepStmt.setLong(1, nation.n_nationkey);
				nationPrepStmt.setString(2, nation.n_name);
				nationPrepStmt.setLong(3, nation.n_regionkey);
				nationPrepStmt.setString(4, nation.n_comment);
				nationPrepStmt.addBatch();

				long tmpTime = new java.util.Date().getTime();
				String etStr = "  Elasped Time(ms): "
						+ ((tmpTime - lastTimeMS) / 1000.000)
						+ "                    ";
				LOG.debug(etStr.substring(0, 30)
						+ "  Writing record " + k + " of " + t);
				lastTimeMS = tmpTime;
				nationPrepStmt.executeBatch();
				nationPrepStmt.clearBatch();
				conn.commit();
				line = br.readLine();
			}

			long tmpTime = new java.util.Date().getTime();
			String etStr = "  Elasped Time(ms): "
					+ ((tmpTime - lastTimeMS) / 1000.000)
					+ "                    ";
			LOG.debug(etStr.substring(0, 30) + "  Writing record " + k
					+ " of " + t);
			lastTimeMS = tmpTime;

			conn.commit();
			now = new java.util.Date();
			LOG.debug("End Region Load @  " + now);

		} catch (SQLException se) {
			LOG.debug(se.getMessage());
			conn.rollback();
		} catch (Exception e) {
			e.printStackTrace();
			conn.rollback();
		}

		return (k);

	} // end loadNations()
	
	static int loadSuppliers() throws SQLException {
		
		int k = 0;
		int t = 0;
		
		try {

			now = new java.util.Date();
			LOG.debug("\nStart Supplier Load @ " + now
					+ " ...");

			Supplier supplier = new Supplier();
			
			File file = new File("src", "com/oltpbenchmark/benchmarks/chbenchmark/supplier_gen.tbl");
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line = br.readLine();
			while (line != null) {
				StringTokenizer st = new StringTokenizer(line, "|");
				if (!st.hasMoreTokens()) { LOG.error("invalid input file: " + file.getAbsolutePath()); }
				supplier.su_suppkey = Integer.parseInt(st.nextToken());
				if (!st.hasMoreTokens()) { LOG.error("invalid input file: " + file.getAbsolutePath()); }
				supplier.su_name = st.nextToken();
				if (!st.hasMoreTokens()) { LOG.error("invalid input file: " + file.getAbsolutePath()); }
				supplier.su_address = st.nextToken();
				if (!st.hasMoreTokens()) { LOG.error("invalid input file: " + file.getAbsolutePath()); }
				supplier.su_nationkey = Integer.parseInt(st.nextToken());
				if (!st.hasMoreTokens()) { LOG.error("invalid input file: " + file.getAbsolutePath()); }
				supplier.su_phone = st.nextToken();
				if (!st.hasMoreTokens()) { LOG.error("invalid input file: " + file.getAbsolutePath()); }
				supplier.su_acctbal = Float.parseFloat(st.nextToken());
				if (!st.hasMoreTokens()) { LOG.error("invalid input file: " + file.getAbsolutePath()); }
				supplier.su_comment = st.nextToken();
				if (st.hasMoreTokens()) { LOG.error("invalid input file: " + file.getAbsolutePath()); }

				k++;
				
				supplierPrepStmt.setLong(1, supplier.su_suppkey);
				supplierPrepStmt.setString(2, supplier.su_name);
				supplierPrepStmt.setString(3, supplier.su_address);
				supplierPrepStmt.setLong(4, supplier.su_nationkey);
				supplierPrepStmt.setString(5, supplier.su_phone);
				supplierPrepStmt.setDouble(6, supplier.su_acctbal);
				supplierPrepStmt.setString(7, supplier.su_comment);
				supplierPrepStmt.addBatch();

				if ((k % configCommitCount) == 0) {
					long tmpTime = new java.util.Date().getTime();
					String etStr = "  Elasped Time(ms): "
							+ ((tmpTime - lastTimeMS) / 1000.000)
							+ "                    ";
					LOG.debug(etStr.substring(0, 30)
							+ "  Writing record " + k + " of " + t);
					lastTimeMS = tmpTime;
					supplierPrepStmt.executeBatch();
					supplierPrepStmt.clearBatch();
					conn.commit();
				}
				line = br.readLine();
			}

			long tmpTime = new java.util.Date().getTime();
			String etStr = "  Elasped Time(ms): "
					+ ((tmpTime - lastTimeMS) / 1000.000)
					+ "                    ";
			LOG.debug(etStr.substring(0, 30) + "  Writing record " + k
					+ " of " + t);
			lastTimeMS = tmpTime;

			supplierPrepStmt.executeBatch();

			conn.commit();
			now = new java.util.Date();
			LOG.debug("End Region Load @  " + now);

		} catch (SQLException se) {
			LOG.debug(se.getMessage());
			conn.rollback();
		} catch (Exception e) {
			e.printStackTrace();
			conn.rollback();
		}

		return (k);

	} // end loadSuppliers()

	protected long loadHelper() {
		long totalRows = 0;
		try {
			totalRows += loadRegions();
			totalRows += loadNations();
			totalRows += loadSuppliers();
		}
		catch (SQLException e) {
			LOG.debug(e.getMessage());
		}
		return totalRows;
	}	
	
}
