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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.apache.log4j.Logger;
import org.fluttercode.datafactory.impl.DataFactory;
import org.joda.time.DateTime;

import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.benchmarks.tpcc.TPCCLoader;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.util.SQLUtil;

public class SWBankLoader extends Loader {
    
    private static final Logger LOG = Logger.getLogger(SWBankLoader.class);
    
    private final static DataFactory df = new DataFactory();
    private final static DateTime mindt = new DateTime("1990-01-01");
    private final static DateTime maxdt = new DateTime("2010-12-31");

    private long totaltuples = 0;

    public SWBankLoader(BenchmarkModule benchmark, Connection conn) {
        super(benchmark, conn);
        // TODO Auto-generated constructor stub
    }

    @Override
    public void load() throws SQLException {
        // TODO Auto-generated method stub
        int c_id = 1;  
        int b_id = 1;
        long cust_id = 1;
        long a_id = 1;
 
                
        String c_sql = createInsertSql(SWBankConstants.TABLENAME_COUNTRY);
        String b_sql = createInsertSql(SWBankConstants.TABLENAME_BRANCH);
        String cust_sql = createInsertSql(SWBankConstants.TABLENAME_CUSTOMER);
        String a_sql = createInsertSql(SWBankConstants.TABLENAME_ACCOUNT);
        String chk_sql = createInsertSql(SWBankConstants.TABLENAME_CHECKING);
        String sav_sql = createInsertSql(SWBankConstants.TABLENAME_SAVING);
        
        PreparedStatement c_ps = this.conn.prepareStatement(c_sql);
        PreparedStatement b_ps = this.conn.prepareStatement(b_sql);
        PreparedStatement cust_ps = this.conn.prepareStatement(cust_sql);
        PreparedStatement a_ps = this.conn.prepareStatement(a_sql);
        PreparedStatement chk_ps = this.conn.prepareStatement(chk_sql);
        PreparedStatement sav_ps = this.conn.prepareStatement(sav_sql);
        
        InputStream cstream = this.getClass()
        .getResourceAsStream(SWBankConstants.COUNTRIES_DATAFILE);
        
        assert(cstream != null);
        
        BufferedReader bf = new BufferedReader(new InputStreamReader(
                cstream));
        try {
            bf.readLine(); // skip header
            
            String line = bf.readLine();
            
            while (line != null){
                // Insert a country
                String[] sline = line.split(",");
                String c = sline[0];
                int f = Integer.parseInt(sline[1]);
                
                c_ps.setInt(1, c_id);
                c_ps.setString(2, c);
                c_ps.setInt(3,f);
                
                c_ps.executeUpdate();
                incrementTotalDataObjects();
                
                // create and insert branches based scale factor
                // scale factors are integers for now
                //TODO: support for decimal scale factors
                // for scalefactor = 1, one branch per country
                
                int b_num = (int) Math.round(this.scaleFactor*SWBankConstants.BRANCH_PER_COUNTRY);
                
                if (b_num == 0){
                    b_num = 1;
                }
                
//                LOG.info("b_num = "+b_num);
                
                for (int b_i = 0; b_i < b_num; b_i++) {
                    
                    b_ps.setInt(1, b_id);
                    b_ps.setInt(2, c_id);
                    b_ps.setString(3, df.getAddress());
                    b_ps.setString(4, df.getNumberText(5)); // zipcode
                    b_ps.setDouble(5, 0.0d);
                    b_ps.setTimestamp(6, new Timestamp(System.currentTimeMillis()));
                    b_ps.setDouble(7, 0.0d);
                    b_ps.setTimestamp(8, new Timestamp(System.currentTimeMillis()));
                    b_ps.executeUpdate();
                    incrementTotalDataObjects();
                    
                    //create and insert customers  
                    // default: 1K customers per branch
                    int cust_num = (int) Math.round(this.scaleFactor*SWBankConstants.CUSTOMER_PER_BRANCH);
                    for (int cust_i = 0; cust_i < cust_num; cust_i++) {
                        cust_ps.setLong(1, cust_id);
                        cust_ps.setString(2, df.getFirstName());
                        cust_ps.setString(3,df.getLastName());
                        cust_ps.setDate(4, new java.sql.Date(df.getDateBetween(mindt.toDate(), maxdt.toDate()).getTime()));
                        cust_ps.setString(5, df.getAddress());
                        cust_ps.setString(6, df.getNumberText(5)); // zipcode
                        cust_ps.setInt(7, 0); // current transaction count
                        cust_ps.setInt(8, 0); // total transaction count
                        cust_ps.setTimestamp(9, new Timestamp(System.currentTimeMillis())); // reporting time stamp of total tx count
                        cust_ps.executeUpdate();
                        incrementTotalDataObjects();
                        
                        /// create account
                        // 1 account per customer for now.
                        a_ps.setLong(1, a_id);
                        a_ps.setLong(2, cust_id);
                        a_ps.setLong(3, b_id);
                        
                        a_ps.executeUpdate();
                        incrementTotalDataObjects();
                        
                        
                        // initialize checking
                        chk_ps.setLong(1, a_id);
                        chk_ps.setFloat(2, SWBankConstants.INITAL_BALANCE);                        
                        chk_ps.executeUpdate();
                        incrementTotalDataObjects();
                        
                        // initialize saving
                        sav_ps.setLong(1, a_id);
                        sav_ps.setFloat(2, SWBankConstants.INITAL_BALANCE);
                        sav_ps.executeUpdate();
                        incrementTotalDataObjects();
                        
                        a_id++;
                        cust_id++;
                    }
                    
                    b_id++;
                    
                } // end for
                
                
                
                //initialize their balances
                // one account per customer. 
                
                
                // initialize account balances to 10K
                
                // execute an sql batch per country
                
                
                ++c_id;
                line = bf.readLine();
            }
                    
            LOG.info("Total tuples = "+totaltuples);
            
        } catch (IOException e) {
          throw new RuntimeException("Error in reading countries data file");  
        } catch (SQLException e) {
            e.printStackTrace();
            throw e.getNextException();
        } 
        
    }

    private void incrementTotalDataObjects() {
        ++totaltuples ;
    }

    /**
     * @return
     */
    private String createInsertSql(String tablename) {
        Table catalog_tbl = getTableCatalog(tablename);
        String sql;
        
        // Load countries
        if (this.getDatabaseType() == DatabaseType.POSTGRES
                || this.getDatabaseType() == DatabaseType.MONETDB)
            sql = SQLUtil.getInsertSQL(catalog_tbl, false);
        else
            sql = SQLUtil.getInsertSQL(catalog_tbl);
        return sql;
    }

}
