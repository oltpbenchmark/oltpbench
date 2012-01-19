/***************************************************************************
 *  Copyright (C) 2011 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  http://hstore.cs.brown.edu/                                            *
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
package com.oltpbenchmark.benchmarks.seats.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.benchmarks.seats.SEATSConstants;

public class DeleteReservation extends Procedure {
    private static final Logger LOG = Logger.getLogger(DeleteReservation.class);
    
    public final SQLStmt GetCustomerByIdStr = new SQLStmt(
            "SELECT C_ID " + 
            "  FROM " + SEATSConstants.TABLENAME_CUSTOMER + 
            " WHERE C_ID_STR = ?");
    
    public final SQLStmt GetCustomerByFFNumber = new SQLStmt(
            "SELECT C_ID, FF_AL_ID " +
            "  FROM " + SEATSConstants.TABLENAME_CUSTOMER + ", " + 
                        SEATSConstants.TABLENAME_FREQUENT_FLYER + 
            " WHERE FF_C_ID_STR = ? AND FF_C_ID = C_ID");
    
    public final SQLStmt GetCustomerReservation = new SQLStmt(
            "SELECT C_SATTR00, C_SATTR02, C_SATTR04, " +
            "       C_IATTR00, C_IATTR02, C_IATTR04, C_IATTR06, " +
            "       F_SEATS_LEFT, " +
            "       R_ID, R_SEAT, R_PRICE, R_IATTR00 " +
            "  FROM " + SEATSConstants.TABLENAME_CUSTOMER + ", " +
                        SEATSConstants.TABLENAME_FLIGHT + ", " +
                        SEATSConstants.TABLENAME_RESERVATION +
            " WHERE C_ID = ? AND C_ID = R_C_ID " +
            "   AND F_ID = ? AND F_ID = R_F_ID "
    );
    
    public final SQLStmt DeleteReservation = new SQLStmt(
            "DELETE FROM " + SEATSConstants.TABLENAME_RESERVATION +
            " WHERE R_ID = ? AND R_C_ID = ? AND R_F_ID = ?");

    public final SQLStmt UpdateFlight = new SQLStmt(
            "UPDATE " + SEATSConstants.TABLENAME_FLIGHT +
            "   SET F_SEATS_LEFT = F_SEATS_LEFT + 1 " + 
            " WHERE F_ID = ? ");
    
    public final SQLStmt UpdateCustomer = new SQLStmt(
            "UPDATE " + SEATSConstants.TABLENAME_CUSTOMER +
            "   SET C_BALANCE = C_BALANCE + ?, " +
            "       C_IATTR00 = ?, " +
            "       C_IATTR10 = C_IATTR10 - 1, " + 
            "       C_IATTR11 = C_IATTR10 - 1 " +
            " WHERE C_ID = ? ");
    
    public final SQLStmt UpdateFrequentFlyer = new SQLStmt(
            "UPDATE " + SEATSConstants.TABLENAME_FREQUENT_FLYER +
            "   SET FF_IATTR10 = FF_IATTR10 - 1 " + 
            " WHERE FF_C_ID = ? " +
            "   AND FF_AL_ID = ?");
    
    public void run(Connection conn, long f_id, Long c_id, String c_id_str, String ff_c_id_str, Long ff_al_id) throws SQLException {
        final boolean debug = LOG.isDebugEnabled();
        PreparedStatement stmt = null; 
        
        // If we weren't given the customer id, then look it up
        if (c_id == null) {
            boolean has_al_id = false;
            
            // Use the customer's id as a string
            if (c_id_str != null && c_id_str.length() > 0) {
                stmt = this.getPreparedStatement(conn, GetCustomerByIdStr, c_id_str);
            }
            // Otherwise use their FrequentFlyer information
            else {
                assert(ff_c_id_str.isEmpty() == false);
                assert(ff_al_id != null);
                stmt = this.getPreparedStatement(conn, GetCustomerByFFNumber, ff_c_id_str);
                has_al_id = true;
            }
            ResultSet results = stmt.executeQuery();
            if (results.next()) {
                c_id = results.getLong(1);
                if (has_al_id) ff_al_id = results.getLong(2);
            } else {
                throw new UserAbortException(String.format("No Customer record was found [c_id_str=%s, ff_c_id_str=%s, ff_al_id=%s]",
                                                           c_id_str, ff_c_id_str, ff_al_id));
            }
        }

        // Now get the result of the information that we need
        // If there is no valid customer record, then throw an abort
        // This should happen 5% of the time
        stmt = this.getPreparedStatement(conn, GetCustomerReservation);
        stmt.setLong(1, c_id);
        stmt.setLong(2, f_id);
        ResultSet results = stmt.executeQuery();
        if (results.next() == false) {
            throw new UserAbortException(String.format("No Customer information record found for id '%d'", c_id));
        }
        long c_iattr00 = results.getLong(4) + 1;
        long seats_left = results.getLong(8); 
        long r_id = results.getLong(9);
        double r_price = results.getDouble(11);
        int updated = 0;
        
        // Now delete all of the flights that they have on this flight
        stmt = this.getPreparedStatement(conn, DeleteReservation, r_id, c_id, f_id);
        updated = stmt.executeUpdate();
        assert(updated == 1);
        
        // Update Available Seats on Flight
        stmt = this.getPreparedStatement(conn, UpdateFlight, f_id);
        updated = stmt.executeUpdate();
        
        // Update Customer's Balance
        stmt = this.getPreparedStatement(conn, UpdateCustomer, -1*r_price, c_iattr00, c_id);
        updated = stmt.executeUpdate();
        assert(updated == 1);
        
        // Update Customer's Frequent Flyer Information (Optional)
        if (ff_al_id != null) {
            stmt = this.getPreparedStatement(conn, UpdateFrequentFlyer, c_id, ff_al_id);
            updated = stmt.executeUpdate();
            assert(updated == 1) :
                String.format("Failed to update FrequentFlyer info [c_id=%d, ff_al_id=%d]", c_id, ff_al_id);
        }
        
        if (debug)
            LOG.debug(String.format("Deleted reservation on flight %d for customer %d [seatsLeft=%d]", f_id, c_id, seats_left+1));        
    }

}
