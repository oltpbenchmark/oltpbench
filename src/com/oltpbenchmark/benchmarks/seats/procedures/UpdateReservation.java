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
/* This file is part of VoltDB. 
 * Copyright (C) 2009 Vertica Systems Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be 
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR 
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.                       
 */
package com.oltpbenchmark.benchmarks.seats.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.api.Procedure;

import com.oltpbenchmark.benchmarks.seats.SEATSConstants;
import com.oltpbenchmark.benchmarks.seats.SEATSConstants.ErrorType;

public class UpdateReservation extends Procedure {
    private static final Logger LOG = Logger.getLogger(UpdateReservation.class);
    
    public final SQLStmt CheckSeat = new SQLStmt(
        "SELECT R_ID " +
        "  FROM " + SEATSConstants.TABLENAME_RESERVATION +
        " WHERE R_F_ID = ? and R_SEAT = ?");

    public final SQLStmt CheckCustomer = new SQLStmt(
        "SELECT R_ID " + 
        "  FROM " + SEATSConstants.TABLENAME_RESERVATION +
        " WHERE R_F_ID = ? AND R_C_ID = ?");

    private static final String BASE_SQL = "UPDATE " + SEATSConstants.TABLENAME_RESERVATION +
                                           "   SET R_SEAT = ?, %s = ? " +
                                           " WHERE R_ID = ? AND R_C_ID = ? AND R_F_ID = ?";
    
    public final SQLStmt ReserveSeat0 = new SQLStmt(String.format(BASE_SQL, "R_IATTR00"));
    public final SQLStmt ReserveSeat1 = new SQLStmt(String.format(BASE_SQL, "R_IATTR01"));
    public final SQLStmt ReserveSeat2 = new SQLStmt(String.format(BASE_SQL, "R_IATTR02"));
    public final SQLStmt ReserveSeat3 = new SQLStmt(String.format(BASE_SQL, "R_IATTR03"));

    public static final int NUM_UPDATES = 4;
    public final SQLStmt ReserveSeats[] = {
        ReserveSeat0,
        ReserveSeat1,
        ReserveSeat2,
        ReserveSeat3,
    };
    
    public void run(Connection conn, long r_id, long f_id, long c_id, long seatnum, long attr_idx, long attr_val) throws SQLException {
        final boolean debug = LOG.isDebugEnabled();
        assert(attr_idx >= 0);
        assert(attr_idx < ReserveSeats.length);
        
        PreparedStatement stmt = null;
        ResultSet results = null;
        
        // Check if Seat is Available
        stmt = this.getPreparedStatement(conn, CheckSeat, f_id, seatnum);
        results = stmt.executeQuery();
        if (results.next()) {
            throw new UserAbortException(ErrorType.SEAT_ALREADY_RESERVED +
                                         String.format(" Seat %d is already reserved on flight #%d", seatnum, f_id));
        }
        // Check if the Customer already has a seat on this flight
        stmt = this.getPreparedStatement(conn, CheckCustomer, f_id, c_id);
        results = stmt.executeQuery();
        if (results.next() == false) {
            throw new UserAbortException(ErrorType.CUSTOMER_ALREADY_HAS_SEAT +
                                         String.format(" Customer %d does not have an existing reservation on flight #%d", c_id, f_id));
        }
        
        // Update the seat reservation for the customer
        stmt = this.getPreparedStatement(conn, ReserveSeats[(int)attr_idx], seatnum, attr_val, r_id, c_id, f_id);
        int updated = stmt.executeUpdate();
        if (updated != 1) {
            String msg = String.format("Failed to update reservation on flight %d for customer #%d - Updated %d records", f_id, c_id, updated);
            if (debug) LOG.warn(msg);
            throw new UserAbortException(ErrorType.VALIDITY_ERROR + " " + msg);
        }
        
        if (debug)
            LOG.debug(String.format("Updated reservation on flight %d for customer %d", f_id, c_id));
        return;
    } 
}
