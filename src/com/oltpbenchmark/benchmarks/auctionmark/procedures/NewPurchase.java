/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
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
package com.oltpbenchmark.benchmarks.auctionmark.procedures;

import java.sql.Connection;
import java.sql.Timestamp;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkConstants;
import com.oltpbenchmark.benchmarks.auctionmark.util.AuctionMarkUtil;
import com.oltpbenchmark.benchmarks.auctionmark.util.ItemStatus;

/**
 * NewPurchase
 * Description goes here...
 * @author visawee
 */
public class NewPurchase extends Procedure {
    
    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------
    
    public final SQLStmt getItemMaxBid = new SQLStmt(
        "SELECT * FROM " + AuctionMarkConstants.TABLENAME_ITEM_MAX_BID +
        " WHERE imb_i_id = ? AND imb_u_id = ?"
    );
    
    public final SQLStmt getMaxBid = new SQLStmt(
        "SELECT * FROM " + AuctionMarkConstants.TABLENAME_ITEM_BID +
        " WHERE imb_i_id = ? AND imb_u_id = ? " +
        " ORDER BY ib_bid DESC LIMIT 1" 
    );
    
    public final SQLStmt insertItemMaxBid = new SQLStmt(
        "INSERT INTO " + AuctionMarkConstants.TABLENAME_ITEM_MAX_BID + " (" +
        "imb_i_id, " +
        "imb_u_id, " +
        "imb_ib_id, " +
        "imb_ib_i_id, " +
        "imb_ib_u_id, " +
        "imb_created, " +
        "imb_updated " +
        ") VALUES (" +
        "?, " + // imb_i_id
        "?, " + // imb_u_id
        "?, " + // imb_ib_id
        "?, " + // imb_ib_i_id
        "?, " + // imb_ib_u_id
        "?, " + // imb_created
        "? "  + // imb_updated
        ")"
    );
    
    public final SQLStmt getItemInfo = new SQLStmt(
        "SELECT i_num_bids, i_current_price, i_end_date, " +
        "       ib_id, ib_buyer_id, " +
        "       u_balance " +
		"  FROM " + AuctionMarkConstants.TABLENAME_ITEM + ", " +
		            AuctionMarkConstants.TABLENAME_ITEM_MAX_BID + ", " +
		            AuctionMarkConstants.TABLENAME_ITEM_BID + ", " +
		            AuctionMarkConstants.TABLENAME_USERACCT +
        " WHERE i_id = ? AND i_u_id = ? " +
        "   AND imb_i_id = i_id AND imb_u_id = i_u_id " +
        "   AND imb_ib_id = ib_id AND imb_ib_i_id = ib_i_id AND imb_ib_u_id = ib_u_id " +
        "   AND ib_buyer_id = u_id "
    );

    public final SQLStmt getBuyerInfo = new SQLStmt(
        "SELECT u_id, u_balance " +
        "  FROM " + AuctionMarkConstants.TABLENAME_USERACCT +
        " WHERE u_id = ? "
    );
    
    public final SQLStmt insertPurchase = new SQLStmt(
        "INSERT INTO " + AuctionMarkConstants.TABLENAME_ITEM_PURCHASE + " (" +
        	"ip_id," +
        	"ip_ib_id," +
        	"ip_ib_i_id," +  
        	"ip_ib_u_id," +  
        	"ip_date" +     
        ") VALUES(?,?,?,?,?)"
    );
    
    public final SQLStmt updateItem = new SQLStmt(
        "UPDATE " + AuctionMarkConstants.TABLENAME_ITEM +
          " SET i_status = " + ItemStatus.CLOSED.ordinal() + ", i_updated = ? " +
        " WHERE i_id = ? AND i_u_id = ? "
    );    
    
    public final SQLStmt updateUserItem = new SQLStmt(
        "UPDATE " + AuctionMarkConstants.TABLENAME_USERACCT_ITEM + " " +
           "SET ui_ip_id = ?, " +
           "    ui_ip_ib_id = ?, " +
           "    ui_ip_ib_i_id = ?, " +
           "    ui_ip_ib_u_id = ?" +
        " WHERE ui_u_id = ? AND ui_i_id = ? AND ui_i_u_id = ?"
    );
    
    public final SQLStmt insertUserItem = new SQLStmt(
        "INSERT INTO " + AuctionMarkConstants.TABLENAME_USERACCT_ITEM + "(" +
            "ui_u_id, " +
            "ui_i_id, " +
            "ui_i_u_id, " +
            "ui_ip_id, " +
            "ui_ip_ib_id, " +
            "ui_ip_ib_i_id, " +
            "ui_ip_ib_u_id, " +
            "ui_created" +     
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    );
    
    public final SQLStmt updateUserBalance = new SQLStmt(
        "UPDATE " + AuctionMarkConstants.TABLENAME_USERACCT + " " +
           "SET u_balance = u_balance + ? " + 
        " WHERE u_id = ?"
    );
    
    // -----------------------------------------------------------------
    // RUN METHOD
    // -----------------------------------------------------------------
    
    public Object[] run(Connection conn, Timestamp benchmarkTimes[],
                        long item_id, long seller_id, long ip_id, double buyer_credit) throws SQLException {
        final Timestamp currentTime = AuctionMarkUtil.getProcTimestamp(benchmarkTimes);
        
        PreparedStatement stmt = null;
        ResultSet results = null;
        int updated;
        boolean adv;
        
        // HACK: Check whether we have an ITEM_MAX_BID record. If not, we'll insert one
        stmt = this.getPreparedStatement(conn, getItemMaxBid, item_id, seller_id);
        results = stmt.executeQuery();
        if (results.next() == false) {
            stmt = this.getPreparedStatement(conn, getMaxBid, item_id, seller_id);
            results = stmt.executeQuery();
            adv = results.next();
            assert(adv);
            long bid_id = results.getLong(1);

            updated = this.getPreparedStatement(conn, insertItemMaxBid, item_id,
                                                                        seller_id,
                                                                        bid_id,
                                                                        item_id,
                                                                        seller_id,
                                                                        currentTime,
                                                                        currentTime).executeUpdate();
            assert(updated == 1) :
                String.format("Failed to update %s for Seller #%d's Item #%d",
                              AuctionMarkConstants.TABLENAME_ITEM_MAX_BID, seller_id, item_id);
        }
        results.close();
        
        // Get the ITEM_MAX_BID record so that we know what we need to process
        // At this point we should always have an ITEM_MAX_BID record
        stmt = this.getPreparedStatement(conn, getItemInfo, item_id, seller_id);
        results = stmt.executeQuery();
        if (results.next() == false) {
            String msg = "No ITEM_MAX_BID is available record for item " + item_id;
            throw new UserAbortException(msg);
        }
        int col = 1;
        long i_num_bids = results.getLong(col++);
        double i_current_price = results.getDouble(col++);
        Timestamp i_end_date = results.getTimestamp(col++);
        ItemStatus i_status = ItemStatus.CLOSED;
        long ib_id = results.getLong(col++);
        long ib_buyer_id = results.getLong(col++);
        double u_balance = results.getDouble(col++);
        results.close();
        
        // Make sure that the buyer has enough money to cover this charge
        // We can add in a credit for the buyer's account
        if (i_current_price > (buyer_credit + u_balance)) {
            String msg = String.format("Buyer #%d does not have enough money in account to purchase Item #%d" +
                                       "[maxBid=%.2f, balance=%.2f, credit=%.2f]",
                                       ib_buyer_id, item_id, i_current_price, u_balance, buyer_credit);
            throw new UserAbortException(msg);
        }

        // Set item_purchase_id
        updated = this.getPreparedStatement(conn, insertPurchase, ip_id, ib_id, item_id, seller_id, currentTime).executeUpdate();
        assert(updated == 1);
        
        // Update item status to close
        updated = this.getPreparedStatement(conn, updateItem, currentTime, item_id, seller_id).executeUpdate();
        assert(updated == 1) :
            String.format("Failed to update %s for Seller #%d's Item #%d",
                          AuctionMarkConstants.TABLENAME_ITEM, seller_id, item_id);
        
        // And update this the USERACT_ITEM record to link it to the new ITEM_PURCHASE record
        // If we don't have a record to update, just go ahead and create it
        updated = this.getPreparedStatement(conn, updateUserItem, ip_id, ib_id, item_id, seller_id,
                                                                  ib_buyer_id, item_id, seller_id).executeUpdate();
        if (updated == 0) {
            updated = this.getPreparedStatement(conn, insertUserItem, ib_buyer_id, item_id, seller_id,
                                                                      ip_id, ib_id, item_id, seller_id,
                                                                      currentTime).executeUpdate();
        }
        assert(updated == 1) :
            String.format("Failed to update %s for Buyer #%d's Item #%d",
                          AuctionMarkConstants.TABLENAME_USERACCT_ITEM, ib_buyer_id, item_id);
        
        // Decrement the buyer's account 
        updated = this.getPreparedStatement(conn, updateUserBalance, -1*(i_current_price) + buyer_credit, ib_buyer_id).executeUpdate();
        assert(updated == 1) :
            String.format("Failed to update %s for Buyer #%d",
                          AuctionMarkConstants.TABLENAME_USERACCT, ib_buyer_id);
        
        // And credit the seller's account
        this.getPreparedStatement(conn, updateUserBalance, i_current_price, seller_id).executeUpdate();
        assert(updated == 1) :
            String.format("Failed to update %s for Seller #%d",
                          AuctionMarkConstants.TABLENAME_USERACCT, seller_id);
        
        // Return a tuple of the item that we just updated
        return new Object[] {
            // ITEM ID
            item_id,
            // SELLER ID
            seller_id,
            // ITEM_NAME
            null,
            // CURRENT PRICE
            i_current_price,
            // NUM BIDS
            i_num_bids,
            // END DATE
            i_end_date,
            // STATUS
            i_status.ordinal(),
            // PURCHASE ID
            ip_id,
            // BID ID
            ib_id,
            // BUYER ID
            ib_buyer_id,
        };
    }	
}