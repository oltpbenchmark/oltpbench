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
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkConstants;
import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkConstants.ItemStatus;
import com.oltpbenchmark.benchmarks.auctionmark.util.AuctionMarkUtil;

/**
 * NewItem
 * @author pavlo
 * @author visawee
 */
public class NewItem extends Procedure {
    private static final Logger LOG = Logger.getLogger(NewItem.class);
    
	// -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------
	
    public final SQLStmt insertItem = new SQLStmt(
        "INSERT INTO " + AuctionMarkConstants.TABLENAME_ITEM + "(" +
        	"i_id," + 
        	"i_u_id," + 
        	"i_c_id," + 
        	"i_name," + 
        	"i_description," + 
        	"i_user_attributes," + 
        	"i_initial_price," +
        	"i_current_price," + 
        	"i_num_bids," + 
        	"i_num_images," + 
        	"i_num_global_attrs," + 
        	"i_start_date," + 
        	"i_end_date," +
        	"i_status, " +
        	"i_updated," +
        	"i_iattr0" + 
        ") VALUES (" +
            "?," +  // i_id
            "?," +  // i_u_id
            "?," +  // i_c_id
            "?," +  // i_name
            "?," +  // i_description
            "?," +  // i_user_attributes
            "?," +  // i_initial_price
            "?," +  // i_current_price
            "?," +  // i_num_bids
            "?," +  // i_num_images
            "?," +  // i_num_global_attrs
            "?," +  // i_start_date
            "?," +  // i_end_date
            "?," +  // i_status
            "?," +  // i_updated
            "1"  +  // i_attr0
        ")"
    );
    
    public final SQLStmt getCategory = new SQLStmt(
        "SELECT * FROM " + AuctionMarkConstants.TABLENAME_CATEGORY + " WHERE c_id = ? "
    );
    
    public final SQLStmt getCategoryParent = new SQLStmt(
        "SELECT * FROM " + AuctionMarkConstants.TABLENAME_CATEGORY + " WHERE c_parent_id = ? "
    );
    
    public final SQLStmt getGlobalAttribute = new SQLStmt(
        "SELECT gag_name, gav_name, gag_c_id " +
          "FROM " + AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_GROUP + ", " +
                    AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_VALUE +
        " WHERE gav_id = ? AND gav_gag_id = ? " +
           "AND gav_gag_id = gag_id"
    );
    
    public final SQLStmt insertItemAttribute = new SQLStmt(
		"INSERT INTO " + AuctionMarkConstants.TABLENAME_ITEM_ATTRIBUTE + "(" +
			"ia_id," + 
			"ia_i_id," + 
			"ia_u_id," + 
			"ia_gav_id," + 
			"ia_gag_id" + 
		") VALUES(?, ?, ?, ?, ?)"
	);

    public final SQLStmt insertImage = new SQLStmt(
		"INSERT INTO " + AuctionMarkConstants.TABLENAME_ITEM_IMAGE + "(" +
			"ii_id," + 
			"ii_i_id," + 
			"ii_u_id," + 
			"ii_sattr0" + 
		") VALUES(?, ?, ?, ?)"
	);
    
    public final SQLStmt updateUserBalance = new SQLStmt(
		"UPDATE " + AuctionMarkConstants.TABLENAME_USER + " " +
		   "SET u_balance = u_balance - 1, " +
		   "    u_updated = ? " +
		" WHERE u_id = ?"
	);
    
    // -----------------------------------------------------------------
    // RUN METHOD
    // -----------------------------------------------------------------
    
	/**
	 * Insert a new ITEM record for a user.
	 * The benchmark client provides all of the preliminary information 
	 * required for the new item, as well as optional information to create
	 * derivative image and attribute records. After inserting the new ITEM
	 * record, the transaction then inserts any GLOBAL ATTRIBUTE VALUE and
	 * ITEM IMAGE. The unique identifer for each of these records is a
	 * composite 64-bit key where the lower 60-bits are the i id parameter and the
	 * upper 4-bits are used to represent the index of the image/attribute.
	 * For example, if the i id is 100 and there are four items, then the
	 * composite key will be 0 100 for the first image, 1 100 for the second,
	 * and so on. After these records are inserted, the transaction then updates
	 * the USER record to add the listing fee to the seller's balance.
	 */
    public Object[] run(Connection conn, Date benchmarkTimes[],
                        long item_id, long seller_id, long category_id,
                        String name, String description, long duration, double initial_price, String attributes,
                        long gag_ids[], long gav_ids[], String images[]) throws SQLException {
        final Date currentTime = AuctionMarkUtil.getProcTimestamp(benchmarkTimes);
        final boolean debug = LOG.isDebugEnabled();
        
        // Calculate endDate
        Date end_date = new Date(currentTime.getTime() + (duration * AuctionMarkConstants.MILLISECONDS_IN_A_DAY));
        
        if (debug) {
            LOG.debug("NewItem :: run ");
            LOG.debug(">> item_id = " + item_id + " , seller_id = " + seller_id + ", category_id = " + category_id);
            LOG.debug(">> name = " + name + " , description length = " + description.length());
            LOG.debug(">> initial_price = " + initial_price + " , attributes length = " + attributes.length());
            LOG.debug(">> gag_ids[].length = " + gag_ids.length + " , gav_ids[] length = " + gav_ids.length);
            LOG.debug(">> image length = " + images.length + " ");
            LOG.debug(">> start = " + currentTime + ", end = " + end_date);
        }

        // Get attribute names and category path and append
        // them to the item description
        PreparedStatement stmt = null;
        ResultSet results = null;
        int updated;
        
        // ATTRIBUTES
        description += "\nATTRIBUTES: ";
        stmt = this.getPreparedStatement(conn, getGlobalAttribute);
        for (int i = 0; i < gag_ids.length; i++) {
            int col = 1;
            stmt.setLong(col++, gav_ids[i]);
            stmt.setLong(col++, gag_ids[i]);
            results = stmt.executeQuery();
            if (results.next()) {
                col = 1;
                description += String.format(" * %s -> %s\n", results.getString(col++), results.getString(col++));
            }
        } // FOR
        
        // CATEGORY
        stmt = this.getPreparedStatement(conn, getCategory, category_id);
        results = stmt.executeQuery();
        boolean adv = results.next();
        assert(adv);
        String category_name = results.getString(1);
        
        stmt = this.getPreparedStatement(conn, getCategoryParent, category_id);
        results = stmt.executeQuery();
        String category_parent = null;
        if (results.next()) {
            category_parent = results.getString(1);
        } else {
            category_parent = "<ROOT>";
        }
        description += String.format("\nCATEGORY: %s >> %s", category_parent, category_name);

        // Insert new ITEM tuple
        updated = this.getPreparedStatement(conn, insertItem,
                                            item_id, seller_id, category_id,
                                            name, description, attributes,
                                            initial_price, initial_price, 0,
                                            images.length, gav_ids.length,
                                            currentTime, end_date,
                                            ItemStatus.OPEN.ordinal(), currentTime).executeUpdate();
        assert(updated == 1);

        // Insert ITEM_ATTRIBUTE tuples
        stmt = this.getPreparedStatement(conn, insertItemAttribute);
        for (int i = 0; i < gav_ids.length; i++) {
            int param = 1;
            stmt.setLong(param++, AuctionMarkUtil.getUniqueElementId(item_id, i));
            stmt.setLong(param++, item_id);
            stmt.setLong(param++, seller_id);
            stmt.setLong(param++, gag_ids[i]);
            stmt.setLong(param++, gag_ids[i]);
            updated = stmt.executeUpdate();
            assert(updated == 1);
        } // FOR
        
        // Insert ITEM_IMAGE tuples
        stmt = this.getPreparedStatement(conn, insertImage); 
        for (int i = 0; i < images.length; i++) {
            int param = 1;
            stmt.setLong(param++, AuctionMarkUtil.getUniqueElementId(item_id, i));
            stmt.setLong(param++, item_id);
            stmt.setLong(param++, seller_id);
            stmt.setString(param++, images[i]);
            updated = stmt.executeUpdate();
            assert(updated == 1);
        } // FOR

        // Update listing fee
        updated = this.getPreparedStatement(conn, updateUserBalance, currentTime, seller_id).executeUpdate();
        assert(updated == 1);
        
        // Return new item_id and user_id
        return new Object[] {
            // ITEM ID
            item_id,
            // SELLER ID
            seller_id,
            // ITEM_NAME
            name,
            // CURRENT PRICE
            initial_price,
            // NUM BIDS
            0l,
            // END DATE
            end_date,
            // STATUS
            ItemStatus.OPEN.ordinal()
        };
    }
}