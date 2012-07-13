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
package com.oltpbenchmark.benchmarks.wikipedia.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import net.spy.memcached.MemcachedClient;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.benchmarks.wikipedia.WikipediaConstants;
import com.oltpbenchmark.benchmarks.wikipedia.util.Article;

public class GetPageAuthenticated extends Procedure {
	
    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------
    
    public SQLStmt selectPage = new SQLStmt(
        "SELECT * FROM " + WikipediaConstants.TABLENAME_PAGE + 
        " WHERE page_namespace = ? AND page_title = ? LIMIT 1"
    );
    public SQLStmt selectPageRestriction = new SQLStmt(
        "SELECT * FROM " + WikipediaConstants.TABLENAME_PAGE_RESTRICTIONS + 
        " WHERE pr_page = ?"
    );
    public SQLStmt selectIpBlocks = new SQLStmt(
        "SELECT * FROM " + WikipediaConstants.TABLENAME_IPBLOCKS +
        " WHERE ipb_user = ?"
    ); 
    public SQLStmt selectPageRevision = new SQLStmt(
        "SELECT * " +
        "  FROM " + WikipediaConstants.TABLENAME_PAGE + ", " +
                    WikipediaConstants.TABLENAME_REVISION +
        " WHERE page_id = rev_page " +
        "   AND rev_page = ? " +
        "   AND page_id = ? " +
        "   AND rev_id = page_latest LIMIT 1"
    );
    public SQLStmt selectText = new SQLStmt(
        "SELECT old_text,old_flags FROM " + WikipediaConstants.TABLENAME_TEXT +
        " WHERE old_id = ? LIMIT 1"
    );
    
	public SQLStmt selectUser = new SQLStmt(
        "SELECT * FROM " + WikipediaConstants.TABLENAME_USER + 
        " WHERE user_id = ? LIMIT 1"
    );
	public SQLStmt selectGroup = new SQLStmt(
        "SELECT ug_group FROM " + WikipediaConstants.TABLENAME_USER_GROUPS + 
        " WHERE ug_user = ?"
    );

	private String mcUserTableKey(int userId) {
	    return "wikidb:user:id" + userId;
	}
	
	private String mcKeyRevisionTextKey(int textId) {
	    return "wikidb:revisiontext:textid:" + textId;
	}
	
    // -----------------------------------------------------------------
    // RUN
    // -----------------------------------------------------------------
	
    public Article run(Connection conn, MemcachedClient mcclient, boolean forSelect, String userIp, int userId, int nameSpace, String pageTitle) throws SQLException {
        // =======================================================
        // LOADING BASIC DATA: txn1
        // =======================================================
        // Retrieve the user data, if the user is logged in

        // FIXME TOO FREQUENTLY SELECTING BY USER_ID
        String userText = userIp;
        PreparedStatement st = this.getPreparedStatement(conn, selectUser);
        if (userId > 0) {
            boolean doFetchUserTable = true;
            boolean doFetchGrpTable = true;
            if (mcclient != null) {
                Object x;
                if ((x = mcclient.get(mcUserTableKey(userId))) != null) {
                    userText = (String) x;
                    doFetchUserTable = false;
                    // XXX: hack- we use the first character of the mc value to denote
                    // whether or not we have "cachced" the groups
                    assert(userText.length() > 1);
                    doFetchGrpTable = userText.charAt(0) != '1';
                    userText = userText.substring(1);
                }
            }

            ResultSet rs;
            if (doFetchUserTable) {
                st.setInt(1, userId);
                rs = st.executeQuery();
                if (rs.next()) {
                    userText = rs.getString("user_name");
                } else {
                    rs.close();
                    throw new UserAbortException("Invalid UserId: " + userId);
                }
                rs.close();
            }
            
            if (doFetchGrpTable) {
                // Fetch all groups the user might belong to (access control
                // information)
                st = this.getPreparedStatement(conn, selectGroup);
                st.setInt(1, userId);
                rs = st.executeQuery();
                while (rs.next()) {
                    @SuppressWarnings("unused")
                    String userGroupName = rs.getString(1);
                }
                rs.close();
            }
            
            if (mcclient != null) {
                // XXX: we really should be putting group name information for the
                // memcache value, but since the default transcoder uses java serialization
                // for complex type (eg arrays) we just don't store it to avoid a perf
                // penalty (which could be optimized away if we cared)
                mcclient.add(mcUserTableKey(userId), WikipediaConstants.MC_KEY_TIMEOUT, "1" + userText);
            }
        }

        st = this.getPreparedStatement(conn, selectPage);
        st.setInt(1, nameSpace);
        st.setString(2, pageTitle);
        ResultSet rs = st.executeQuery();

        if (!rs.next()) {
            rs.close();
            throw new UserAbortException("INVALID page namespace/title:" + nameSpace + "/" + pageTitle);
        }
        int pageId = rs.getInt("page_id");
        assert !rs.next();
        rs.close();

        st = this.getPreparedStatement(conn, selectPageRestriction);
        st.setInt(1, pageId);
        rs = st.executeQuery();
        while (rs.next()) {
            byte[] pr_type = rs.getBytes(1);
            assert(pr_type != null);
        }
        rs.close();
        
        // check using blocking of a user by either the IP address or the
        // user_name
        st = this.getPreparedStatement(conn, selectIpBlocks);
        st.setInt(1, userId);
        rs = st.executeQuery();
        while (rs.next()) {
            byte[] ipb_expiry = rs.getBytes(11);
            assert(ipb_expiry != null);
        }
        rs.close();

        st = this.getPreparedStatement(conn, selectPageRevision);
        st.setInt(1, pageId);
        st.setInt(2, pageId);
        rs = st.executeQuery();
        if (!rs.next()) {
            rs.close();
            throw new UserAbortException("no such revision: page_id:" + pageId + " page_namespace: " + nameSpace + " page_title:" + pageTitle);
        }

        int revisionId = rs.getInt("rev_id");
        int textId = rs.getInt("rev_text_id");
        assert !rs.next();
        rs.close();

        boolean doFetch = true;
        if (mcclient != null && mcclient.get(mcKeyRevisionTextKey(textId)) != null) {
            doFetch = false;
        }
        if (doFetch) {
            // NOTE: the following is our variation of wikipedia... the original did
            // not contain old_page column!
            // sql =
            // "SELECT old_text,old_flags FROM `text` WHERE old_id = '"+textId+"' AND old_page = '"+pageId+"' LIMIT 1";
            // For now we run the original one, which works on the data we have
            st = this.getPreparedStatement(conn, selectText);
            st.setInt(1, textId);
            rs = st.executeQuery();
            if (!rs.next()) {
                String msg = "No such text: " + textId + " for page_id:" + pageId + " page_namespace: " + nameSpace + " page_title:" + pageTitle;
                throw new UserAbortException(msg);
            }
            if (mcclient != null) {
                mcclient.add(mcKeyRevisionTextKey(textId), WikipediaConstants.MC_KEY_TIMEOUT, rs.getString(1));
            }
        }
        
        Article a = null;
        if (!forSelect)
            a = new Article(userText, pageId, rs.getString("old_text"), textId, revisionId);
        assert !rs.next();
        rs.close();

        return a;
    }

}
