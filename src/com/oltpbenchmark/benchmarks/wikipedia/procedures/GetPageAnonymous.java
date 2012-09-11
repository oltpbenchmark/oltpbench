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
import java.util.ArrayList;
import java.util.Random;

import net.spy.memcached.MemcachedClient;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.wikipedia.WikipediaConstants;
import com.oltpbenchmark.benchmarks.wikipedia.util.Article;
import com.oltpbenchmark.distributions.ZipFianDistribution;

public class GetPageAnonymous extends Procedure {
	
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
	// XXX this is hard for translation
	public SQLStmt selectIpBlocks = new SQLStmt(
        "SELECT * FROM " + WikipediaConstants.TABLENAME_IPBLOCKS + 
        " WHERE ipb_address = ?"
    ); 
	
	public SQLStmt selectRevisionsByPage = new SQLStmt(
	        "SELECT rev_id FROM " + WikipediaConstants.TABLENAME_REVISION + " WHERE page_id = ?");
	
	public SQLStmt selectPageRevision = new SQLStmt(
        "SELECT * " +
	    "  FROM " + WikipediaConstants.TABLENAME_PAGE + ", " +
	                WikipediaConstants.TABLENAME_REVISION +
	    " WHERE page_id = rev_page " +
        "   AND rev_page = ? " +
	    "   AND page_id = ? " +
        "   AND rev_id = page_latest LIMIT 1"
    );
	
	public SQLStmt selectPageRevisionNotLatest = new SQLStmt(
	        "SELECT * " +
	                "  FROM " + WikipediaConstants.TABLENAME_PAGE + ", " +
	                WikipediaConstants.TABLENAME_REVISION +
	                " WHERE page_id = rev_page " +
	                "   AND rev_page = ? " +
	                "   AND page_id = ? " +
	                "   AND rev_id = ? LIMIT 1"
	        );
	
	public SQLStmt selectText = new SQLStmt(
        "SELECT old_text, old_flags FROM " + WikipediaConstants.TABLENAME_TEXT +
        " WHERE old_id = ? LIMIT 1"
    );

	private String mcKeyRevisionTextKey(int textId) {
	    return "wikidb:revisiontext:textid:" + textId;
	}
	
	private final Random rnd = new Random();
	
	private boolean flipCoin(double phead) {
	    assert phead <= 1.0;
	    return rnd.nextDouble() <= phead;
	}
	
	// -----------------------------------------------------------------
    // RUN
    // -----------------------------------------------------------------
	
	public Article run(Connection conn, MemcachedClient mcclient,
	        boolean forSelect, String userIp,
			int pageNamespace, String pageTitle) throws UserAbortException, SQLException {		
	    int param = 1;
	    
		PreparedStatement st = this.getPreparedStatement(conn, selectPage);
        st.setInt(param++, pageNamespace);
        st.setString(param++, pageTitle);
        ResultSet rs = st.executeQuery();
        if (!rs.next()) {
            String msg = String.format("Invalid Page: Namespace:%d / Title:--%s--", pageNamespace, pageTitle);
            throw new UserAbortException(msg);
        }
        int pageId = rs.getInt(1);
        rs.close();

        st = this.getPreparedStatement(conn, selectPageRestriction);
        st.setInt(1, pageId);
        rs = st.executeQuery();
        while (rs.next()) {
            byte[] pr_type = rs.getBytes(1);
            assert(pr_type != null);
        } // WHILE
        rs.close();
        // check using blocking of a user by either the IP address or the
        // user_name

        st = this.getPreparedStatement(conn, selectIpBlocks);
		st.setString(1, userIp);
        rs = st.executeQuery();
        while (rs.next()) {
            byte[] ipb_expiry = rs.getBytes(11);
            assert(ipb_expiry != null);
        } // WHILE
        rs.close();

        int revisionId, textId;
        if (flipCoin(WikipediaConstants.PAST_REV_CHECK_PROB)) {
            // get a list of all past revisions
            st = this.getPreparedStatement(conn, selectRevisionsByPage);
            st.setInt(1, pageId);
            rs = st.executeQuery();
            
            ArrayList<Integer> revIds = new ArrayList<Integer>();
            while (rs.next()) {
                revIds.add(rs.getInt(1));
            }
            if (revIds.isEmpty()) {
                String msg = "bad pageid: " + pageId;
                throw new UserAbortException(msg);
            }
            
            int revId;
            if (revIds.size() == 1) {
                // only one rev, so we must pick it
                revId = revIds.get(0);
            } else {
            
                // revIds.size() - 1 so we exclude the latest revision (we want to make
                // this branch load *not* the latest page
                ZipFianDistribution zipf = new ZipFianDistribution(
                        rnd, revIds.size() - 1, WikipediaConstants.PAST_REV_ZIPF_SKEW);
                
                // pick the index into revIds
                int idx = zipf.next();
                assert idx >= 0 && idx < (revIds.size() - 1);
                    
                // index from the end, since we want to favor latest revisions
                revId = revIds.get( (revIds.size() - 2) - idx );
            }
            
            st = this.getPreparedStatement(conn, selectPageRevisionNotLatest);
            st.setInt(1, pageId);
            st.setInt(2, pageId);
            st.setInt(3, revId);
            rs = st.executeQuery();
            if (!rs.next()) {
                String msg = String.format("Invalid Page: Namespace:%d / Title:--%s-- / PageId:%d",
                                           pageNamespace, pageTitle, pageId);
                throw new UserAbortException(msg);
            }

            revisionId = rs.getInt("rev_id");
            textId = rs.getInt("rev_text_id");
            assert !rs.next();
            rs.close();
        } else {
            // only interested in latest revision
            st = this.getPreparedStatement(conn, selectPageRevision);
            st.setInt(1, pageId);
            st.setInt(2, pageId);
            rs = st.executeQuery();
            if (!rs.next()) {
                String msg = String.format("Invalid Page: Namespace:%d / Title:--%s-- / PageId:%d",
                                           pageNamespace, pageTitle, pageId);
                throw new UserAbortException(msg);
            }

            revisionId = rs.getInt("rev_id");
            textId = rs.getInt("rev_text_id");
            assert !rs.next();
            rs.close();
        }
        
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
                String msg = "No such text: " + textId + " for page_id:" + pageId + " page_namespace: " + pageNamespace + " page_title:" + pageTitle;
                throw new UserAbortException(msg);
            }
            if (mcclient != null) {
                mcclient.add(mcKeyRevisionTextKey(textId), WikipediaConstants.MC_KEY_TIMEOUT, rs.getString(1));
            }
        }

        Article a = null;
        if (!forSelect)
			a = new Article(userIp, pageId, rs.getString("old_text"), textId, revisionId);
        assert !rs.next();
        rs.close();
        return a;
    }

}
