/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

//
// Accepts a vote, enforcing business logic: make sure the vote is for a valid
// contestant and that the voter (phone number of the caller) is not above the
// number of allowed votes.
//

package com.oltpbenchmark.benchmarks.voter.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

public class Vote extends Procedure {
	
    // potential return codes
    public static final long VOTE_SUCCESSFUL = 0;
    public static final long ERR_INVALID_CONTESTANT = 1;
    public static final long ERR_VOTER_OVER_VOTE_LIMIT = 2;
	
    // Checks if the vote is for a valid contestant
    public final SQLStmt checkContestantStmt = new SQLStmt(
	   "SELECT contestant_number FROM CONTESTANTS WHERE contestant_number = ?;"
    );
	
    // Checks if the voter has exceeded their allowed number of votes
    public final SQLStmt checkVoterStmt = new SQLStmt(
		"SELECT COUNT(*) FROM VOTES WHERE phone_number = ?;"
    );
	
    // Checks an area code to retrieve the corresponding state
    public final SQLStmt checkStateStmt = new SQLStmt(
		"SELECT state FROM AREA_CODE_STATE WHERE area_code = ?;"
    );
	
    // Records a vote
    public final SQLStmt insertVoteStmt = new SQLStmt(
		"INSERT INTO VOTES (vote_id, phone_number, state, contestant_number, created) " +
    "VALUES (?, ?, ?, ?, NOW());"
    );
	
    public long run(Connection conn, long voteId, long phoneNumber, int contestantNumber, long maxVotesPerPhoneNumber) throws SQLException {
		
        PreparedStatement ps = getPreparedStatement(conn, checkContestantStmt);
        ps.setInt(1, contestantNumber);
        ResultSet rs = ps.executeQuery();
        try {
            if (!rs.next()) {
                return ERR_INVALID_CONTESTANT;    
            }
        } finally {
            rs.close();
        }
        
        ps = getPreparedStatement(conn, checkVoterStmt);
        ps.setLong(1, phoneNumber);
        rs = ps.executeQuery();
        boolean hasVoterEnt = rs.next();
        try {
            if (hasVoterEnt && rs.getLong(1) >= maxVotesPerPhoneNumber) {
                return ERR_VOTER_OVER_VOTE_LIMIT;
            }
        } finally {
            rs.close();
        }
        
        ps = getPreparedStatement(conn, checkStateStmt);
        ps.setShort(1, (short)(phoneNumber / 10000000l));
        rs = ps.executeQuery();
        // Some sample client libraries use the legacy random phone generation that mostly
        // created invalid phone numbers. Until refactoring, re-assign all such votes to
        // the "XX" fake state (those votes will not appear on the Live Statistics dashboard,
        // but are tracked as legitimate instead of invalid, as old clients would mostly get
        // it wrong and see all their transactions rejected).
        final String state = rs.next() ? rs.getString(1) : "XX";
        rs.close();

        ps = getPreparedStatement(conn, insertVoteStmt);
        ps.setLong(1, voteId);
        ps.setLong(2, phoneNumber);
        ps.setString(3, state);
        ps.setInt(4, contestantNumber);
        ps.execute();
		
        // Set the return value to 0: successful vote
        return VOTE_SUCCESSFUL;
    }
}
