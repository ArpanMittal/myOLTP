/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
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
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.benchmarks.voter.ContestantResult;
import com.oltpbenchmark.benchmarks.voter.StateResult;
import com.oltpbenchmark.benchmarks.voter.VoteCountResult;
import com.oltpbenchmark.benchmarks.voter.VoterConstants;
import com.oltpbenchmark.benchmarks.ycsb.YCSBConstants;
import com.usc.dblab.cafe.NgCache;

public class Vote extends Procedure {
	
    // potential return codes
    public static final long VOTE_SUCCESSFUL = 0;
    public static final long ERR_INVALID_CONTESTANT = 1;
    public static final long ERR_VOTER_OVER_VOTE_LIMIT = 2;
	
    // Checks if the vote is for a valid contestant
    public final SQLStmt checkContestantStmt = new SQLStmt(
	   "SELECT * FROM CONTESTANTS WHERE contestant_number = ?;"
    );
	
    // Checks if the voter has exceeded their allowed number of votes
    public final SQLStmt checkVoterStmt = new SQLStmt(
		"SELECT COUNT(*) FROM VOTES WHERE phone_number = ?;"
    );
	
    // Checks an area code to retrieve the corresponding state
    public final SQLStmt checkStateStmt = new SQLStmt(
		"SELECT * FROM AREA_CODE_STATE WHERE area_code = ?;"
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
    
    
public long run(Connection conn, long voteId, long phoneNumber, int contestantNumber, long maxVotesPerPhoneNumber, NgCache cafe) throws SQLException {
	try {
		cafe.startSession("Vote");
		String getContestant = String.format(VoterConstants.TABLENAME_CONTESTANTS_KEY, contestantNumber);
		ContestantResult contestantResut = (ContestantResult)cafe.readStatement(getContestant);
		if(contestantResut == null) {
			return ERR_INVALID_CONTESTANT; 
		}

		String getVoterCount = String.format(VoterConstants.TABLENAME_VOTES_KEY, phoneNumber,maxVotesPerPhoneNumber);
		VoteCountResult voterCountResult = (VoteCountResult)cafe.readStatement(getVoterCount);
		if(voterCountResult == null || Integer.parseInt(voterCountResult.getVote_count())>maxVotesPerPhoneNumber)
			return ERR_VOTER_OVER_VOTE_LIMIT;

		
		String getState = String.format(VoterConstants.TABLENAME_LOCATIONS_KEY,(short)(phoneNumber / 10000000l) );
		StateResult stateResult = (StateResult)cafe.readStatement(getState);
		String state;
		if(stateResult == null)
			state = "XX";
		else
			state = stateResult.getState_name();

		
		String insertVoteKey = String.format(VoterConstants.INSER_TABLENAME_VOTES_KEY, voteId, phoneNumber,state,contestantNumber);
		
		boolean success = cafe.writeStatement(insertVoteKey);
        assert(success) :
            String.format("Failed to insert %s for Votes #%s", voteId, phoneNumber);
        
        String updateVoterCount = String.format(VoterConstants.UPDATE_TABLENAME_VOTES_KEY, phoneNumber, maxVotesPerPhoneNumber,Integer.parseInt(voterCountResult.getVote_count())+1);
        success = cafe.writeStatement(updateVoterCount);
		assert(success):
			String.format("Failed to increment update for Votes #%s", phoneNumber);

		
		  if (cafe.validateSession()) {
              conn.commit();
              cafe.commitSession();
          } else {
              conn.rollback();
              cafe.abortSession();
          }
	}catch (Exception e) {
//	    e.printStackTrace(System.out);
		conn.rollback();
		try {
			cafe.abortSession();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		throw new UserAbortException("Some error happens. "+ e.getMessage());
    
	}
	
        return VOTE_SUCCESSFUL;
	
		
    }
    

// For Cache warmup
public long warmuprun(Connection conn, long voteId, long phoneNumber, int contestantNumber, long maxVotesPerPhoneNumber, NgCache cafe) throws SQLException {
	try {
		cafe.startSession("Vote");
		String getContestant = String.format(VoterConstants.TABLENAME_CONTESTANTS_KEY, contestantNumber);
		ContestantResult contestantResut = (ContestantResult)cafe.readStatement(getContestant);
		if(contestantResut == null) {
			return ERR_INVALID_CONTESTANT; 
		}

		String getVoterCount = String.format(VoterConstants.TABLENAME_VOTES_KEY, phoneNumber,maxVotesPerPhoneNumber);
		VoteCountResult voterCountResult = (VoteCountResult)cafe.readStatement(getVoterCount);
		if(voterCountResult == null || Integer.parseInt(voterCountResult.getVote_count())>maxVotesPerPhoneNumber)
			return ERR_VOTER_OVER_VOTE_LIMIT;
		
		String getState = String.format(VoterConstants.TABLENAME_LOCATIONS_KEY,(short)(phoneNumber / 10000000l) );
		StateResult stateResult = (StateResult)cafe.readStatement(getState);
		String state;
		if(stateResult == null)
			state = "XX";
		else
			state = stateResult.getState_name();
		
		  if (cafe.validateSession()) {
              conn.commit();
              cafe.commitSession();
          } else {
              conn.rollback();
              cafe.abortSession();
          }
	}catch (Exception e) {
		conn.rollback();
		try {
			cafe.abortSession();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		throw new UserAbortException("Some error happens. "+ e.getMessage());
    
	}
	System.out.println("heelo"+VOTE_SUCCESSFUL+"=true");
        return VOTE_SUCCESSFUL;
	
//	return maxVotesPerPhoneNumber;		
    }
    
}
