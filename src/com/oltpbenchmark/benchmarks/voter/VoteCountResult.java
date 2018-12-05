package com.oltpbenchmark.benchmarks.voter;

import com.usc.dblab.cafe.QueryResult;

public class VoteCountResult extends QueryResult {
	int voter_id;
	String vote_count;
	/**
	 * @param query
	 * @param voter_id
	 * @param vote_count
	 */
	public VoteCountResult(String query, int phone_num, String vote_count) {
		super(query);
		this.voter_id = phone_num;
		this.vote_count = vote_count;
	}
	/**
	 * @return the voter_id
	 */
	public int getPhone_num() {
		return voter_id;
	}
	/**
	 * @param voter_id the voter_id to set
	 */
	public void setPhone_num(int voter_id) {
		this.voter_id = voter_id;
	}
	/**
	 * @return the vote_count
	 */
	public String getVote_count() {
		return vote_count;
	}
	/**
	 * @param vote_count the vote_count to set
	 */
	public void setVote_count(String vote_count) {
		this.vote_count = vote_count;
	}
	
	

}
