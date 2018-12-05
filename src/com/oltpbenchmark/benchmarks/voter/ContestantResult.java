package com.oltpbenchmark.benchmarks.voter;

import com.usc.dblab.cafe.QueryResult;

public class ContestantResult extends QueryResult {
	final int key;
    String name;
	
	public ContestantResult(String query, int key, String name) {
		super(query);
		this.key = key;
		this.name = name;
		
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @return the key
	 */
	public int getKey() {
		return key;
	}
	
	
}
