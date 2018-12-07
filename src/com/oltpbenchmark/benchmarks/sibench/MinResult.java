package com.oltpbenchmark.benchmarks.sibench;

import com.usc.dblab.cafe.QueryResult;

public class MinResult extends QueryResult{
	

	int id ;
	int value;
	
	/**
	 * @return the value
	 */
	public int getValue() {
		return value;
	}

	/**
	 * @param value the value to set
	 */
	public void setValue(int value) {
		this.value = value;
	}

	public MinResult(String query,int id, int value) {
		super(query);
		this.id = id;
		this.value = value;
	}

	/**
	 * @return the id
	 */
	public int getId() {
		return id;
	}

	/**
	 * @param id the id to set
	 */
	public void setId(int id) {
		this.id = id;
	}

	
	
	
	
}
