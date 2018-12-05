package com.oltpbenchmark.benchmarks.voter;

import com.usc.dblab.cafe.QueryResult;

public class StateResult extends QueryResult{

	final int area_code;
    String state_name;
	/**
	 * @param query
	 * @param area_code
	 * @param state_name
	 */
	public StateResult(String query, int area_code, String state_name) {
		super(query);
		this.area_code = area_code;
		this.state_name = state_name;
	}
	/**
	 * @return the state_name
	 */
	public String getState_name() {
		return state_name;
	}
	/**
	 * @param state_name the state_name to set
	 */
	public void setState_name(String state_name) {
		this.state_name = state_name;
	}
	/**
	 * @return the area_code
	 */
	public int getArea_code() {
		return area_code;
	}
    
	

}
