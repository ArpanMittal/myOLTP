package com.oltpbenchmark.benchmarks.ycsb.results;

import com.usc.dblab.cafe.QueryResult;

public class UserResult extends QueryResult {
	final int ycsb_key;
    String field_01;
    String field_02;
    String field_03;
    String field_04;
    String field_05;
    String field_06;
    String field_07;
    String field_08;
    String field_09;
    String field_10;
	/**
	 * @param query
	 * @param ycsb_key
	 * @param field_01
	 * @param field_02
	 * @param field_03
	 * @param field_04
	 * @param field_05
	 * @param field_06
	 * @param field_07
	 * @param field_08
	 * @param field_09
	 * @param field_10
	 */
	public UserResult(String query, int ycsb_key, String field_01, String field_02, String field_03, String field_04,
			String field_05, String field_06, String field_07, String field_08, String field_09, String field_10) {
		super(query);
		this.ycsb_key = ycsb_key;
		this.field_01 = field_01;
		this.field_02 = field_02;
		this.field_03 = field_03;
		this.field_04 = field_04;
		this.field_05 = field_05;
		this.field_06 = field_06;
		this.field_07 = field_07;
		this.field_08 = field_08;
		this.field_09 = field_09;
		this.field_10 = field_10;
	}
	/**
	 * @return the ycsb_key
	 */
	public int getYcsb_key() {
		return ycsb_key;
	}
	/**
	 * @return the field_01
	 */
	public String getField_01() {
		return field_01;
	}
	/**
	 * @return the field_02
	 */
	public String getField_02() {
		return field_02;
	}
	/**
	 * @return the field_03
	 */
	public String getField_03() {
		return field_03;
	}
	/**
	 * @return the field_04
	 */
	public String getField_04() {
		return field_04;
	}
	/**
	 * @return the field_05
	 */
	public String getField_05() {
		return field_05;
	}
	/**
	 * @return the field_06
	 */
	public String getField_06() {
		return field_06;
	}
	/**
	 * @return the field_07
	 */
	public String getField_07() {
		return field_07;
	}
	/**
	 * @return the field_08
	 */
	public String getField_08() {
		return field_08;
	}
	/**
	 * @return the field_09
	 */
	public String getField_09() {
		return field_09;
	}
	/**
	 * @return the field_10
	 */
	public String getField_10() {
		return field_10;
	}
	/**
	 * @param field_01 the field_01 to set
	 */
	public void setField_01(String field_01) {
		this.field_01 = field_01;
	}
	/**
	 * @param field_02 the field_02 to set
	 */
	public void setField_02(String field_02) {
		this.field_02 = field_02;
	}
	/**
	 * @param field_03 the field_03 to set
	 */
	public void setField_03(String field_03) {
		this.field_03 = field_03;
	}
	/**
	 * @param field_04 the field_04 to set
	 */
	public void setField_04(String field_04) {
		this.field_04 = field_04;
	}
	/**
	 * @param field_05 the field_05 to set
	 */
	public void setField_05(String field_05) {
		this.field_05 = field_05;
	}
	/**
	 * @param field_06 the field_06 to set
	 */
	public void setField_06(String field_06) {
		this.field_06 = field_06;
	}
	/**
	 * @param field_07 the field_07 to set
	 */
	public void setField_07(String field_07) {
		this.field_07 = field_07;
	}
	/**
	 * @param field_08 the field_08 to set
	 */
	public void setField_08(String field_08) {
		this.field_08 = field_08;
	}
	/**
	 * @param field_09 the field_09 to set
	 */
	public void setField_09(String field_09) {
		this.field_09 = field_09;
	}
	/**
	 * @param field_10 the field_10 to set
	 */
	public void setField_10(String field_10) {
		this.field_10 = field_10;
	}
	
	
    
	

}
