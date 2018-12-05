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



package com.oltpbenchmark.benchmarks.voter;

public abstract class VoterConstants {

	public static final int MAX_VOTES = 1000; 
	public static final int NUM_CONTESTANTS = 6; 
	
	public static final String TABLENAME_CONTESTANTS = "CONTESTANTS";
	public static final String TABLENAME_VOTES = "VOTES";
	public static final String TABLENAME_LOCATIONS = "AREA_CODE_STATE";
	
	public static final String WB_TABLENAME_CONTESTANTS = "WB_CONTESTANTS";
	public static final String WB_TABLENAME_VOTES = "WB_VOTES";
	public static final String WB_TABLENAME_LOCATIONS = "WB_AREA_CODE_STATE";
	// Initialize some common constants and variables
    public static final String CONTESTANT_NAMES_CSV = "Edwina Burnam,Tabatha Gehling,Kelly Clauss,Jessie Alloway," +
											   "Alana Bregman,Jessie Eichman,Allie Rogalski,Nita Coster," +
											   "Kurt Walser,Ericka Dieter,Loraine NygrenTania Mattioli";
    public static final String TABLENAME_CONTESTANTS_KEY = TABLENAME_CONTESTANTS+",%s";
    public static final String TABLENAME_VOTES_KEY = TABLENAME_VOTES+",%s"+",%s";
    public static final String TABLENAME_LOCATIONS_KEY = TABLENAME_LOCATIONS+",%s";
    
    public static final String TABLENAME_INSERT_VOTES_KEY= TABLENAME_VOTES+",%s";
    
    public static final String WB_TABLENAME_CONTESTANTS_KEY = WB_TABLENAME_CONTESTANTS+",%s";
    public static final String WB_TABLENAME_VOTES_KEY = WB_TABLENAME_VOTES+",%s"+",%s";
    public static final String WB_TABLENAME_LOCATIONS_key = WB_TABLENAME_LOCATIONS+",%s";
    
    public static final String WB_TABLENAME_INSERT_VOTES_KEY= WB_TABLENAME_VOTES+",%s";
    
    
    
    
    
    
}
