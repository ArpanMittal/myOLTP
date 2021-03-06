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

package com.oltpbenchmark.benchmarks.ycsb;

public final class YCSBConstants {

    public static final int RECORD_COUNT = 800000;
    
    public static final int NUM_FIELDS = 10;
    
    public static final int FIELD_SIZE = 100; // chars
    
    public static final String QUERY_USERTABLE = "Q_USERTABLE";
    public static final String QUERY_KEY = QUERY_USERTABLE +",%s";
    public static final String UPDATE_QUERY_USERTABLE = "U_USERTABLE";
    public static final String UPDATE_QUERY_KEY = UPDATE_QUERY_USERTABLE +",%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s";
    public static final String INSERT_QUERY_USERTABLE = "I_USERTABLE";
    public static final String INSERT_QUERY_KEY = INSERT_QUERY_USERTABLE +",%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s";
    public static final String DELETE_QUERY_USERTABLE = "D_USERTABLE";
    public static final String DELETE_QUERY_KEY = DELETE_QUERY_USERTABLE + ",%s";
    //Memcached
    public static final String MEM_UPDATE_USERTABLE = "U_MEM_USERTABLE";
    public static final String MEM_UPDATE_USERTABLE_KEY = MEM_UPDATE_USERTABLE + ",%s";
    
    //Writeback
    public static final String WB_UPDATE_USERTABLE = "U_WB_USERTABLE";
    public static final String WB_UPDATE_USERTABLE_KEY = WB_UPDATE_USERTABLE + ",%s";
    /**
     * How big should a commit batch be when loading
     */
    public static final int COMMIT_BATCH_SIZE = 100;
    
    /**
     * How many records will each thread load.
     */
    public static final int THREAD_BATCH_SIZE = 50000;

    public static final int MAX_SCAN = 1000;

}
