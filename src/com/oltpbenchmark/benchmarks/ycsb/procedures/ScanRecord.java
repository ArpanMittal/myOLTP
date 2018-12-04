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

package com.oltpbenchmark.benchmarks.ycsb.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.benchmarks.ycsb.YCSBConstants;
import com.oltpbenchmark.benchmarks.ycsb.results.UserResult;
import com.usc.dblab.cafe.NgCache;

public class ScanRecord extends Procedure{
    public final SQLStmt scanStmt = new SQLStmt(
        "SELECT * FROM USERTABLE WHERE YCSB_KEY>? AND YCSB_KEY<?"
    );
    
	//FIXME: The value in ysqb is a byteiterator
    public void run(Connection conn, int start, int count, List<String[]> results) throws SQLException {
        PreparedStatement stmt = this.getPreparedStatement(conn, scanStmt);
        stmt.setInt(1, start); 
        stmt.setInt(2, start+count); 
        ResultSet r=stmt.executeQuery();
        while(r.next()) {
            String data[] = new String[YCSBConstants.NUM_FIELDS];
        	for(int i = 0; i < data.length; i++)
        		data[i] = r.getString(i+1);
        	results.add(data);
        }
        r.close();
    }
    
    public void run(Connection conn, int start,NgCache cafe, int count, List<String[]> results) throws SQLException {
    	for(int i=start;i<start+count;i++) {
    		try {
    			cafe.startSession("Read");
    			String getUser = String.format(YCSBConstants.QUERY_KEY, i);
    			UserResult user_result = (UserResult) cafe.readStatement(getUser);
    			String data[] = new String[YCSBConstants.NUM_FIELDS];
    			results.add(user_result.getResult(data));
//            	for(int j = 1; j <= YCSBConstants.NUM_FIELDS; j++)
//            		data[j] = user_result.
//            	results.add(data);
    			System.out.println(user_result.getField_01()+","+user_result.getField_02()+","+user_result.getField_03()+","+user_result.getField_04()+","+user_result.getField_09());
    			conn.commit();
    			
    			try {
    				cafe.commitSession();
    			} catch (Exception e) {
    				// TODO Auto-generated catch block
    				e.printStackTrace();
    			}	
    			
        	}catch (Exception e) {
//    		    e.printStackTrace(System.out);
    			conn.rollback();
    			try {
    				cafe.abortSession();
    			} catch (Exception e1) {
    				// TODO Auto-generated catch block
    				e1.printStackTrace();
    			}
    			throw new UserAbortException("Some error happens. "+ e.getMessage());
            
        	}
        }
    	
    }
    
}
