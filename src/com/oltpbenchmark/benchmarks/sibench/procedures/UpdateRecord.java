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

package com.oltpbenchmark.benchmarks.sibench.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.meetup.memcached.COException;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.benchmarks.sibench.MinResult;
import com.oltpbenchmark.benchmarks.sibench.SIConstants;
import com.oltpbenchmark.benchmarks.ycsb.YCSBConstants;
import com.usc.dblab.cafe.NgCache;

public class UpdateRecord extends Procedure{
    public final SQLStmt updateStmt = new SQLStmt(
        "UPDATE SITEST SET value = value + 1 WHERE id = ?"
    );
    static MinRecord minRecord = new MinRecord();

    public void run(Connection conn, int id) throws SQLException {
        PreparedStatement stmt = this.getPreparedStatement(conn, updateStmt);
        stmt.setInt(1, id);
        stmt.executeUpdate();
	conn.commit();
    }
    
    public void run(Connection conn, int id, NgCache cafe) throws SQLException {
    	while (true) {
    		try {
    			cafe.startSession("SIUpdateRecord");
    			
    			
    			
    			String getUser = String.format(SIConstants.QUERY_MIN);
    			MinResult min_result = (MinResult) cafe.readStatement(getUser);
    	        
    	        String userValue = String.format(SIConstants.UPDATE_VALUE_KEY,id);
    	        MinResult user_result = (MinResult)cafe.readStatement(userValue);
   	        
    			String updateValue = String.format(SIConstants.UPDATE_VALUE_INCR_KEY,id,user_result.getValue());
    			boolean success = cafe.writeStatement(updateValue);
    			 assert(success) :
     	            String.format("Failed to update %s for customer #%s", SIConstants.UPDATE_VALUE,id);	
    			//update minresut
    			if(user_result.getValue()+1<min_result.getValue()) {
    				String updateMinKey = String.format(SIConstants.QUERY_UP_MEM_MIN_KEY,id,user_result.getValue()+1);
    	     
    				success = cafe.writeStatement(updateMinKey);
    				assert(success) :
         	            String.format("Failed to minupdate %s for customer #%s", SIConstants.UPDATE_VALUE,id);	
    			}
    				
    			
    	       
    	        
    	        if (cafe.validateSession()) {
                    conn.commit();
                    cafe.commitSession();
                } else {
                    conn.rollback();
                    cafe.abortSession();
                }
    			
    			
    			break;
    		} catch (Exception e) {
    			conn.rollback();
    			try {
    				cafe.abortSession();
    			} catch (Exception e1) {
    				// TODO Auto-generated catch block
    				e1.printStackTrace();
    			}
    			
    			if (!(e instanceof COException))
    			    throw new UserAbortException("Some error happens. "+ e.getMessage());
    		}
        }
    }
    
    public void warmupRun(Connection conn, int id, NgCache cafe) throws SQLException {
    	try {
    		  String userValue = String.format(SIConstants.UPDATE_VALUE_KEY,id);
  	        MinResult user_result = (MinResult)cafe.readStatement(userValue);
    	}catch (Exception e) {
//		    e.printStackTrace(System.out);
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
