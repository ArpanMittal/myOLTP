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

import com.meetup.memcached.COException;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.benchmarks.ycsb.YCSBConstants;
import com.oltpbenchmark.benchmarks.ycsb.results.UserResult;
import com.usc.dblab.cafe.NgCache;

public class ReadModifyWriteRecord extends Procedure {
    public final SQLStmt selectStmt = new SQLStmt(
        "SELECT * FROM USERTABLE where YCSB_KEY=? FOR UPDATE"
    );
    public final SQLStmt updateAllStmt = new SQLStmt(
        "UPDATE USERTABLE SET FIELD1=?,FIELD2=?,FIELD3=?,FIELD4=?,FIELD5=?," +
        "FIELD6=?,FIELD7=?,FIELD8=?,FIELD9=?,FIELD10=? WHERE YCSB_KEY=?"
    );
	//FIXME: The value in ysqb is a byteiterator
    public void run(Connection conn, int keyname, String fields[], String results[]) throws SQLException {
        
        // Fetch it!
        PreparedStatement stmt = this.getPreparedStatement(conn, selectStmt);
        stmt.setInt(1, keyname);          
        ResultSet r = stmt.executeQuery();
        while (r.next()) {
        	for (int i = 0; i < YCSBConstants.NUM_FIELDS; i++)
        	    results[i] = r.getString(i+1);
        }
        r.close();
        
        // Update that mofo
        stmt = this.getPreparedStatement(conn, updateAllStmt);
        stmt.setInt(11, keyname);
        
        for (int i = 0; i < fields.length; i++) {
        	stmt.setString(i+1, fields[i]);
        }
        stmt.executeUpdate();
    }
    
    public void run(Connection conn, String keyName, NgCache cafe, String value[], String results[]) throws SQLException {
        //TODO Complete this functionality
        // Fetch it!
//        PreparedStatement stmt = this.getPreparedStatement(conn, selectStmt);
//        stmt.setInt(1, keyname);          
//        ResultSet r = stmt.executeQuery();
//        while (r.next()) {
//        	for (int i = 0; i < YCSBConstants.NUM_FIELDS; i++)
//        	    results[i] = r.getString(i+1);
//        }
//        r.close();
//        
//        // Update that mofo
//        stmt = this.getPreparedStatement(conn, updateAllStmt);
//        stmt.setInt(11, keyname);
//        
//        for (int i = 0; i < fields.length; i++) {
//        	stmt.setString(i+1, fields[i]);
//        }
//        stmt.executeUpdate();
    	while (true) {
    		try {
    			cafe.startSession("ReadModifyRecord");
    			
    			String getUser = String.format(YCSBConstants.QUERY_KEY, keyName);
    			UserResult user_result = (UserResult) cafe.readStatement(getUser);
    			System.out.println(user_result.getField_01());
    			
    			
    			
    			String updateQuery = String.format(YCSBConstants.UPDATE_QUERY_KEY,keyName,value[0],value[1],value[2],value[3],value[4],value[5],value[6],value[7],value[8],value[9]);
    			boolean success = cafe.writeStatement(updateQuery);
    	        assert(success) :
    	            String.format("Failed to update %s for customer #%s", YCSBConstants.UPDATE_QUERY_USERTABLE, keyName);			
    			
    	        if (cafe.validateSession()) {
                    conn.commit();
                    cafe.commitSession();
                } else {
                    conn.rollback();
                    cafe.abortSession();
                }
    			
    			break;
    		} catch (Exception e) {
    //		    e.printStackTrace(System.out);
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

}
