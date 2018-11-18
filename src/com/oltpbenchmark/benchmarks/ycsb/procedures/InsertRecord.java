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
import java.sql.SQLException;

import com.meetup.memcached.COException;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.benchmarks.ycsb.YCSBConstants;
import com.usc.dblab.cafe.NgCache;

public class InsertRecord extends Procedure {
    public final SQLStmt insertStmt = new SQLStmt(
        "INSERT INTO USERTABLE VALUES (?,?,?,?,?,?,?,?,?,?,?)"
    );

    // FIXME: The value in ysqb is a byteiterator
    public int run(Connection conn, int keyname, String vals[]) throws SQLException {
        PreparedStatement stmt = this.getPreparedStatement(conn, this.insertStmt);
        stmt.setInt(1, keyname);
        for (int i = 0; i < vals.length; i++) {
            stmt.setString(i + 2, vals[i]);
        }
        return stmt.executeUpdate();
    }
    
    
    public void run(Connection conn, String keyname, NgCache cafe, String value[]) throws SQLException {
    	while (true) {
    		try {
    			cafe.startSession("InsertRecord");
    			
    			String insertRecord= String.format(YCSBConstants.INSERT_QUERY_KEY,keyname,value[0],value[1],value[2],value[3],value[4],value[5],value[6],value[7],value[8],value[9]);
    			boolean success = cafe.writeStatement(insertRecord);
    	        assert(success) :
    	            String.format("Failed to insert %s for customer #%s", YCSBConstants.INSERT_QUERY_USERTABLE, keyname);			
    			
    			conn.commit();
    			cafe.commitSession();
    			
    			
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
