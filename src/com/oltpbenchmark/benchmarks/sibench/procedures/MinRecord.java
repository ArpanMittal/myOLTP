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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.benchmarks.sibench.MinResult;
import com.oltpbenchmark.benchmarks.sibench.SIConstants;
import com.oltpbenchmark.benchmarks.ycsb.YCSBConstants;
import com.oltpbenchmark.benchmarks.ycsb.results.UserResult;
import com.usc.dblab.cafe.NgCache;

public class MinRecord extends Procedure{
    public final SQLStmt minStmt = new SQLStmt(
        "SELECT id FROM SITEST ORDER BY value ASC LIMIT 1"
    );

    public int run(Connection conn) throws SQLException {
        PreparedStatement stmt = this.getPreparedStatement(conn, minStmt);
        ResultSet r=stmt.executeQuery();
        int minId = 0;
        while(r.next())
        {
        	minId = r.getInt(1);
        }
        r.close();
	conn.commit();
        return minId;
    }
    
    public MinResult run(Connection conn, NgCache cafe) throws SQLException {
    	try {
			cafe.startSession("MinRecord");
			String getUser = String.format(SIConstants.QUERY_MIN);
			MinResult min_result = (MinResult) cafe.readStatement(getUser);

			conn.commit();
			
			try {
				cafe.commitSession();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return min_result;
			
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
