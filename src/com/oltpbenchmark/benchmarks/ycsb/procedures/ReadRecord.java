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

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.benchmarks.smallbank.SmallBankConstants;
import com.oltpbenchmark.benchmarks.smallbank.results.AccountResult;
import com.oltpbenchmark.benchmarks.ycsb.YCSBConstants;
import com.oltpbenchmark.benchmarks.ycsb.results.UserResult;
import com.usc.dblab.cafe.NgCache;

public class ReadRecord extends Procedure{
    public final SQLStmt readStmt = new SQLStmt(
        "SELECT * FROM USERTABLE WHERE YCSB_KEY=?"
    );
    
	//FIXME: The value in ysqb is a byteiterator
    public void run(Connection conn, int keyname, String results[]) throws SQLException {
        PreparedStatement stmt = this.getPreparedStatement(conn, readStmt);
        stmt.setInt(1, keyname);          
        ResultSet r = stmt.executeQuery();
        while(r.next()) {
            for (int i = 0; i < YCSBConstants.NUM_FIELDS; i++)
                results[i] = r.getString(i+1);
        } // WHILE
        r.close();
    }
    
    public void run(Connection conn, String keyname, NgCache cafe, String results[]) throws SQLException {
    	int retry = 0;
    	while (true) {
    	try {
			cafe.startSession("Read");
			String getUser = String.format(YCSBConstants.QUERY_KEY, keyname);
			UserResult user_result = (UserResult) cafe.readStatement(getUser);
			//results[0] = keyname;
			results[0] = user_result.getField_01();
			results[1] = user_result.getField_02();
			results[2] = user_result.getField_03();
			results[3] = user_result.getField_04();
			results[4] = user_result.getField_05();
			results[5] = user_result.getField_06();
			results[6] = user_result.getField_07();
			results[7] = user_result.getField_08();
			results[8] = user_result.getField_09();
			results[9] = user_result.getField_10();
			//System.out.println(results[9]);
			conn.commit();
			
			try {
				cafe.commitSession();
				break;
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
			
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
    	retry++;
		if(retry>10)
			break;
    	}
    	cafe.getStats().incr("retry"+retry);
    }

	public void runWrite(Connection conn, int keyname, String[] results) throws SQLException {
		// TODO Auto-generated method stub
		PreparedStatement stmt = this.getPreparedStatement(conn, readStmt);
        stmt.setInt(1, keyname);          
        ResultSet r = stmt.executeQuery();
        while(r.next()) {
            for (int i = 0; i <= YCSBConstants.NUM_FIELDS; i++)
                results[i] = r.getString(i+1);
        } // WHILE
        r.close();
	}

}
