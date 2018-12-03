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
import com.oltpbenchmark.benchmarks.tpcc.TPCCConfig;
import com.oltpbenchmark.benchmarks.ycsb.YCSBConstants;
import com.usc.dblab.cafe.NgCache;

public class DeleteRecord extends Procedure{
    public final SQLStmt deleteStmt = new SQLStmt(
        "DELETE FROM USERTABLE where YCSB_KEY=?"
    );
    
	//FIXME: The value in ysqb is a byteiterator
    public int run(Connection conn, int keyname) throws SQLException {
        PreparedStatement stmt = this.getPreparedStatement(conn, deleteStmt);
        stmt.setInt(1, keyname);          
        return stmt.executeUpdate();
    }
    
    public void run(Connection conn,NgCache cafe, String keyname) throws SQLException {
//        PreparedStatement stmt = this.getPreparedStatement(conn, deleteStmt);
//        stmt.setInt(1, keyname);          
//        return stmt.executeUpdate();
        
//        while (true) {
            try {
            	cafe.startSession("DeleteRecord");
            	String deleteOrder = String.format(YCSBConstants.DELETE_QUERY_KEY, keyname);
            	boolean success = cafe.writeStatement(deleteOrder);
                if (!success) {
                   
                    throw new UserAbortException(" delete failed (not running with SERIALIZABLE isolation?)"+keyname);
                }
//                conn.commit();
//                cafe.commitSession();
                if (cafe.validateSession()) {
                    conn.commit();
                    cafe.commitSession();
                } else {
                    conn.rollback();
                    cafe.abortSession();
                }
            }catch (Exception e) {
                //                e.printStackTrace(System.out);
                try {
                    cafe.abortSession();
                } catch (Exception e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
                // throw new UserAbortException("Some error happens. "+ e.getMessage());

                if (e instanceof COException) {
//                    cafe.getStats().incr(((COException) e).getKey());
                }

                try {
                    conn.rollback();
                } catch (SQLException e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
        }
    }
//   }
}
