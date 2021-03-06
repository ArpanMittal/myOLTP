/***************************************************************************
 *  Copyright (C) 2013 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
package com.oltpbenchmark.benchmarks.smallbank.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import com.meetup.memcached.COException;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.Config;
import com.oltpbenchmark.benchmarks.smallbank.SmallBankConstants;
import com.oltpbenchmark.benchmarks.smallbank.SmallBankWorker;
import com.oltpbenchmark.benchmarks.smallbank.results.AccountResult;
import com.usc.dblab.cafe.NgCache;

/**
 * DepositChecking Procedure
 * Original version by Mohammad Alomari and Michael Cahill
 * @author pavlo
 */
public class DepositChecking extends Procedure {
    
    public final SQLStmt GetAccount = new SQLStmt(
        "SELECT * FROM " + SmallBankConstants.TABLENAME_ACCOUNTS +
        " WHERE name = ?"
    );
    
    public final SQLStmt UpdateCheckingBalance = new SQLStmt(
        "UPDATE " + SmallBankConstants.TABLENAME_CHECKING + 
        "   SET bal = bal + ? " +
        " WHERE custid = ?"
    );
    
    public void run(Connection conn, String custName, double amount, Map<String, Object> tres) throws SQLException {
        // First convert the custName to the custId
        PreparedStatement stmt0 = this.getPreparedStatement(conn, GetAccount, custName);
        ResultSet r0 = stmt0.executeQuery();
        if (r0.next() == false) {
            String msg = "Invalid account '" + custName + "'";
            throw new UserAbortException(msg);
        }
        long custId = r0.getLong(1);

        // Then update their checking balance
        PreparedStatement stmt1 = this.getPreparedStatement(conn, UpdateCheckingBalance, amount, custId);
        int status = stmt1.executeUpdate();
        assert(status == 1) :
            String.format("Failed to update %s for customer #%d [amount=%.2f]",
                          SmallBankConstants.TABLENAME_CHECKING, custId, amount);
        
        generateLog(tres, custId, amount);
        
        return;
    }

    public void run(Connection conn, String custName, double amount, NgCache cafe, Map<String, Object> tres) throws SQLException {
        int retry = 0;
    	while (true) {
    		try {
    			cafe.startSession("DepositChecking");
    			
    			String getAccount = String.format(SmallBankConstants.QUERY_ACCOUNT, custName);
    			AccountResult actRes = (AccountResult) cafe.readStatement(getAccount);
    			long custId = actRes.getCustId();
    			
    			String updateCheckingBalance = String.format(SmallBankConstants.UPDATE_INCR_CHECKING_BAL, custId, amount);
    			boolean success = cafe.writeStatement(updateCheckingBalance);
    	        assert(success) :
    	            String.format("Failed to update %s for customer #%d [amount=%.2f]",
    	                          SmallBankConstants.TABLENAME_CHECKING, custId, amount);			
    			
//    			conn.commit();
//    			cafe.commitSession();
    			
    			if (cafe.validateSession()) {
                    conn.commit();
                    cafe.commitSession();
                    break;
                } else {
                    conn.rollback();
                    cafe.abortSession();
                }
    			
    			generateLog(tres, custId, amount);
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
    		retry++;
    		if(retry>10)
    			break;
        }
    	cafe.getStats().incr("retry"+retry);
	}

    private void generateLog(Map<String, Object> tres, long custId,
            double amount) {
        if (Config.ENABLE_LOGGING && tres != null) {
            tres.put(SmallBankConstants.CUSTID, custId);
            tres.put(SmallBankConstants.AMOUNT, amount);
            if (Config.DEBUG) {
                System.out.println(this.getClass().getSimpleName() + ": "+new PrettyPrintingMap<String, Object>(tres));
            }
        }
    }
}