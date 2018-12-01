package com.oltpbenchmark.benchmarks.smallbank.procedures;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Random;

import com.oltpbenchmark.api.Procedure;
import com.usc.dblab.cafe.NgCache;

public abstract class SmallBankProcedure extends Procedure  {
public static PrintWriter out = null;
    
    static {
        if (com.oltpbenchmark.benchmarks.Config.DEBUG) {
            try {
                out = new PrintWriter("/home/arpan/verify.txt");
            } catch (FileNotFoundException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
    
    protected void sleepRetry() {
        try {
            Thread.sleep(5);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public abstract ResultSet run(
            Connection conn,
            Random gen,
            Map<String, Object> tres) throws SQLException;
    
    public abstract ResultSet run(
            Connection conn, 
            Random gen,
            NgCache cafe, Map<String, Object> tres) throws SQLException;
}
