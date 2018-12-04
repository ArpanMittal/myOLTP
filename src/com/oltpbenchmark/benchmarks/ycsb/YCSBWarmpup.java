package com.oltpbenchmark.benchmarks.ycsb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.meetup.memcached.SockIOPool;
import com.oltpbenchmark.benchmarks.Config;
import com.oltpbenchmark.benchmarks.smallbank.SmallBankBenchmark;
import com.oltpbenchmark.benchmarks.smallbank.SmallBankCacheStore;
import com.oltpbenchmark.benchmarks.smallbank.SmallBankWorker;
import com.oltpbenchmark.benchmarks.smallbank.SmallBankWriteBack;
import com.oltpbenchmark.benchmarks.smallbank.procedures.Balance;
import com.oltpbenchmark.benchmarks.smallbank.procedures.GetAccount;
import com.oltpbenchmark.benchmarks.ycsb.procedures.ReadRecord;
import com.usc.dblab.cafe.CachePolicy;
import com.usc.dblab.cafe.CacheStore;
import com.usc.dblab.cafe.NgCache;
import com.usc.dblab.cafe.Stats;
import com.usc.dblab.cafe.WriteBack;

public class YCSBWarmpup {

    
    static SockIOPool cacheConnectionPool;
    

    static Random rand = new Random();
    static final int DB_SIZE = 800000;
    
    public static void main(String[] args) {    
//        String dbip = "168.62.24.93";
        String[] caches = null;
//        String[] caches = {"168.62.24.93:11211"};
        //caches = "168.62.24.93:11211";
        if (args.length >=2)
            caches = args[1].split(",");
        String dbip = args[2];
		String dbname = args[3];
		String dbpass = args[4];
        if (caches != null) {
            cacheConnectionPool = SockIOPool.getInstance(Config.CACHE_POOL_NAME);
            cacheConnectionPool.setServers(caches);
            cacheConnectionPool.setFailover(true);
            cacheConnectionPool.setInitConn(10);
            cacheConnectionPool.setMinConn(5);
            cacheConnectionPool.setMaxConn(200);
            cacheConnectionPool.setNagle(false);
            cacheConnectionPool.setSocketTO(0);
            cacheConnectionPool.setAliveCheck(true);
            cacheConnectionPool.initialize();
            System.out.println("Cache servers: "+Arrays.toString(caches));
        } else {
            System.out.println("No cache is provided.");
        }
        
        int nthreads = 10;
        int perThread = DB_SIZE / nthreads;
        WarmupThread[] threads = new WarmupThread[nthreads];
        for (int i = 0; i < nthreads; ++i) {
            int st = i*perThread;
            int end = (i+1)*perThread;
            threads[i] = new WarmupThread(st, end, dbip, caches);
            threads[i].start();
        }
        
        for (int i = 0; i < nthreads; ++i) {
            try {
                threads[i].join();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
}
    class WarmupThread extends Thread {
        private Connection conn = null;
        private NgCache cache = null;
        int start, end;
        
//        Balance procBalance = new Balance();
//        GetAccount procGetAcct = new GetAccount();
        ReadRecord readRecord = new ReadRecord();

        
        public WarmupThread(int start, int end, String dbip, String[] caches) {
            try {
//                conn = DriverManager.getConnection(
//                        "jdbc:mysql://168.62.24.93:3306/smallbank?serverTimezone=UTC&amp;useSSL=false&amp;rewriteBatchedStatements=true", 
//                        "user", "123456");
                conn = DriverManager.getConnection(
    					"jdbc:mysql://168.62.24.93:3306/ycsb?serverTimezone=UTC&useSSL=false","user" , "123456");
                conn.setAutoCommit(false);
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            
            if (caches != null) {
                CacheStore cacheStore = new YCSBCacheStore(conn);
                WriteBack cacheBack = new YCSBWriteBack(conn);
                
                cache = new NgCache(cacheStore, cacheBack, 
                        Config.CACHE_POOL_NAME, CachePolicy.WRITE_THROUGH, 0, Stats.getStatsInstance(0),"jdbc:mysql://168.62.24.93:3306/ycsb?serverTimezone=UTC", 
                        "user", "123456", false, 0, 0, 1);
            }
            
            this.start = start;
            this.end = end;
        }
        
        @Override
        public void run() {
            Map<String, Object> tres = new HashMap<>();
            for (int i = start; i < end; i++) {
                if (i % 10000 == 0) {
                    System.out.println("Warmup "+i+"...");
                }
                
                try {
                    if (cache == null) {
                    	readRecord.run(conn, i, new String[YCSBConstants.NUM_FIELDS]);
                    } else {
                    	readRecord.run(conn, i+"", cache, new String[YCSBConstants.NUM_FIELDS]);
                    }
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                try {
                    conn.commit();
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                tres.clear();
            }        
        }
    }
        


