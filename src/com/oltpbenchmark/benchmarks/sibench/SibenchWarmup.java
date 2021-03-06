package com.oltpbenchmark.benchmarks.sibench;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.meetup.memcached.SockIOPool;
import com.oltpbenchmark.benchmarks.Config;
import com.oltpbenchmark.benchmarks.sibench.procedures.UpdateRecord;
import com.oltpbenchmark.benchmarks.smallbank.SmallBankCacheStore;
import com.oltpbenchmark.benchmarks.smallbank.SmallBankWriteBack;
import com.oltpbenchmark.benchmarks.voter.PhoneCallGenerator;
import com.oltpbenchmark.benchmarks.voter.VoterBenchmark;
import com.oltpbenchmark.benchmarks.voter.VoterConstants;
import com.oltpbenchmark.benchmarks.voter.VoterWorker;
import com.oltpbenchmark.benchmarks.voter.PhoneCallGenerator.PhoneCall;
import com.oltpbenchmark.benchmarks.voter.procedures.Vote;
import com.usc.dblab.cafe.CachePolicy;
import com.usc.dblab.cafe.CacheStore;
import com.usc.dblab.cafe.NgCache;
import com.usc.dblab.cafe.Stats;
import com.usc.dblab.cafe.WriteBack;


public class SibenchWarmup {
static SockIOPool cacheConnectionPool;
    
    static VoterBenchmark bench;
    static VoterWorker worker;
    static Random rand = new Random();
    static final int DB_SIZE = SIConstants.RECORD_COUNT;
    
    public static void main(String[] args) {    
        String[] caches = null;
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
        
        int nthreads = 100;
        int perThread = DB_SIZE / nthreads;
        WarmupThread[] threads = new WarmupThread[nthreads];
        for (int i = 0; i < nthreads; ++i) {
            int st = i*perThread;
            int end = (i+1)*perThread;
            threads[i] = new WarmupThread(st, end, dbip, caches,dbname, dbpass);
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
    
    UpdateRecord updateRecor = new UpdateRecord();
    
    private final PhoneCallGenerator switchboard = new PhoneCallGenerator(0,6);

    
    public WarmupThread(int start, int end, String dbip, String[] caches, String dbname, String dbpass) {
        try {
            conn = DriverManager.getConnection(
                    dbip, 
                    dbname, dbpass);
           
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        if (caches != null) {
            CacheStore cacheStore = new SmallBankCacheStore(conn);
            WriteBack cacheBack = new SmallBankWriteBack(conn);
            
            cache = new NgCache(cacheStore, cacheBack, 
                    Config.CACHE_POOL_NAME, CachePolicy.WRITE_THROUGH, 0, Stats.getStatsInstance(0),dbip, dbname, dbpass, false, 0, 0, 1);
        }
        
        this.start = start;
        this.end = end;
    }
    
    @Override
    public void run() {
        Map<String, Object> tres = new HashMap<>();
        for (int i = start; i < end; i++) {
        	
            
            try {
            	System.out.println("warmup="+i);
				updateRecor.warmupRun(conn, i, cache);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
                
        }        
    }
}
