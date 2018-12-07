package com.oltpbenchmark.benchmarks.sibench;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Random;

import com.meetup.memcached.SockIOPool;
import com.oltpbenchmark.benchmarks.Config;
import com.oltpbenchmark.benchmarks.sibench.procedures.MinRecord;
import com.oltpbenchmark.benchmarks.sibench.procedures.UpdateRecord;
import com.oltpbenchmark.benchmarks.voter.VoterCacheStore;
import com.oltpbenchmark.benchmarks.voter.VoterWriteBack;
import com.oltpbenchmark.benchmarks.voter.procedures.Vote;
import com.usc.dblab.cafe.CachePolicy;
import com.usc.dblab.cafe.CacheStore;
import com.usc.dblab.cafe.NgCache;
import com.usc.dblab.cafe.Stats;
import com.usc.dblab.cafe.WriteBack;

public class SITest {
	static SockIOPool cacheConnectionPool;
    static Connection conn;
    static NgCache cache;
    static Random rand = new Random();
    static final int DB_SIZE = 20000;
    static MinRecord minRecord = new MinRecord();
    static UpdateRecord updateRecord = new UpdateRecord();
	public static void main(String[] args) {
		 Config.DEBUG = true;
	        
	        cacheConnectionPool = SockIOPool.getInstance(Config.CACHE_POOL_NAME);
	        cacheConnectionPool.setServers(new String[] { "168.62.24.93:11211" });
	        cacheConnectionPool.setFailover(true);
	        cacheConnectionPool.setInitConn(10);
	        cacheConnectionPool.setMinConn(5);
	        cacheConnectionPool.setMaxConn(200);
	        cacheConnectionPool.setNagle(false);
	        cacheConnectionPool.setSocketTO(0);
	        cacheConnectionPool.setAliveCheck(true);
	        cacheConnectionPool.initialize();
	        System.out.println("Cache servers: "+Arrays.toString(Config.cacheServers));
	        
	        try {
	            conn = DriverManager.getConnection(
	                    "jdbc:mysql://168.62.24.93:3306/sibench?serverTimezone=UTC", 
	                    "user", "123456");
	            conn.setAutoCommit(false);
	        } catch (SQLException e) {
	            // TODO Auto-generated catch block
	            e.printStackTrace();
	        }
	        
	        CacheStore cacheStore = new SICacheStore(conn);
	        WriteBack cacheBack = new SIWriteBack(conn);
	        
	        cache = new NgCache(cacheStore, cacheBack, 
	                Config.CACHE_POOL_NAME, CachePolicy.WRITE_BACK, 1, Stats.getStatsInstance(0), "jdbc:mysql://168.62.24.93:3306/sibench?serverTimezone=UTC", 
	                "user", "123456", false, 0, 0, 1); 
	        
	        verifyCacheHit();
	}
	private static void verifyCacheHit() {
		// TODO Auto-generated method stub
		 try {
//			 for(int i=20881;i<20882;i++) {
//				 int min_id = (minRecord.run(conn, cache)).getId();
				 updateRecord.run(conn, 1,cache);
//			 }
			 System.out.println(Stats.getAllStats().toString(2));
		 }catch (SQLException e) {
	            // TODO Auto-generated catch block
	            e.printStackTrace();
	        }
		
	}
}
