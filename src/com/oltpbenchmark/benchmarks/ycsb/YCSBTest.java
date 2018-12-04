package com.oltpbenchmark.benchmarks.ycsb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

import com.meetup.memcached.SockIOPool;
import com.oltpbenchmark.benchmarks.Config;
import com.oltpbenchmark.benchmarks.smallbank.SmallBankCacheStore;
import com.oltpbenchmark.benchmarks.smallbank.SmallBankWriteBack;
import com.oltpbenchmark.benchmarks.smallbank.procedures.Balance;
import com.oltpbenchmark.benchmarks.ycsb.procedures.DeleteRecord;
import com.oltpbenchmark.benchmarks.ycsb.procedures.InsertRecord;
import com.oltpbenchmark.benchmarks.ycsb.procedures.ReadModifyWriteRecord;
import com.oltpbenchmark.benchmarks.ycsb.procedures.ReadRecord;
import com.oltpbenchmark.benchmarks.ycsb.procedures.ScanRecord;
import com.oltpbenchmark.benchmarks.ycsb.procedures.UpdateRecord;
import com.usc.dblab.cafe.CachePolicy;
import com.usc.dblab.cafe.CacheStore;
import com.usc.dblab.cafe.NgCache;
import com.usc.dblab.cafe.Stats;
import com.usc.dblab.cafe.WriteBack;

public class YCSBTest {
	static ReadRecord readRecord = new ReadRecord();
	static UpdateRecord updateRecord = new UpdateRecord();
	static InsertRecord insertRecord = new InsertRecord();
	static DeleteRecord deleteRecord = new DeleteRecord();
	static ScanRecord scanRecord = new ScanRecord();
	static ReadModifyWriteRecord readModifyWriteRecord = new ReadModifyWriteRecord();
	static SockIOPool cacheConnectionPool;
    static Connection conn;
    static NgCache cache;
    static Random rand = new Random();
    static final int DB_SIZE = 20000;
    
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
                    "jdbc:mysql://168.62.24.93:3306/ycsb?serverTimezone=UTC", 
                    "user", "123456");
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        CacheStore cacheStore = new YCSBCacheStore(conn);
        WriteBack cacheBack = new YCSBWriteBack(conn);
        
        cache = new NgCache(cacheStore, cacheBack, 
                Config.CACHE_POOL_NAME, CachePolicy.WRITE_BACK, 1, Stats.getStatsInstance(0), "jdbc:mysql://168.62.24.93:3306/ycsb?serverTimezone=UTC", 
                "user", "123456", false, 0, 0, 1); 
        
       // verifyCacheHit();
 
    }
	
	   public static void verifyCacheHit() {
	        try {
	        	String results[] = new String[20];
	        	ArrayList<String[]> result_scan = new ArrayList<String[]>();
	            for (int i = 998; i <999; i++) {
	            	//readRecord.run(conn, 2, results);
	                //readRecord.run(conn, "10", cache);
	                String[] val = {"99","22","23","24","25","26","27","28","29","21"};
	                String[] result = new String[10];
//	                readRecord.run(conn, "509", cache);
//	                readRecord.run(conn, i+"", cache);
//	                System.out.println(Stats.getAllStats().toString(2));
//	                System.out.println("After Update");
//	                updateRecord.run(conn,i+"", cache, val);
//	                System.out.println(Stats.getAllStats().toString(2));
	                scanRecord.run(conn, i, cache, 10, result_scan);
//	                
//	                readRecord.run(conn, "502", cache);
	                readModifyWriteRecord.run(conn,i+"",cache,val,result);
//	                insertRecord.run(conn,i+"",cache,val);
//	                deleteRecord.run(conn, cache, i+"");
	                //System.out.println("after rmw");
	                //readRecord.run(conn, i+"", cache);
	                //System.out.println(Stats.getAllStats().toString(2));
	                //readRecord.run(conn, "1", cache);
	                //deleteRecord.run(conn, cache, "520");
	                //readRecord.run(conn, "502", cache);
	            }
	            System.out.println(Stats.getAllStats().toString(2));
	        } catch (SQLException e) {
	            // TODO Auto-generated catch block
	            e.printStackTrace();
	        }
	    }
    
}
