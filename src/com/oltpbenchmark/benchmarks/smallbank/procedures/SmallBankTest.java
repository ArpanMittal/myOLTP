package com.oltpbenchmark.benchmarks.smallbank.procedures;

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
import com.usc.dblab.cafe.WriteBack;
import com.usc.dblab.cafe.CachePolicy;
import com.usc.dblab.cafe.CacheStore;
import com.usc.dblab.cafe.NgCache;
import com.usc.dblab.cafe.Stats;

public class SmallBankTest {
    static Balance procBalance = new Balance();
    static Amalgamate procAmalgamate = new Amalgamate();
    static DepositChecking procDepositChecking = new DepositChecking();
    static SendPayment procSendPayment = new SendPayment();
    static TransactSavings procTransactSavings = new TransactSavings();
    static WriteCheck procWriteCheck = new WriteCheck();
    static SockIOPool cacheConnectionPool;
    static Connection conn;
    static NgCache cache;
    
    static SmallBankBenchmark bench;
    static SmallBankWorker worker;
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
                    "jdbc:mysql://168.62.24.93:3306/smallbank?serverTimezone=UTC", 
                    "user", "123456");
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        CacheStore cacheStore = new SmallBankCacheStore(conn);
        WriteBack cacheBack = new SmallBankWriteBack(conn);
        
        cache = new NgCache(cacheStore, cacheBack, 
                Config.CACHE_POOL_NAME, CachePolicy.WRITE_BACK,1 , Stats.getStatsInstance(0), "jdbc:mysql://168.62.24.93:3306/smallbank?serverTimezone=UTC", 
                "user", "123456", false, 0, 0, 1); 
        
        System.out.println(getName(1));
        System.out.println(getName(322));
        System.out.println(getName(1123));
//        verifyCacheHit();
//        System.out.println("====== Verify DepositChecking");
        //verifyDepositChecking();
//        System.out.println("====== Verify WriteCheck");
//        verifyWriteCheck();
//        System.out.println("====== Verify TransactSavings");
//        verifyTransactSavings();
//        
//        System.out.println("====== Verify Amalgamate");
//        verifyAmalgamate(false);
//        System.out.println("-------------------------");
//        verifyAmalgamate(true);
//        
//        System.out.println("====== Verify Send Payment");
//        verifySendPayment(false);
//        System.out.println("-------------------------");
//        verifySendPayment(true);     
    }
    
    public static void verifyCacheHit() {
        try {
            for (long i = 10; i <= 20; i++) {
                procBalance.run(conn, getName(i), cache, null);
            }
            System.out.println(Stats.getAllStats().toString(2));
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    
    
    public static void verifyDepositChecking() {
        Map<String, Object> tres = new HashMap<String, Object>();
        long id = 4;
        String name = getName(id);
        try {
        	for(int i =1;i<=10; i++) {
            	System.out.println(i);
            	name =  getName(i);
            // on cache misses
	            procBalance.run(conn, name, cache, tres);
	            
	            tres.clear();
	            procDepositChecking.run(conn, name, .5, cache, tres);
	            
	            
	            tres.clear();
//	            System.out.println("after update");
	            procBalance.run(conn, name, cache, tres);
	            
	//            // on cache hits
	            tres.clear();
//	            System.out.println("after update");
//	            procDepositChecking.run(conn, name, .5, cache, tres);
//	//            
//	            tres.clear();
//	            procBalance.run(conn, name, cache, tres);
        	}
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    public static void verifyWriteCheck() {
        Map<String, Object> tres = new HashMap<String, Object>();
        long id = rand.nextInt(DB_SIZE);
        String name = getName(id);
        try {
            // on cache misses
            procBalance.run(conn, name, tres);
            
            tres.clear();
            procWriteCheck.run(conn, name, 0.25, cache, tres);
            
            tres.clear();
            procBalance.run(conn, name, cache, tres);
            
            // on cache hits
            tres.clear();
            procWriteCheck.run(conn, name, 0.77, cache, tres);
            
            tres.clear();
            procBalance.run(conn, name, cache, tres);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    public static void verifyTransactSavings() {
        Map<String, Object> tres = new HashMap<String, Object>();
        long id = rand.nextInt(DB_SIZE);
        String name = getName(id);
        try {
            // on cache misses
            procBalance.run(conn, name, tres);
            
            tres.clear();
            procTransactSavings.run(conn, name, 0.25, cache, tres);
            
            tres.clear();
            procBalance.run(conn, name, cache, tres);
            
            // on cache hits
            tres.clear();
            procTransactSavings.run(conn, name, 0.77, cache, tres);
            
            tres.clear();
            procBalance.run(conn, name, cache, tres);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    private static String getName(long id) {
        String name = id+"";
        int cnt = 0;
        while (id > 0) {
            id /= 10;
            cnt++;
        }
        for (int i = 0; i < 64-cnt; ++i) {
            name = "0"+name;
        }
        return name;
    }

    public static void verifyAmalgamate(boolean cacheHit) {
        Map<String, Object> tres = new HashMap<String, Object>();
        long custId0 = rand.nextInt(DB_SIZE);
        long custId1 = rand.nextInt(DB_SIZE);
        for(int i =10;i<=15; i++) {
        	System.out.println(i);
	        String name1 = getName(i);
	        String name2 = getName(i+5);
	        
	        try {
	            if (!cacheHit) {
	                procBalance.run(conn, name1, tres);            
	                tres.clear();
	                procBalance.run(conn, name2, tres);
	            } else {
	                procBalance.run(conn, name1, cache, tres);            
	                tres.clear();
	                procBalance.run(conn, name2, cache, tres);
	            }
	            
	            tres.clear();
	            procAmalgamate.run(conn, i, i+5, cache, tres);
	            
	            tres.clear();
	            System.out.println("After Amalgamate");
	            procBalance.run(conn, name1, cache, tres);
	            tres.clear();
	            procBalance.run(conn, name2, cache, tres);
	        } catch (SQLException e) {
	            // TODO Auto-generated catch block
	            e.printStackTrace();
	        }
        }
    }
    
    public static void verifySendPayment(boolean cacheHit) {
        Map<String, Object> tres = new HashMap<String, Object>();
        long custId0 = rand.nextInt(DB_SIZE);
        long custId1 = rand.nextInt(DB_SIZE);
        String name1 = getName(custId0);
        String name2 = getName(custId1);
        
        try {
            if (!cacheHit) {
                procBalance.run(conn, name1, tres);            
                tres.clear();
                procBalance.run(conn, name2, tres);
            } else {
                procBalance.run(conn, name1, cache, tres);            
                tres.clear();
                procBalance.run(conn, name2, cache, tres);
            }
            
            tres.clear();
            procSendPayment.run(conn, custId0, custId1, 0.7, cache, tres);
            
            tres.clear();
            procBalance.run(conn, name1, cache, tres);
            tres.clear();
            procBalance.run(conn, name2, cache, tres);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
