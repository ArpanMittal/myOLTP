package com.oltpbenchmark.benchmarks.smallbank.procedures;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.meetup.memcached.SockIOPool;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.benchmarks.Config;
import com.oltpbenchmark.benchmarks.smallbank.SmallBankCacheStore;
import com.oltpbenchmark.benchmarks.smallbank.SmallBankConstants;
import com.oltpbenchmark.benchmarks.smallbank.SmallBankWriteBack;
import com.oltpbenchmark.benchmarks.smallbank.results.CheckingResult;
import com.oltpbenchmark.benchmarks.smallbank.results.SavingsResult;
import com.oltpbenchmark.benchmarks.tpcc.TPCCCacheStore;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConfig;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWriteBack;
import com.oltpbenchmark.benchmarks.tpcc.procedures.ReadOnly;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryGetItemResult;
import com.usc.dblab.cafe.CachePolicy;
import com.usc.dblab.cafe.CacheStore;
import com.usc.dblab.cafe.NgCache;
import com.usc.dblab.cafe.Stats;
import com.usc.dblab.cafe.WriteBack;

public class ReadOnly_SmallBank extends SmallBankProcedure {
	public static void main(String[] args) {
		Config.DEBUG = false;

		int scale = Integer.parseInt(args[0]);
		String[] cacheServers = args[1].split(",");
		String dbip = args[2];
		String dbname = args[3];
		String dbpass = args[4];
		boolean aDistPerThread = Boolean.parseBoolean(args[9]);

		System.out.println("Scale = " + scale);

		SockIOPool cacheConnectionPool = SockIOPool.getInstance(Config.CACHE_POOL_NAME);
		cacheConnectionPool.setServers(cacheServers);
		cacheConnectionPool.setFailover(true);
		cacheConnectionPool.setInitConn(10);
		cacheConnectionPool.setMinConn(5);
		cacheConnectionPool.setMaxConn(200);
		cacheConnectionPool.setNagle(false);
		cacheConnectionPool.setSocketTO(0);
		cacheConnectionPool.setAliveCheck(true);
		cacheConnectionPool.initialize();
		System.out.println("Cache servers: " + Arrays.toString(cacheServers));

		com.usc.dblab.cafe.Config.storeCommitedSessions = false;

		LoadThread[] threads = null;

		threads = new LoadThread[(0 - 0 + 1) * 1];
		for (int i = 0; i <= 0; i++) {
			for (int j = 1; j <= 1; j++) {
				threads[(i - 0) * 1 + (j - 1)] = new LoadThread(dbip, dbname, dbpass);
				threads[(i - 0) * 1 + (j - 1)].start();
			}
		}

		for (int i = 1; i <= threads.length; i++) {
			try {
				threads[i - 1].join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
//        
		System.out.println(Stats.getAllStats().toString(4));

		System.out.println("Loading completed.");
	}

	@Override
	public ResultSet run(Connection conn, Random gen, Map<String, Object> tres) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet run(Connection conn, Random gen, NgCache cafe, Map<String, Object> tres) throws SQLException {
		// TODO Auto-generated method stub
//		System.out.println("Cache QUERY_ACCOUNT done.");

		int start = 0; // numWarehouses *(w_id-1)+1;
		int end = SmallBankConstants.NUM_ACCOUNTS; // numWarehouses * w_id;
//		int main_end = 305000;
		for (int i_id = start; i_id <=end ; ++i_id) {
			try {

				
				System.out.println("account cached"+i_id);
				cafe.startSession(null);

				String getSendAcct = String.format(SmallBankConstants.QUERY_ACCOUNT_BY_CUSTID, i_id);
				cafe.readStatement(getSendAcct);

				// conn.commit();
//				try {
				cafe.commitSession();
//				} catch (Exception e) {
				// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
			} catch (Exception e) {
				e.printStackTrace(System.out);
				try {
					cafe.abortSession();
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
		}
		System.out.println("Cache QUERY_ACCOUNT done.");

		for (int i_id = start; i_id <= end; ++i_id) {
			try {

				System.out.println("checking cached"+i_id);
				cafe.startSession(null);

				String getCheckingBalance = String.format(SmallBankConstants.QUERY_CHECKING_BAL, i_id);
				CheckingResult checkingRes = (CheckingResult) cafe.readStatement(getCheckingBalance);

//					conn.commit();
					try {
				cafe.commitSession();
					} catch (Exception e) {
				// TODO Auto-generated catch block
						e.printStackTrace();
					}
			} catch (Exception e) {
				e.printStackTrace(System.out);
				try {
					cafe.abortSession();
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
		}
		System.out.println("Cache QUERY_CHECKING done.");

		for (int i_id = start; i_id <= end; ++i_id) {
			try {

					
				System.out.println("saving cached"+i_id);
				cafe.startSession(null);

				String getCheckingBalance = String.format(SmallBankConstants.QUERY_SAVINGS_BAL, i_id);
				SavingsResult checkingRes = (SavingsResult) cafe.readStatement(getCheckingBalance);

//					conn.commit();
//					try {
				cafe.commitSession();
//					} catch (Exception e) {
				// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
			} catch (Exception e) {
				e.printStackTrace(System.out);
				try {
					cafe.abortSession();
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
		}
		System.out.println("Cache QUERY_SAVING done.");
		return null;
	}
}

class LoadThread extends Thread {
//  static Random rand = new Random();
	int warehouseId;
	int scale;
	int distperwhse;
	int custperdist;
	boolean aDistPerThread = true;
	int districtId = 0;
	int threadId = 0;

	String dbip, dbname, dbpass;

	public LoadThread(String dbip, String dbname, String dbpass) {
//      this.scale = scale;
//      this.warehouseId = warehouseId;
		this.dbip = dbip;
		this.dbname = dbname;
		this.dbpass = dbpass;
//      this.distperwhse = distperwhse;
//      this.custperdist = custperdist;
//      this.aDistPerThread = aDistPerThread;
//      this.districtId = districtId;
//      this.threadId = threadId;
	}

	@Override
	public void run() {
//      if (aDistPerThread) {
//          System.out.println("Start loading for warehouse "+warehouseId+", district "+districtId);
//      } else {
//          System.out.println("Start loading for warehouse "+warehouseId);
//      }
		System.out.println("Start loading for Smallbank");
		Connection conn = null;
		try {
			if (!dbip.contains("jdbc")) {
				conn = DriverManager.getConnection(
						"jdbc:mysql://" + dbip + ":3306/smallbank?serverTimezone=UTC&useSSL=false", dbname, dbpass);
			} else {
				conn = DriverManager.getConnection(dbip, dbname, dbpass);
			}
			conn.setAutoCommit(true);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		CacheStore cacheStore = new SmallBankCacheStore(conn);
		WriteBack cacheBack = new SmallBankWriteBack(conn);

		NgCache cache = null;
		if (dbip.contains("jdbc")) {
			cache = new NgCache(cacheStore, cacheBack, Config.CACHE_POOL_NAME, CachePolicy.WRITE_THROUGH, 0,
					Stats.getStatsInstance(threadId), dbip, dbname, dbpass, false, 0, 0, 0);
		} else {
			cache = new NgCache(cacheStore, cacheBack, Config.CACHE_POOL_NAME, CachePolicy.WRITE_THROUGH, 0,
					Stats.getStatsInstance(threadId),
					"jdbc:mysql://" + dbip + ":3306/smallbank?serverTimezone=UTC&useSSL=false", dbname, dbpass, false,
					0, 0, 0);
		}

		ReadOnly_SmallBank ro = new ReadOnly_SmallBank();
		Map<String, Object> tres = new HashMap<>();
		try {
			ro.run(conn, null, cache, tres);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		cache.clean();
	}
}
