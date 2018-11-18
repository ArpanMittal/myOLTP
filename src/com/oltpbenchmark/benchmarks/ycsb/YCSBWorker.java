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

package com.oltpbenchmark.benchmarks.ycsb;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.Config;
import com.oltpbenchmark.benchmarks.smallbank.SmallBankCacheStore;
import com.oltpbenchmark.benchmarks.smallbank.SmallBankConstants;
import com.oltpbenchmark.benchmarks.smallbank.SmallBankWriteBack;
import com.oltpbenchmark.benchmarks.ycsb.procedures.DeleteRecord;
import com.oltpbenchmark.benchmarks.ycsb.procedures.InsertRecord;
import com.oltpbenchmark.benchmarks.ycsb.procedures.ReadModifyWriteRecord;
import com.oltpbenchmark.benchmarks.ycsb.procedures.ReadRecord;
import com.oltpbenchmark.benchmarks.ycsb.procedures.ScanRecord;
import com.oltpbenchmark.benchmarks.ycsb.procedures.UpdateRecord;
import com.oltpbenchmark.distributions.CounterGenerator;
import com.oltpbenchmark.distributions.ZipfianGenerator;
import com.oltpbenchmark.types.TransactionStatus;
import com.oltpbenchmark.util.TextGenerator;
import com.usc.dblab.cafe.CachePolicy;
import com.usc.dblab.cafe.CacheStore;
import com.usc.dblab.cafe.NgCache;
import com.usc.dblab.cafe.Stats;
import com.usc.dblab.cafe.WriteBack;

/**
 * YCSBWorker Implementation
 * I forget who really wrote this but I fixed it up in 2016...
 * @author pavlo
 *
 */
public class YCSBWorker extends Worker<YCSBBenchmark> {

    private ZipfianGenerator readRecord;
    private static CounterGenerator insertRecord;
    private ZipfianGenerator randScan;

    private final char data[] = new char[YCSBConstants.FIELD_SIZE];
    private final String params[] = new String[YCSBConstants.NUM_FIELDS]; 
    private final String results[] = new String[YCSBConstants.NUM_FIELDS];
    private int threadId;
    private final UpdateRecord procUpdateRecord;
    private final ScanRecord procScanRecord;
    private final ReadRecord procReadRecord;
    private final ReadModifyWriteRecord procReadModifyWriteRecord;
    private final InsertRecord procInsertRecord;
    private final DeleteRecord procDeleteRecord;
    public NgCache cafe = null;
    private CacheStore cacheStore;
    private WriteBack cacheBack;
    public HashMap<String, Object> transactionResults = null;
    public BufferedWriter updateLogAll = null;
    public BufferedWriter readLogAll = null;
    private StringBuilder readLog = null;
    private StringBuilder updateLog = null;
    
    public YCSBWorker(YCSBBenchmark benchmarkModule, int id, int init_record_count) {
        super(benchmarkModule, id);
        readRecord = new ZipfianGenerator(init_record_count);// pool for read keys
        randScan = new ZipfianGenerator(YCSBConstants.MAX_SCAN);
        
        synchronized (YCSBWorker.class) {
            // We must know where to start inserting
            if (insertRecord == null) {
                insertRecord = new CounterGenerator(init_record_count);
            }
        } // SYNCH
        
        // This is a minor speed-up to avoid having to invoke the hashmap look-up
        // everytime we want to execute a txn. This is important to do on 
        // a client machine with not a lot of cores
        if (Config.CAFE) {
            cacheStore = new YCSBCacheStore(conn);
            cacheBack = new YCSBWriteBack(conn);
            Stats stats = Stats.getStatsInstance(threadId);
            if (SmallBankConstants.STATS)
                Stats.stats = true;
            this.cafe = new NgCache(cacheStore, cacheBack, Config.CACHE_POOL_NAME, CachePolicy.WRITE_BACK, 1, stats,
                    this.benchmarkModule.workConf.getDBConnection(), this.benchmarkModule.workConf.getDBUsername(), 
                    this.benchmarkModule.workConf.getDBPassword(), true, Config.AR_SLEEP, 0, 10);
        }

        if (Config.ENABLE_LOGGING) {
            try {
                String dir = SmallBankConstants.TRACE_LOGGING_DIR;
                File ufile = new File(dir + "/update0-" + threadId + ".txt");
                FileWriter ufstream = new FileWriter(ufile);
                updateLogAll = new BufferedWriter(ufstream);
                // read file
                File rfile = new File(dir + "/read0-" + threadId + ".txt");
                FileWriter rfstream = new FileWriter(rfile);
                readLogAll = new BufferedWriter(rfstream);
                readLog = new StringBuilder();
                updateLog = new StringBuilder();
            } catch (IOException e) {
                e.printStackTrace(System.out);
            }
            transactionResults = new HashMap<String, Object>();
        }
        this.procUpdateRecord = this.getProcedure(UpdateRecord.class);
        this.procScanRecord = this.getProcedure(ScanRecord.class);
        this.procReadRecord = this.getProcedure(ReadRecord.class);
        this.procReadModifyWriteRecord = this.getProcedure(ReadModifyWriteRecord.class);
        this.procInsertRecord = this.getProcedure(InsertRecord.class);
        this.procDeleteRecord = this.getProcedure(DeleteRecord.class);
    }

    @Override
    protected TransactionStatus executeWork(TransactionType nextTrans) throws UserAbortException, SQLException {
        Class<? extends Procedure> procClass = nextTrans.getProcedureClass();
        
        if (procClass.equals(DeleteRecord.class)) {
            deleteRecord();
        } else if (procClass.equals(InsertRecord.class)) {
            insertRecord();
        } else if (procClass.equals(ReadModifyWriteRecord.class)) {
            readModifyWriteRecord();
        } else if (procClass.equals(ReadRecord.class)) {
            readRecord();
        } else if (procClass.equals(ScanRecord.class)) {
            scanRecord();
        } else if (procClass.equals(UpdateRecord.class)) {
            updateRecord();
        }
        conn.commit();
        return (TransactionStatus.SUCCESS);
    }

    private void updateRecord() throws SQLException {
        assert (this.procUpdateRecord!= null);
        int keyname = readRecord.nextInt();
        this.buildParameters();
        if(Config.CAFE)
        	this.procUpdateRecord.run(conn, keyname+"", this.cafe, this.params);
//        	this.procUpdateRecord.run(conn, keyname, this.params);
        else
        	this.procUpdateRecord.run(conn, keyname, this.params);
    }

    private void scanRecord() throws SQLException {
        assert (this.procScanRecord != null);
        int keyname = readRecord.nextInt();
        int count = randScan.nextInt();
        this.procScanRecord.run(conn, keyname, count, new ArrayList<String[]>());
    }

    private void readRecord() throws SQLException {
        assert (this.procReadRecord != null);
        int keyname = readRecord.nextInt();
        if(Config.CAFE)
        	this.procReadRecord.run(conn, keyname+"", this.cafe);
        	//this.procReadRecord.run(conn, keyname, this.results);
        else
        	this.procReadRecord.run(conn, keyname, this.results);
    }

    private void readModifyWriteRecord() throws SQLException {
        assert (this.procReadModifyWriteRecord != null);
        int keyname = readRecord.nextInt();
        this.buildParameters();
        if(Config.CAFE)
        	this.procReadModifyWriteRecord.run(conn, keyname+"", this.cafe, this.params, this.results);
        else
        	this.procReadModifyWriteRecord.run(conn, keyname, this.params, this.results);
    }

    private void insertRecord() throws SQLException {
        assert (this.procInsertRecord != null);
        int keyname = insertRecord.nextInt();
        this.buildParameters();
        if(Config.CAFE)
        	this.procInsertRecord.run(conn, keyname+"",this.cafe, this.params);
        else
        	this.procInsertRecord.run(conn, keyname, this.params);
    }

    private void deleteRecord() throws SQLException {
        assert (this.procDeleteRecord != null);
        int keyname = readRecord.nextInt();
        if(Config.CAFE)
        	this.procDeleteRecord.run(conn, this.cafe,keyname+"");
        else
        	this.procDeleteRecord.run(conn, keyname);
    }

    private void buildParameters() {
        Random rng = rng();
        for (int i = 0; i < this.params.length; i++) {
            this.params[i] = new String(TextGenerator.randomFastChars(rng, this.data));
        } // FOR
    }
}
