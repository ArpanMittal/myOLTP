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

package com.oltpbenchmark.benchmarks.voter;

import java.sql.SQLException;

import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.Config;
import com.oltpbenchmark.benchmarks.smallbank.SmallBankConstants;
import com.oltpbenchmark.benchmarks.voter.PhoneCallGenerator.PhoneCall;
import com.oltpbenchmark.benchmarks.voter.procedures.Vote;
import com.oltpbenchmark.benchmarks.ycsb.YCSBCacheStore;
import com.oltpbenchmark.benchmarks.ycsb.YCSBWriteBack;
import com.oltpbenchmark.types.TransactionStatus;
import com.usc.dblab.cafe.CachePolicy;
import com.usc.dblab.cafe.CacheStore;
import com.usc.dblab.cafe.NgCache;
import com.usc.dblab.cafe.Stats;
import com.usc.dblab.cafe.WriteBack;

public class VoterWorker extends Worker<VoterBenchmark> {

    private final PhoneCallGenerator switchboard;
    public NgCache cafe = null;
    private CacheStore cacheStore;
    private WriteBack cacheBack;
    private int threadId;
    
    public VoterWorker(VoterBenchmark benchmarkModule, int id) {
        super(benchmarkModule, id);
        switchboard = new PhoneCallGenerator(0, benchmarkModule.numContestants);
        if (Config.CAFE) {
            cacheStore = new VoterCacheStore(conn);
            cacheBack = new VoterWriteBack(conn);
            Stats stats = Stats.getStatsInstance(threadId);
            if (SmallBankConstants.STATS)
                Stats.stats = true;
            this.cafe = new NgCache(cacheStore, cacheBack, Config.CACHE_POOL_NAME, CachePolicy.WRITE_BACK, 1, stats,
                    this.benchmarkModule.workConf.getDBConnection(), this.benchmarkModule.workConf.getDBUsername(), 
                    this.benchmarkModule.workConf.getDBPassword(), true, Config.AR_SLEEP, 0, 10);
        }
    }

    @Override
    protected TransactionStatus executeWork(TransactionType txnType) throws UserAbortException, SQLException {
        assert (txnType.getProcedureClass().equals(Vote.class));
        PhoneCall call = switchboard.receive();
        Vote proc = getProcedure(Vote.class);
        assert (proc != null);
        if(Config.CAFE)
        	proc.run(conn, call.voteId, call.phoneNumber, call.contestantNumber, VoterConstants.MAX_VOTES,cafe);
        else
        	proc.run(conn, call.voteId, call.phoneNumber, call.contestantNumber, VoterConstants.MAX_VOTES);
        conn.commit();
        return TransactionStatus.SUCCESS;
    }

}
