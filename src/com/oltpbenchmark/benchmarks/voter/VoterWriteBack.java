package com.oltpbenchmark.benchmarks.voter;

import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.NotImplementedException;

import com.usc.dblab.cafe.Change;
import com.usc.dblab.cafe.QueryResult;
import com.usc.dblab.cafe.Session;
import com.usc.dblab.cafe.Stats;
import com.usc.dblab.cafe.WriteBack;

public class VoterWriteBack extends WriteBack{

	@Override
	public Set<String> getMapping(String key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public LinkedHashMap<String, Change> bufferChanges(String dml, Set<String> buffKeys) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean applyBufferedWrite(String buffKey, Object buffValue) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isIdempotent(Object buffValue) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Object convertToIdempotent(Object buffValue) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<String> rationalizeRead(String query) {
		// TODO Auto-generated method stub
		String[] tokens = query.split(",");
        Set<String> set = new HashSet<>();
        switch (tokens[0]) {
        	case VoterConstants.TABLENAME_CONTESTANTS:
        		set.add(String.format(VoterConstants.TABLENAME_CONTESTANTS_KEY,tokens[1]));
        		break;
        	case VoterConstants.TABLENAME_LOCATIONS:
        		set.add(String.format(VoterConstants.TABLENAME_LOCATIONS_key,tokens[1]));
        		break;
  		 	//For select number of votes statement in votes
        	case VoterConstants.TABLENAME_VOTES:
        		set.add(String.format(VoterConstants.TABLENAME_VOTES_KEY,tokens[1],tokens[2]));
        		break;
        }
		return null;
	}

	@Override
	public LinkedHashMap<String, Change> rationalizeWrite(String dml) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] serialize(Change change) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Change deserialize(byte[] bytes) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean applySessions(List<Session> sess, Connection conn, Statement stmt, PrintWriter sessionWriter,
			Stats stats) throws Exception {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public QueryResult merge(String query, QueryResult result, LinkedHashMap<String, List<Change>> buffVals) {
		// TODO Auto-generated method stub
		if (buffVals == null || buffVals.size() == 0) 
			return result;
		
		
			String tokens[] = query.split(",");
			throw new NotImplementedException("implement insert in merge");
        // since each query impacts only one data item, get the change list.
//			List<Change> changes = buffVals.values().iterator().next();
//			switch (tokens[0]) {
//			
//			}
//		return null;
	}

	@Override
	public byte[] serializeSessionChanges(Map<String, List<Change>> changesMap) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getTeleWPartition(String sessId) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Map<String, List<Change>> deserializeSessionChanges(byte[] bytes) {
		// TODO Auto-generated method stub
		ByteBuffer buff = ByteBuffer.wrap(bytes);

        int offset = 0;
        LinkedHashMap<String, List<Change>> changesMap = new LinkedHashMap<>();
        while (offset < bytes.length) {
            int sz = buff.getInt();
            byte[] b = new byte[sz];
            buff.get(b);
            String it = new String(b);
            offset += 4+sz;

            sz = buff.getInt();
            offset += 4;

            List<Change> changes = new ArrayList<>();
            for (int i = 0; i < sz; ++i) {
                int len = buff.getInt();
                offset += 4;

                b = new byte[len];
                buff.get(b);
                offset += len;
                Change c = deserialize(b);
                changes.add(c);
            }

            changesMap.put(it, changes);
        }

        return changesMap;
		
	}
}
