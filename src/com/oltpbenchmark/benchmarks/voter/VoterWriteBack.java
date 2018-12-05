package com.oltpbenchmark.benchmarks.voter;

import static com.oltpbenchmark.benchmarks.tpcc.TPCCConfig.DML_UPDATE_STOCK_PRE;

import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.NotImplementedException;

import com.oltpbenchmark.benchmarks.ycsb.YCSBConstants;
import com.usc.dblab.cafe.Change;
import com.usc.dblab.cafe.Delta;
import com.usc.dblab.cafe.QueryResult;
import com.usc.dblab.cafe.Session;
import com.usc.dblab.cafe.Stats;
import com.usc.dblab.cafe.WriteBack;

import edu.usc.dblab.intervaltree.Interval1D;

public class VoterWriteBack extends WriteBack{
	private static final String INSERT = "I";
    private static final String SET = "S";
    private static final String INCR = "A";
    private static final String DELETE = "D";
    private Statement stmt;
	private final Connection conn;
	
	public VoterWriteBack(Connection conn) {
		// TODO Auto-generated constructor stub
		this.conn = conn;
	}

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
//        	case VoterConstants.TABLENAME_CONTESTANTS:
//        		set.add(String.format(VoterConstants.WB_TABLENAME_CONTESTANTS_KEY,tokens[1]));
//        		break;
//        	case VoterConstants.TABLENAME_LOCATIONS:
//        		set.add(String.format(VoterConstants.WB_TABLENAME_LOCATIONS_key,tokens[1]));
//        		break;
  		 	//For select number of votes statement in votes
        	case VoterConstants.UPDATE_TABLENAME_VOTES:
        		set.add(String.format(VoterConstants.WB_TABLENAME_VOTES_KEY,tokens[1],tokens[2]));
        		break;
        }
		return set;
	}
	//For range QC
	@Override
    public Map<Interval1D, List<Change>> bufferPoints(String dml) {
		return null;
//        String[] tokens = dml.split(",");
//        
//        Map<Interval1D, List<Change>> m = new HashMap<>();
//        switch (tokens[0]) {
//        case DML_UPDATE_STOCK_PRE:
//            int p1 = Integer.parseInt(tokens[3]);
//            int p2 = Integer.parseInt(tokens[7]);
//            int i_id = Integer.parseInt(tokens[2]);
//            Change d = new Change(Change.TYPE_APPEND, INSERT+","+i_id);
//            List<Change> cs = new ArrayList<>(); cs.add(d);
//            m.put(new Interval1D(p1,p1), cs);
//            
//            d = new Change(Change.TYPE_APPEND, DELETE+","+i_id);
//            cs = new ArrayList<>(); cs.add(d);
//            m.put(new Interval1D(p2,p2), cs);
//            return m;
//        default:
//            return null;
//        }
    }

	@Override
	public LinkedHashMap<String, Change> rationalizeWrite(String dml) {
		// TODO Auto-generated method stub
		String[] tokens = dml.split(",");
        LinkedHashMap<String, Change> map = new LinkedHashMap<>();
        String it = null;
        Change c = null;
        switch (tokens[0]) {
        case VoterConstants.INSER_TABLENAME_VOTES:{
    		String s = String.format("%s,voteId,%s;%s,phoneNumber,%s;%s,state,%s;%s,contestantNumber,%s;", SET, tokens[1], SET, tokens[2],SET, tokens[3],SET, tokens[4]);
    		c = new Change(Change.TYPE_SET,s);
    		it = String.format(String.format(VoterConstants.WB_INSER_TABLENAME_VOTES_KEY, tokens[1]));
	       
	        break;
    	}case VoterConstants.UPDATE_TABLENAME_VOTES:{
    		String s = String.format("%s,count,%s", SET, tokens[3]);
    		c = new Change(Change.TYPE_SET,s);
    		it = String.format(String.format(VoterConstants.WB_UPDATE_TABLENAME_VOTES_KEY, tokens[1], tokens[2]));
    	}
        }
        if (c != null) 
        	map.put(it, c);
        return map;
	}

	@Override
	public byte[] serialize(Change change) {
		// TODO Auto-generated method stub
        int len = 0;
//        String sid = change.getSid();
//        len += 4+sid.length();

//        int sequenceId = change.getSequenceId();
//        len += 4;

        String val = (String) change.getValue();
        len += 4+val.length();

        ByteBuffer buff = ByteBuffer.allocate(len);
//        buff.putInt(sid.length());
//        buff.put(sid.getBytes());
//        buff.putInt(sequenceId);
        buff.putInt(val.length());
        buff.put(val.getBytes());
        return buff.array();
		
		
	}

	@Override
	public Change deserialize(byte[] bytes) {
		// TODO Auto-generated method stub
		ByteBuffer buff = ByteBuffer.wrap(bytes);
//      int len = buff.getInt();
//      byte[] bs = new byte[len];
//      buff.get(bs);
//      String sid = new String(bs);

//      int seqId = buff.getInt();

      int len = buff.getInt();
      byte[] bs = new byte[len];
      buff.get(bs);
      String val = new String(bs);
      Change change = new Change(Change.TYPE_APPEND, val);
//      change.setSid(sid);
//      change.setSequenceId(seqId);
      return change;
		
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
		int totalSize = 0;

        LinkedHashMap<String, byte[][]> bytesMap = new LinkedHashMap<>();
        for (String it: changesMap.keySet()) {
            totalSize += 4+it.length();

            List<Change> changes = changesMap.get(it);
            totalSize += 4; // storing the size of the list

            byte[][] bytesList = new byte[changes.size()][];
            for (int i = 0; i < changes.size(); ++i) {
                Change c = changes.get(i);
                byte[] bytes = serialize(c);
                bytesList[i] = bytes;
                
                totalSize += 4;
                totalSize += bytes.length;
            }

            bytesMap.put(it, bytesList);
        }

        ByteBuffer buff = ByteBuffer.allocate(totalSize);
        for (String it: changesMap.keySet()) {
            buff.putInt(it.length());
            buff.put(it.getBytes());

            byte[][] bytesList = bytesMap.get(it);
            buff.putInt(bytesList.length);
            for (int i = 0; i < bytesList.length; ++i) {
                buff.putInt(bytesList[i].length);
                buff.put(bytesList[i]);
            }
        }

        return buff.array();
		
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
