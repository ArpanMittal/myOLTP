package com.oltpbenchmark.benchmarks.sibench;

import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.NotImplementedException;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.sibench.procedures.UpdateRecord;
import com.oltpbenchmark.benchmarks.voter.VoterConstants;
import com.oltpbenchmark.types.DatabaseType;
import com.usc.dblab.cafe.Change;
import com.usc.dblab.cafe.Delta;
import com.usc.dblab.cafe.QueryResult;
import com.usc.dblab.cafe.Session;
import com.usc.dblab.cafe.Stats;
import com.usc.dblab.cafe.WriteBack;

import edu.usc.dblab.intervaltree.Interval1D;

public class SIWriteBack extends WriteBack{
	private static final String INSERT = "I";
    private static final String SET = "S";
    private static final String INCR = "A";
    private static final String DELETE = "D";
    private Statement stmt;
	private final Connection conn;
	private DatabaseType dbType;
	final static UpdateRecord updateRecord = new UpdateRecord();
    private Map<String, SQLStmt> name_stmt_xref;
    private final Map<SQLStmt, String> stmt_name_xref = new HashMap<SQLStmt, String>();
    private final Map<SQLStmt, PreparedStatement> prepardStatements = new HashMap<SQLStmt, PreparedStatement>();
    
    
    public SIWriteBack(Connection conn) {
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
        	case SIConstants.QUERY_MIN:{
        		set.add(String.format(SIConstants.WB_QUERY_MEM_MIN));
        		break;
        	}case SIConstants.READ_VALUE:{
        		set.add(String.format(SIConstants.WB_UPDATE_VALUE_KEY,tokens[1]));
        	}
        }
		return set;
	}

	@Override
	public LinkedHashMap<String, Change> rationalizeWrite(String dml) {
		// TODO Auto-generated method stub
		String[] tokens = dml.split(",");
        LinkedHashMap<String, Change> map = new LinkedHashMap<>();
        String it = null;
        Change c = null;
        switch (tokens[0]) {
        	case SIConstants.UPDATE_VALUE:
        		String s = String.format("%s,o_value,%s",SET ,Integer.parseInt( tokens[2])+1);
        		c = new Change(Change.TYPE_SET,s);
        		it = String.format(String.format(SIConstants.WB_UPDATE_VALUE_KEY, tokens[1]));
        		break;
        	case SIConstants.QUERY_MIN:{
        		s = String.format("%s,o_id,%s;%s,o_value,%s", SET,(Integer.parseInt(tokens[1])),SET,(Integer.parseInt(tokens[2])));
        		c = new Change(Change.TYPE_SET,s);
        		it = String.format(String.format(SIConstants.WB_QUERY_UP_MEM_MIN_KEY, tokens[1], tokens[2]));
        		break;
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
//       String sid = change.getSid();
//       len += 4+sid.length();

//       int sequenceId = change.getSequenceId();
//       len += 4;

       String val = (String) change.getValue();
       len += 4+val.length();

       ByteBuffer buff = ByteBuffer.allocate(len);
//       buff.putInt(sid.length());
//       buff.put(sid.getBytes());
//       buff.putInt(sequenceId);
       buff.putInt(val.length());
       buff.put(val.getBytes());
       return buff.array();
		
		
	}

	@Override
	public Change deserialize(byte[] bytes) {
		// TODO Auto-generated method stub
			ByteBuffer buff = ByteBuffer.wrap(bytes);
	
			int len = buff.getInt();
	      byte[] bs = new byte[len];
	      buff.get(bs);
	      String val = new String(bs);
	      Change change = new Change(Change.TYPE_APPEND, val);
	
	      return change;
		
	}

	@Override
	public boolean applySessions(List<Session> sessions, Connection conn, Statement stmt, PrintWriter sessionWriter,
			Stats stats) throws Exception {
		// TODO Auto-generated method stub
		Map<String, String> mergeMap = new HashMap<>(); 
		for (Session sess: sessions) {
	            List<String> its = sess.getIdentifiers();
	            for (int i = 0; i < its.size(); ++i) {
	                String identifier = its.get(i);
	                Change change = sess.getChange(i);
	                String val = mergeMap.get(identifier);
	                if (val == null) {
	                    mergeMap.put(identifier, (String)change.getValue());
	                }else {
	                	mergeMap.put(identifier, (String)change.getValue());
	                }
	            }
		 }
		
		PreparedStatement ps = null;
        for (String it: mergeMap.keySet()) {
            String val = mergeMap.get(it);
//            char op = val.charAt(0);
//            double amount = Double.parseDouble(val.substring(1));
            String[] tokens = val.split(";");
            String[] tokens2 = it.split(",");
            String table = tokens2[0];
            switch (table) {
            	case  SIConstants.UPDATE_VALUE:{
            		updateRecord.run(conn, Integer.parseInt(tokens2[1]));
            		break;
            	}
            }
        }
        conn.commit();
		return true;
	}

	@Override
	public QueryResult merge(String query, QueryResult result, LinkedHashMap<String, List<Change>> buffVals) {
		// TODO Auto-generated method stub
		if (buffVals == null || buffVals.size() == 0) 
			return result;
		
		
			String tokens[] = query.split(",");
			throw new NotImplementedException("implement insert in merge");
		
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
	
	 @Override
	    public Map<Interval1D, String> getImpactedRanges(Session sess) {
//	        List<Change> changes = sess.getChanges();
	        Map<Interval1D, String> res = new HashMap<>();
//	        for (Change c: changes) {
//	            String str = (String)c.getValue();
//	            if (str.contains("S_QUANTITY")) {
//	                String[] fs = str.split(";");
//	                for (String f: fs) {
//	                    if (f.contains("S_QUANTITY")) {
//	                        fs = f.split(",");
//	                        int p1 = Integer.parseInt(fs[2]);
//	                        res.put(new Interval1D(p1, p1), sess.getSid());
//	                        int p2 = Integer.parseInt(fs[3]);
//	                        res.put(new Interval1D(p2, p2), sess.getSid());
//	                        return res;
//	                    }
//	                }
//	            }
//	        }
	        
	        return res;
	    }

}
