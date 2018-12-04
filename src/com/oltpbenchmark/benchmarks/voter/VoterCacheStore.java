package com.oltpbenchmark.benchmarks.voter;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.ycsb.YCSBConstants;
import com.oltpbenchmark.benchmarks.ycsb.results.UserResult;
import com.oltpbenchmark.types.DatabaseType;
import com.usc.dblab.cafe.CacheEntry;
import com.usc.dblab.cafe.CacheStore;
import com.usc.dblab.cafe.Delta;
import com.usc.dblab.cafe.QueryResult;

public class VoterCacheStore extends CacheStore{
	private static final String SET = "S";
    private static final String INCR = "P";
    private static final String INCR_OR_SET = "O";
    private static final String ADD = "A";
    private static final String REMOVE_FIRST = "R";
    private static final String RMV = "D";
    private static final String CHECK = "C";
    
    private DatabaseType dbType;
    private Map<String, SQLStmt> name_stmt_xref;
    private final Map<SQLStmt, String> stmt_name_xref = new HashMap<SQLStmt, String>();
    private final Map<SQLStmt, PreparedStatement> prepardStatements = new HashMap<SQLStmt, PreparedStatement>();

    private Connection conn;

    
	@Override
	public Map<String, Delta> updateCacheEntries(String dml, Set<String> keys) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<String> getReferencedKeysFromQuery(String query) {
		// TODO Auto-generated method stub
		String[] tokens = query.split(",");
        String op = tokens[0];
        Set<String> set = new HashSet<>();
        switch (op) {
        	case VoterConstants.TABLENAME_CONTESTANTS:
        		 set.add(String.format(VoterConstants.TABLENAME_CONTESTANTS_KEY,tokens[1]));
        		 break;
        	case VoterConstants.TABLENAME_LOCATIONS:
        		set.add(String.format(VoterConstants.TABLENAME_LOCATIONS_key,tokens[1]));
       		 	break;
        	case VoterConstants.TABLENAME_VOTES:
        		set.add(String.format(VoterConstants.TABLENAME_VOTES_KEY,tokens[1]));
       		 	break;
        }
        
		return set;
	}

	@Override
	public Set<String> getImpactedKeysFromDml(String dml) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public QueryResult queryDataStore(String query) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<CacheEntry> computeCacheEntries(String query, QueryResult result) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean dmlDataStore(String dml) throws Exception {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public CacheEntry applyDelta(Delta delta, CacheEntry cacheEntry) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] serialize(CacheEntry cacheEntry) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CacheEntry deserialize(String key, Object obj, byte[] buffer) {
		// TODO Auto-generated method stub
			byte[] bytes = (byte[]) obj;
        
        int offset = 0;
        ByteBuffer buff = ByteBuffer.wrap(bytes);
		
	        if(key.startsWith(VoterConstants.TABLENAME_CONTESTANTS) || key.startsWith(VoterConstants.TABLENAME_LOCATIONS) || key.startsWith(VoterConstants.TABLENAME_VOTES)) {
	        	Map<String, String> map = new HashMap<>();
	            while (offset < bytes.length) {
	                int len = buff.getInt();
	                offset += 4;

	                byte[] bs = new byte[len];
	                buff.get(bs);
	                offset += len;            
	                String k = new String(bs);

	                len = buff.getInt();
	                offset += 4;

	                bs = new byte[len];
	                buff.get(bs);
	                String v = new String(bs);
	                offset += len;

	                map.put(k, v);
	            }
	            return new CacheEntry(key, map, false);
	                
	        }
			return null;
		
		
	}

	@Override
	public byte[] serialize(Delta change) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getHashCode(String key) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public QueryResult computeQueryResult(String query, Set<CacheEntry> entries) {
		// TODO Auto-generated method stub
		String[] tokens = query.split(",");
		 Set<String> keys = getReferencedKeysFromQuery(query);
	        if (keys.size() == 0) return null;
	        String key = keys.iterator().next();    // only one key
	        CacheEntry entry = null;
	        for (CacheEntry e: entries) {
	            if (e.getKey().equals(key)) {
	                entry = e;
	                break;
	            }
	        }
	        if (entry == null) return null;
	        Map<String, String> map1  = null;
	        Map<Integer, Set<Integer>> map2  = null;
	        List<Integer> ids = null;
	        if (entry.getValue() instanceof HashMap) {
	            HashMap map = (HashMap)entry.getValue();
	            Object akey = map.keySet().iterator().next();
	            if (akey instanceof String) {
	                map1 = (Map<String, String>)entry.getValue();
	            } else if (akey instanceof Integer) {
	                map2 = (Map<Integer, Set<Integer>>)entry.getValue();
	            }
	        }
	        if (entry.getValue() instanceof List) {
	            ids = (List<Integer>)entry.getValue();
	        }
	        switch (tokens[0]) {
	        case YCSBConstants.QUERY_USERTABLE:{
	        	int id = Integer.parseInt(tokens[1]);
                String field1 = map1.get("o_field1");
                String field2 = map1.get("o_field2");
                String field3  = map1.get("o_field3");
                String field4  = map1.get("o_field4");
                String field5 = map1.get("o_field5");
                String field6 = map1.get("o_field6");
                String field7 = map1.get("o_field7");
                String field8 = map1.get("o_field8");
                String field9 = map1.get("o_field9");
                String field10 = map1.get("o_field10");
                return new UserResult(query,id, field1, field2,field3, field4, field5, field6, field7, field8, field9, field10);
	        }
	        case YCSBConstants.INSERT_QUERY_USERTABLE:{
	        	int id = Integer.parseInt(tokens[1]);
                String field1 = map1.get("o_field1");
                String field2 = map1.get("o_field2");
                String field3  = map1.get("o_field3");
                String field4  = map1.get("o_field4");
                String field5 = map1.get("o_field5");
                String field6 = map1.get("o_field6");
                String field7 = map1.get("o_field7");
                String field8 = map1.get("o_field8");
                String field9 = map1.get("o_field9");
                String field10 = map1.get("o_field10");
                return new UserResult(query,id, field1, field2,field3, field4, field5, field6, field7, field8, field9, field10);
	        }
	        }
	
		return null;
	}

}
