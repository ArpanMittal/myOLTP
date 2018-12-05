package com.oltpbenchmark.benchmarks.voter;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.benchmarks.ycsb.YCSBConstants;
import com.oltpbenchmark.benchmarks.ycsb.results.UserResult;
import com.oltpbenchmark.jdbc.AutoIncrementPreparedStatement;
import com.oltpbenchmark.types.DatabaseType;
import com.usc.dblab.cafe.CacheEntry;
import com.usc.dblab.cafe.CacheStore;
import com.usc.dblab.cafe.Delta;
import com.usc.dblab.cafe.QueryResult;
import java.sql.PreparedStatement;

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
    
 // potential return codes
    public static final long VOTE_SUCCESSFUL = 0;
    public static final long ERR_INVALID_CONTESTANT = 1;
    public static final long ERR_VOTER_OVER_VOTE_LIMIT = 2;
	
    // Checks if the vote is for a valid contestant
    public final SQLStmt checkContestantStmt = new SQLStmt(
	   "SELECT * FROM CONTESTANTS WHERE contestant_number = ?;"
    );
	
    // Checks if the voter has exceeded their allowed number of votes
    public final SQLStmt checkVoterStmt = new SQLStmt(
		"SELECT COUNT(*) FROM VOTES WHERE phone_number = ?;"
    );
	
    // Checks an area code to retrieve the corresponding state
    public final SQLStmt checkStateStmt = new SQLStmt(
		"SELECT * FROM AREA_CODE_STATE WHERE area_code = ?;"
    );
	
    // Records a vote
    public final SQLStmt insertVoteStmt = new SQLStmt(
		"INSERT INTO VOTES (vote_id, phone_number, state, contestant_number, created) " +
    "VALUES (?, ?, ?, ?, NOW());"
    );
	

    private Connection conn;
    
    public VoterCacheStore(Connection conn) {
    	this.conn = conn;
    }
    
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
        		set.add(String.format(VoterConstants.TABLENAME_LOCATIONS_KEY,tokens[1]));
       		 	break;
       		 	//For select number of votes statement in votes
        	case VoterConstants.TABLENAME_VOTES:
        		set.add(String.format(VoterConstants.TABLENAME_VOTES_KEY,tokens[1],tokens[2]));
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
		String[] tokens = query.split(",");
        String op = tokens[0];
       
        PreparedStatement stmt = null;
        ResultSet r0 = null;
        switch (op) {
        	case VoterConstants.TABLENAME_CONTESTANTS:{
//        		stmt = this.getPreparedStatement(conn, checkContestantStmt, tokens[1]);
        		stmt = getPreparedStatement(conn, checkContestantStmt);
                stmt.setInt(1, Integer.parseInt(tokens[1]));
                r0 = stmt.executeQuery();
                try {
                    if (!((ResultSet) r0).next()) {
                    	String msg = "invalid contestant '" + tokens[1] + "'";
                    	throw new UserAbortException(msg);
//                        return ERR_INVALID_CONTESTANT;    
                    }else {
//                    	int con_num = r0.getInt(0);
//                    	String name = r0.getString(1);
//                    	String con_num = r0.getString(2);
                    	ContestantResult con_result = new ContestantResult(query,r0.getInt("contestant_number"),r0.getString("contestant_name"));
                    	return con_result;
                    }
                } finally {
                    r0.close();
                }
        	}case VoterConstants.TABLENAME_VOTES:{
        		stmt = this.getPreparedStatement(conn, checkVoterStmt);
                stmt.setLong(1,Long.parseLong(tokens[1]));
                r0 = stmt.executeQuery();
                boolean hasVoterEnt = r0.next();
                try {
                    if (hasVoterEnt && r0.getLong(1) >= Long.parseLong(tokens[2])) {
                    	String msg = "invalid votes '" + tokens[1] + "'";
                    	throw new UserAbortException(msg);
                    }else {
                    	int count = r0.getInt(1);
                    	return new VoteCountResult(query,Long.parseLong(tokens[1]),r0.getInt(1)+"");
                    }
                } finally {
                    r0.close();
                }
        	}case VoterConstants.TABLENAME_LOCATIONS:{
        		stmt = this.getPreparedStatement(conn, checkStateStmt);
                stmt.setShort(1, (short)(Integer.parseInt(tokens[1])));
                r0 = stmt.executeQuery();
              
                final String state = r0.next() ? r0.getString(2) : "XX";
                r0.close();
                return new StateResult(query,(short)(Integer.parseInt(tokens[1])),state);
                
        	}
        }
		return null;
	}
	
	

	@Override
	public Set<CacheEntry> computeCacheEntries(String query, QueryResult result) {
		// TODO Auto-generated method stub
		Set<CacheEntry> set = new HashSet<>();
		String[] tokens = query.split(",");
		CacheEntry e = null;
		String key = null;
		Map<String, String> map = new HashMap<>();
        switch (tokens[0]) {
        	case VoterConstants.TABLENAME_CONTESTANTS:{
        		ContestantResult con_result = (ContestantResult)result;
        		map.put("o_con_name",con_result.getName());
        		key = String.format(VoterConstants.TABLENAME_CONTESTANTS, con_result.getKey());
        		if (map.size() > 0) {
                    e = new CacheEntry(key, map, false);
                }
        		break;
        	}case VoterConstants.TABLENAME_LOCATIONS:{
        		StateResult state_result = (StateResult)result;
        		map.put("o_state_name", state_result.getState_name() );
        		key = String.format(VoterConstants.TABLENAME_LOCATIONS, state_result.getArea_code());
        		if (map.size() > 0) {
                    e = new CacheEntry(key, map, false);
                }
        		break;
        	}case VoterConstants.TABLENAME_VOTES:{
        		VoteCountResult vote_count_result = (VoteCountResult)result;
        		map.put("o_vote_count", vote_count_result.getVote_count());
        		key = String.format(VoterConstants.TABLENAME_VOTES, vote_count_result.getPhone_num());
        		if (map.size() > 0) {
                    e = new CacheEntry(key, map, false);
                }
        		break;
        	}
        	
           }
        
        if (e != null)
            set.add(e);

        return set;
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
		Object val = cacheEntry.getValue();
        byte[] bytes = null;
        
        if (val instanceof HashMap) {
            HashMap map = (HashMap)val;
            if (map.size() == 0) {
                bytes = serializeHashMap((HashMap<String, String>)val);
            } else {
                Object akey = map.keySet().iterator().next();
                if (akey instanceof String) {
                    bytes = serializeHashMap((HashMap<String, String>)val);
                } else if (akey instanceof Integer) {
                    bytes = serializeHashMap2((HashMap<Integer, Set<Integer>>)val);
                }
            }
        } else if (val instanceof List) {
            bytes = serializeList((List<Integer>) val);
        }
        return bytes;
	}

	private byte[] serializeList(List<Integer> val) {
        if (val == null || val.size() == 0) return null;
        ByteBuffer bf = ByteBuffer.allocate(val.size()*4);
        for (Integer v: val) {
            bf.putInt(v);
        }
        return bf.array();
    }

    private byte[] serializeHashMap(Map<String, String> map) {
        int total = 0;
        for (String key: map.keySet()) {
            total += key.length()+4;
            String val = map.get(key);
            total += val.length()+4;
        }

        ByteBuffer buffer = ByteBuffer.allocate(total);
        for (String key: map.keySet()) {
            buffer.putInt(key.length());
            buffer.put(key.getBytes());
            String val = map.get(key);
            buffer.putInt(val.length());
            buffer.put(val.getBytes());            
        }
        return buffer.array();
    }
    
    private byte[] serializeHashMap2(Map<Integer, Set<Integer>> map) {
        int total = 0;
        for (Integer key: map.keySet()) {
            total += 4;
            total += 4; // size of set
            total += 4 * map.get(key).size();
        }

        ByteBuffer buffer = ByteBuffer.allocate(total);
        for (Integer key: map.keySet()) {
            buffer.putInt(key.intValue());
            
            Set<Integer> set = map.get(key);
            buffer.putInt(set.size());
            for (Integer x: set) {
                buffer.putInt(x);
            }
        }
        return buffer.array();
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
	public byte[] serialize(Delta delta) {
		// TODO Auto-generated method stub
		byte[] bytes = null;
        
        if (delta.getType() == Delta.TYPE_APPEND) {
            int x = (int)delta.getValue();
            ByteBuffer bf = ByteBuffer.allocate(4);
            bf.putInt(x);
            bytes = bf.array();
        }
        
        if (delta.getType() == Delta.TYPE_RMW || delta.getType() == Delta.TYPE_SET) {
            Map<String, String> obj = new HashMap<>();
            String val = (String)delta.getValue();
            String[] fields = val.split(";");
            for (String field: fields) {
                String[] tokens = field.split(",");
                obj.put(tokens[1], tokens[2]);
            }        
            bytes = serializeHashMap(obj);
        }
        return bytes;
		
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
	        case VoterConstants.TABLENAME_CONTESTANTS:{
	        	int id = Integer.parseInt(tokens[1]);
                String con_name = map1.get("o_con_name");
                
                return new ContestantResult(query,id,con_name);
	        }
	        case VoterConstants.TABLENAME_LOCATIONS:{
	        	int id = Integer.parseInt(tokens[1]);
                String con_name = map1.get("o_state_name");
                
                return new StateResult(query,id,con_name);
	        }
	        case VoterConstants.TABLENAME_VOTES:{
	        	int vote_id = Integer.parseInt(tokens[1]);
                String o_vote_count = map1.get("o_vote_count");
                return new VoteCountResult(query,vote_id,o_vote_count);
	        }
//	        case VoterConstants.TABLENAME_VOTES:{
//	        	int vote_id = Integer.parseInt(tokens[1]);
//	        	int con_id = Integer.parseInt(tokens[2]);
//	        	
//                String field1 = map1.get("o_field1");
//                String field2 = map1.get("o_field2");
//                String field3  = map1.get("o_field3");
//                String field4  = map1.get("o_field4");
//                String field5 = map1.get("o_field5");
//                String field6 = map1.get("o_field6");
//                String field7 = map1.get("o_field7");
//                String field8 = map1.get("o_field8");
//                String field9 = map1.get("o_field9");
//                String field10 = map1.get("o_field10");
//                return new UserResult(query,id, field1, field2,field3, field4, field5, field6, field7, field8, field9, field10);
//	        }
	        }
	
		return null;
	}
	
	
	 public final PreparedStatement getPreparedStatement(Connection conn, SQLStmt stmt, Object...params) throws SQLException {
	        PreparedStatement pStmt = this.getPreparedStatementReturnKeys(conn, stmt, null);
	        for (int i = 0; i < params.length; i++) {
	            pStmt.setObject(i+1, params[i]);
	        } // FOR
	        return (pStmt);
	    }

	    public final PreparedStatement getPreparedStatementReturnKeys(Connection conn, SQLStmt stmt, int[] is) throws SQLException {
	        assert(this.name_stmt_xref != null) : "The Procedure " + this + " has not been initialized yet!";
	        PreparedStatement pStmt = this.prepardStatements.get(stmt);
	        if (pStmt == null) {
	            assert(this.stmt_name_xref.containsKey(stmt)) :
	                "Unexpected SQLStmt handle in " + this.getClass().getSimpleName() + "\n" + this.name_stmt_xref;

	            // HACK: If the target system is Postgres, wrap the PreparedStatement in a special
	            //       one that fakes the getGeneratedKeys().
	            if (is != null && this.dbType == DatabaseType.POSTGRES) {
	                pStmt = new AutoIncrementPreparedStatement(this.dbType, conn.prepareStatement(stmt.getSQL()));
	            }
	            // Everyone else can use the regular getGeneratedKeys() method
	            else if (is != null) {
	                pStmt = conn.prepareStatement(stmt.getSQL(), is);
	            }
	            // They don't care about keys
	            else {
	                pStmt = conn.prepareStatement(stmt.getSQL());
	            }
	            this.prepardStatements.put(stmt, pStmt);
	        }
	        assert(pStmt != null) : "Unexpected null PreparedStatement for " + stmt;
	        return (pStmt);
	    }
}
