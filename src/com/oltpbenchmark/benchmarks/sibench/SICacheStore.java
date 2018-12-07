package com.oltpbenchmark.benchmarks.sibench;

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
import com.oltpbenchmark.benchmarks.sibench.procedures.MinRecord;
import com.oltpbenchmark.benchmarks.sibench.procedures.UpdateRecord;
import com.oltpbenchmark.benchmarks.voter.ContestantResult;
import com.oltpbenchmark.benchmarks.voter.VoterConstants;
import com.oltpbenchmark.benchmarks.ycsb.YCSBConstants;
import com.oltpbenchmark.jdbc.AutoIncrementPreparedStatement;
import com.oltpbenchmark.types.DatabaseType;
import com.usc.dblab.cafe.CacheEntry;
import com.usc.dblab.cafe.CacheStore;
import com.usc.dblab.cafe.Change;
import com.usc.dblab.cafe.Delta;
import com.usc.dblab.cafe.QueryResult;

public class SICacheStore extends CacheStore{
	private static final String SET = "S";
    private static final String INCR = "P";
    private static final String INCR_OR_SET = "O";
    private static final String ADD = "A";
    private static final String REMOVE_FIRST = "R";
    private static final String RMV = "D";
    private static final String CHECK = "C";
    final static MinRecord minRecord = new MinRecord();
    final static UpdateRecord updateRecord = new UpdateRecord();
	private DatabaseType dbType;
    private Map<String, SQLStmt> name_stmt_xref;
    private final Map<SQLStmt, String> stmt_name_xref = new HashMap<SQLStmt, String>();
    private final Map<SQLStmt, PreparedStatement> prepardStatements = new HashMap<SQLStmt, PreparedStatement>();
    public final SQLStmt minStmt = new SQLStmt(
            "SELECT * FROM SITEST ORDER BY value ASC LIMIT 1"
        );
    
    public final SQLStmt readQuery = new SQLStmt(
            "SELECT * FROM SITEST WHERE id = ?"
        );
    private Connection conn;
    public SICacheStore(Connection conn ) {
    	this.conn = conn;
    }
    
    
	@Override
	public Map<String, Delta> updateCacheEntries(String dml, Set<String> keys) {
		// TODO Auto-generated method stub
		String[] tokens = dml.split(",");
        Delta d = null;
        String s;
        Map<String, Delta> map = new HashMap<>();
        switch (tokens[0]) {
        	case  SIConstants.UPDATE_VALUE:{
        		s = String.format("%s,o_value,%s", SET,(Integer.parseInt(tokens[2])+1));
        		d = new Delta(Change.TYPE_SET, s);
        		map.put(String.format(SIConstants.UPDATE_VALUE_KEY, tokens[1]),d);
        		break;
        	}case SIConstants.QUERY_MIN:{
        		s = String.format("%s,o_id,%s;%s,o_value,%s", SET,(Integer.parseInt(tokens[1])),SET,(Integer.parseInt(tokens[2])));
        		d = new Delta(Change.TYPE_SET, s);
        		map.put(String.format(SIConstants.QUERY_MIN),d);
        	}
        }
		return map;
	}

	@Override
	public Set<String> getReferencedKeysFromQuery(String query) {
		// TODO Auto-generated method stub
		String[] tokens = query.split(",");
        String op = tokens[0];
        Set<String> set = new HashSet<>();

        switch (op) {
        case SIConstants.QUERY_MIN:
            set.add(String.format(SIConstants.QUERY_MIN));
            break;
        case SIConstants.UPDATE_VALUE:
        	set.add(String.format(SIConstants.UPDATE_VALUE_KEY,tokens[1]));
            break;
        }
        return set;
	}

	@Override
	public Set<String> getImpactedKeysFromDml(String dml) {
		// TODO Auto-generated method stub
		String[] tokens = dml.split(",");
        String op = tokens[0];
        Set<String> set = new HashSet<>();
        switch (op) {
        	case SIConstants.UPDATE_VALUE:{
        		set.add(String.format(SIConstants.UPDATE_VALUE_KEY, tokens[1]));
       		 	break;
        	}case SIConstants.QUERY_MIN:{
        		set.add(String.format(SIConstants.QUERY_MIN));
       		 	break;
        	}
        }
		
		return set;
	}

	@Override
	public QueryResult queryDataStore(String query) throws Exception {
		// TODO Auto-generated method stub
		String[] tokens = query.split(",");
        String op = tokens[0];
        
        PreparedStatement stmt = null;
        ResultSet r0 = null;
        switch (op) {
        	case SIConstants.QUERY_MIN:{
        		//int minId = this.minRecord.run(conn);
        		stmt = getPreparedStatement(conn,minStmt);
                r0 = stmt.executeQuery();
                try {
                    if (!((ResultSet) r0).next()) {
                    	String msg = "invalid contestant '" + tokens[1] + "'";
                    	throw new UserAbortException(msg);                   
                    }else {
                    	MinResult min_result = new MinResult(query,r0.getInt(1),r0.getInt(2));
                    	return min_result;
                    }
                } finally {
                    r0.close();
                }
                //break;
        	}case SIConstants.UPDATE_VALUE:{
        		stmt = getPreparedStatement(conn,readQuery);
        		 stmt.setInt(1, Integer.parseInt(tokens[1]));
                r0 = stmt.executeQuery();
                try {
                    if (!((ResultSet) r0).next()) {
                    	String msg = "invalid contestant '" + tokens[1] + "'";
                    	throw new UserAbortException(msg);                   
                    }else {
                    	MinResult min_result = new MinResult(query,r0.getInt(1),r0.getInt(2));
                    	return min_result;
                    }
                } finally {
                    r0.close();
                }
        	}
                
        		//return new MinResult(query,minId);
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

	@Override
	public Set<CacheEntry> computeCacheEntries(String query, QueryResult result) {
		// TODO Auto-generated method stub
		Set<CacheEntry> set = new HashSet<>();
		 String[] tokens = query.split(",");

       CacheEntry e = null;
       Map<String, String> map = new HashMap<>();
       

           switch (tokens[0]) {
           case SIConstants.QUERY_MIN:{
        	   MinResult min = (MinResult)result;
        	   map.put("o_id",min.getId()+"");
        	   map.put("o_value", min.getValue()+"");
        	   String key = String.format(SIConstants.QUERY_MIN);
               
               if (map.size() > 0) {
                   e = new CacheEntry(key, map, false);
               }
               break;
        	   
           }case SIConstants.READ_VALUE:{
        	   MinResult min = (MinResult)result;
        	   map.put("o_id",min.getId()+"");
        	   map.put("o_value", min.getValue()+"");
        	   String key = String.format(SIConstants.READ_VALUE_KEY,tokens[1]);
               
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
		String[] tokens = dml.split(",");
		PreparedStatement ps = null;
        ResultSet r0 = null;
		switch (tokens[0]) {
			case SIConstants.UPDATE_VALUE:{
				updateRecord.run(conn, Integer.parseInt(tokens[1]));
			}
		}
		return true;
	}

	@Override
	public CacheEntry applyDelta(Delta delta, CacheEntry cacheEntry) {
		// TODO Auto-generated method stub
		Object cacheVal = cacheEntry.getValue();
        if (cacheVal instanceof HashMap) {
            HashMap map = (HashMap)cacheVal;
            if (map.size() == 0) {
                applyDeltaHashMap(delta, (HashMap<String, String>)cacheVal);    
            } else {
                Object akey = map.keySet().iterator().next();
                if (akey instanceof String) {
                    applyDeltaHashMap(delta, (HashMap<String, String>)cacheVal);
                } else if (akey instanceof Integer) {
                    applyDeltaHashMap2(delta, (HashMap<Integer, Set<Integer>>)cacheVal);
                }
            }
        } else if (cacheVal instanceof List) {
            List<Integer> val = (List<Integer>)cacheVal;
            Object obj = delta.getValue();
            if (obj instanceof Integer) {
                switch (delta.getType()) {
                case Delta.TYPE_RMW:
                    if (val.contains(obj)) {
                        val.remove((Integer)obj);
                    } else {
                        System.out.println("Value of key "+cacheEntry.getKey()+" does not contains "+obj);
                    }
                    break;
                case Delta.TYPE_APPEND:
                    if (val.contains((obj))) {
                        System.out.println("Value of key "+cacheEntry.getKey()+ " contains "+ obj);
                    } else {
                            val.add((Integer)obj);
                    }
                    break;
                }
            }
        }
        return cacheEntry;
		
	}
	
	private void applyDeltaHashMap2(Delta delta,
            HashMap<Integer, Set<Integer>> v) {
        String dVal = (String) delta.getValue();
        String[] ops = dVal.split(";");
        int old_o_id = 0;
        for (String op: ops) {
            String[] fields = op.split(",");
            switch (fields[0]) {
            case ADD:
                int o_id = Integer.parseInt(fields[2]);
                int i_id = Integer.parseInt(fields[4]);
                 Set<Integer> set = v.get(o_id);
                if (set == null) {
                    set = new HashSet<>();
                    v.put(o_id, set);
                }
                set.add(i_id);
                old_o_id = o_id - 20;
                break;
            case REMOVE_FIRST:
                v.remove(old_o_id);
                break;
            default:
                System.out.println("Error: not a delta of type ADD");
                break;
            }
        }
    }

    private void applyDeltaHashMap(Delta delta, HashMap<String, String> cacheVal) {
        String dVal = (String) delta.getValue();
        String[] ops = dVal.split(";");
        
        // handle special case where there must be a check on the list of attributes.
        if (dVal.contains(CHECK)) {
            for (String op: ops) {
                if (op.contains(CHECK)) {
                    String[] fields = op.split(",");
                    if (fields[0].equals(CHECK)) {
                        String attr = fields[1];
                        String val = cacheVal.get(attr);
                        if (val == null || !val.equals(fields[2])) {
                            return;
                        }
                    }
                }
            }
        }
        
        for (String op: ops) {
            String[] fields = op.split(",");
            switch (fields[0]) {
            case SET:
                cacheVal.put(fields[1], fields[2]);
                break;
//            case INCR:
//                String val = cacheVal.get(fields[1]);
//                if (val != null) {
//                    double d = Double.parseDouble(val);
//                    double i = Double.parseDouble(fields[2]);
//                    cacheVal.put(fields[1], String.valueOf(d+i));
//                }
//                break;
//            case INCR_OR_SET:
//                val = cacheVal.get(fields[1]);
//                if (val == null) {
//                    cacheVal.put(fields[1], fields[2]);
//                } else {
//                    double d = Double.parseDouble(val);
//                    double i = Double.parseDouble(fields[2]);
//                    cacheVal.put(fields[1], String.valueOf(d+i));
//                }
            }
        }
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
		
	        if(key.startsWith(SIConstants.QUERY_MIN) || key.startsWith(SIConstants.UPDATE_VALUE) ) {
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
	        case SIConstants.QUERY_MIN:{
	        	int id = Integer.parseInt(map1.get("o_id"));
	        	int value = Integer.parseInt(map1.get("o_value"));
	        	return new MinResult(query,id,value);
	        }case SIConstants.UPDATE_VALUE:{
	        	int id = Integer.parseInt(tokens[1]);
	        	int value = Integer.parseInt(map1.get("o_value"));
	        	return new MinResult(query,id,value);
	        }
	        
	        }
		return null;
	}

}
