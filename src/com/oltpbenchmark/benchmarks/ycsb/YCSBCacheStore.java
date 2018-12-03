package com.oltpbenchmark.benchmarks.ycsb;

import static com.oltpbenchmark.benchmarks.tpcc.TPCCConfig.DML_DELETE_NEW_ORDER_PRE;
import static com.oltpbenchmark.benchmarks.tpcc.TPCCConfig.DML_UPDATE_DIST_PRE;
import static com.oltpbenchmark.benchmarks.tpcc.TPCCConfig.DML_UPDATE_STOCK_PRE;
import static com.oltpbenchmark.benchmarks.tpcc.TPCCConstants.KEY_LAST_ORDER;
import static com.oltpbenchmark.benchmarks.tpcc.TPCCConstants.KEY_NEW_ORDER_IDS;
import static com.oltpbenchmark.benchmarks.tpcc.TPCCConstants.KEY_STOCK;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.NotImplementedException;

import com.oltpbenchmark.api.SQLStmt;

import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.benchmarks.smallbank.SmallBankConstants;
import com.oltpbenchmark.benchmarks.smallbank.results.AccountResult;
import com.oltpbenchmark.benchmarks.smallbank.results.CheckingResult;
import com.oltpbenchmark.benchmarks.smallbank.results.SavingsResult;
import com.oltpbenchmark.benchmarks.tpcc.procedures.results.QueryGetCustomerById;
import com.oltpbenchmark.benchmarks.ycsb.results.UserResult;
import com.oltpbenchmark.jdbc.AutoIncrementPreparedStatement;
import com.oltpbenchmark.types.DatabaseType;
import com.usc.dblab.cafe.CacheEntry;
import com.usc.dblab.cafe.CacheStore;
import com.usc.dblab.cafe.Change;
import com.usc.dblab.cafe.Delta;
import com.usc.dblab.cafe.QueryResult;

import edu.usc.dblab.intervaltree.Interval1D;

import com.oltpbenchmark.benchmarks.ycsb.YCSBConstants;
import com.oltpbenchmark.benchmarks.ycsb.procedures.DeleteRecord;
import com.oltpbenchmark.benchmarks.ycsb.procedures.InsertRecord;
import com.oltpbenchmark.benchmarks.ycsb.procedures.ReadRecord;
import com.oltpbenchmark.benchmarks.ycsb.procedures.UpdateRecord;

public class YCSBCacheStore extends CacheStore {
	private static final String SET = "S";
    private static final String INCR = "P";
    private static final String INCR_OR_SET = "O";
    private static final String ADD = "A";
    private static final String REMOVE_FIRST = "R";
    private static final String RMV = "D";
    private static final String CHECK = "C";
//	public static final String KEY_USER_NAME_PREFIX = "k_usr_name";
//    public static final String KEY_USER_NAME = KEY_USER_NAME_PREFIX+",%s";
    final static ReadRecord readRecord = new ReadRecord();
    final static UpdateRecord updateRecord = new UpdateRecord();
    final static InsertRecord insertRecord= new InsertRecord(); 
    final static DeleteRecord deleteRecord = new DeleteRecord();
	private DatabaseType dbType;
    private Map<String, SQLStmt> name_stmt_xref;
    private final Map<SQLStmt, String> stmt_name_xref = new HashMap<SQLStmt, String>();
    private final Map<SQLStmt, PreparedStatement> prepardStatements = new HashMap<SQLStmt, PreparedStatement>();
    
    private Connection conn;
	
    public final SQLStmt READ_STMT = new SQLStmt(
            "SELECT * FROM usertable WHERE YCSB_KEY=?"
        );
    
    public final SQLStmt KEY_UPDATE_STMT = new SQLStmt(
            "UPDATE USERTABLE SET FIELD1=?,FIELD2=?,FIELD3=?,FIELD4=?,FIELD5=?," +
            "FIELD6=?,FIELD7=?,FIELD8=?,FIELD9=?,FIELD10=? WHERE YCSB_KEY=?"
        );
    public final SQLStmt scanStmt = new SQLStmt(
            "SELECT * FROM USERTABLE WHERE YCSB_KEY>? AND YCSB_KEY<?"
        );
    
    
    
	/**
	 * @param conn
	 */
	public YCSBCacheStore(Connection conn) {
		
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
        case YCSBConstants.DELETE_QUERY_USERTABLE:
        	//s = String.format("%s,o_field1,%s;%s,o_field2,%s;%s,o_field3,%s;%s,o_field4,%s;%s,o_field5,%s;%s,o_field6,%s;%s,o_field7,%s;%s,o_field8,%s;%s,o_field9,%s;%s,o_field10,%s", SET, tokens[2], SET, tokens[3],SET, tokens[4],SET, tokens[5], SET, tokens[6], SET, tokens[7], SET, tokens[8],SET, tokens[9], SET, tokens[10], SET, tokens[11]);
            s = String.format("%s,o_field1,%s;%s,o_field2,%s;%s,o_field3,%s;%s,o_field4,%s;%s,o_field5,%s;%s,o_field6,%s;%s,o_field7,%s;%s,o_field8,%s;%s,o_field9,%s;%s,o_field10,%s", SET, null, SET, null,SET, null,SET, null, SET, null, SET, null, SET, null,SET, null, SET, null, SET, null);
            d = new Delta(Change.TYPE_RMW, s);
            map.put(String.format(YCSBConstants.MEM_UPDATE_USERTABLE_KEY, tokens[1]), d);  
            break;
        default:
	        s = String.format("%s,o_field1,%s;%s,o_field2,%s;%s,o_field3,%s;%s,o_field4,%s;%s,o_field5,%s;%s,o_field6,%s;%s,o_field7,%s;%s,o_field8,%s;%s,o_field9,%s;%s,o_field10,%s", SET, tokens[2], SET, tokens[3],SET, tokens[4],SET, tokens[5], SET, tokens[6], SET, tokens[7], SET, tokens[8],SET, tokens[9], SET, tokens[10], SET, tokens[11]);
	        
	        d = new Delta(Change.TYPE_SET, s);
	        map.put(String.format(YCSBConstants.MEM_UPDATE_USERTABLE_KEY, tokens[1]), d);  
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
        case YCSBConstants.QUERY_USERTABLE:
            set.add(String.format(YCSBConstants.MEM_UPDATE_USERTABLE_KEY, tokens[1]));
            break;
           
//        case SmallBankConstants.QUERY_ACCOUNT_BY_CUSTID_PREFIX:
//            set.add(String.format(KEY_ACCT_ID, tokens[1]));
//            break;
//        case SmallBankConstants.QUERY_SAVINGS_PREFIX:
//            set.add(String.format(KEY_SAVINGS_BAL, tokens[1]));
//            break;
//        case SmallBankConstants.QUERY_CHECKING_PREFIX:
//            set.add(String.format(KEY_CHECKING_BAL, tokens[1]));
//            break;
        }

        return set;
		
	}

	@Override
	public Set<String> getImpactedKeysFromDml(String dml) {
		// TODO Auto-generated method stub
		//This is dml memcached is different
		String[] tokens = dml.split(",");
        String op = tokens[0];
        Set<String> set = new HashSet<>();

        switch (op) {
        case YCSBConstants.UPDATE_QUERY_USERTABLE:
            set.add(String.format(YCSBConstants.MEM_UPDATE_USERTABLE_KEY, tokens[1]));
//        case SmallBankConstants.UPDATE_SAVINGS_PREFIX:
//            set.add(String.format(KEY_SAVINGS_BAL, tokens[1]));
//            break;
            break;
        case YCSBConstants.INSERT_QUERY_USERTABLE:
        	set.add(String.format(YCSBConstants.MEM_UPDATE_USERTABLE_KEY, tokens[1]));
        	break;
        case YCSBConstants.DELETE_QUERY_USERTABLE:
            set.add(String.format(YCSBConstants.MEM_UPDATE_USERTABLE_KEY, tokens[1]));
            break;
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
        case YCSBConstants.QUERY_USERTABLE:
        	
            //stmt = this.getPreparedStatement(conn, READ_STMT);
            String[] results = new String[11];
            this.readRecord.run(conn, Integer.parseInt(tokens[1]), results);
//            int temp = results.length;
            //stmt.setInt(1, Integer.parseInt(tokens[1]));  
//            stmt.setInt(1, 2);
//            r0 = stmt.executeQuery();
            if (results.length== 0) {
                String msg = "Invalid user_id '" + tokens[1] + "'";
                throw new UserAbortException(msg);
            }
            else if (results.length!= 0) {

            	
        	int user_id = Integer.parseInt(results[0]);
        	String field_01 = results[1];
        	String field_02 = results[2];
        	String field_03 = results[3];
        	String field_04 = results[4];
        	String field_05 = results[5];
        	String field_06 = results[6];
        	String field_07 = results[7];
        	String field_08 = results[8];
        	String field_09 = results[9];
        	String field_10 = results[10];
        	
            	return new UserResult(query, user_id, field_01, field_02, field_03, field_04, field_05, field_06, field_07, field_08, field_09, field_10);
            }
            
            

        }

       
		return null;
	}



	@Override
	public Set<CacheEntry> computeCacheEntries(String query, QueryResult result) {
		// TODO Auto-generated method stub
		Set<CacheEntry> set = new HashSet<>();
		 String[] tokens = query.split(",");
//        Set<String> keys = getReferencedKeysFromQuery(query);
        CacheEntry e = null;
        Map<String, String> map = new HashMap<>();
        
//        for (String key: keys) {
//            String keyPrefix = key.split(",")[0];
            switch (tokens[0]) {
            case YCSBConstants.QUERY_USERTABLE:
                UserResult user_res = (UserResult)result;
                map.put("u_id", String.valueOf(user_res.getYcsb_key()));
                map.put("u_field1", user_res.getField_01());
                map.put("u_field2", user_res.getField_02());
                map.put("u_field3", user_res.getField_03());
                map.put("u_field4", user_res.getField_04());
                map.put("u_field5", user_res.getField_05());
                map.put("u_field6", user_res.getField_06());
                map.put("u_field7", user_res.getField_07());
                map.put("u_field8", user_res.getField_08());
                map.put("u_field9", user_res.getField_09());
                map.put("u_field10", user_res.getField_10());
                String key = String.format(YCSBConstants.MEM_UPDATE_USERTABLE_KEY, tokens[1]);
                
                if (map.size() > 0) {
                    e = new CacheEntry(key, map, false);
                }
                
                //if (e != null) set.add(e);
                
                break;
//            case KEY_ACCT_NAME_PREFIX:
//                AccountResult acctResName = (AccountResult)result;
//                e = new CacheEntry(key, acctResName.getCustId(), true);
//                break;
//            case KEY_CHECKING_PREFIX:
//                CheckingResult cRes = (CheckingResult)result;
//                e = new CacheEntry(key, cRes.getBal(), true);
//                break;
//            case KEY_SAVINGS_PREFIX:
//                SavingsResult sRes = (SavingsResult)result;
//                e = new CacheEntry(key, sRes.getBal(), true);
//                break;
            }
//        }

        if (e != null)
            set.add(e);

        return set;
	}

	@Override
	public boolean dmlDataStore(String dml) throws Exception {
		// TODO Auto-generated method stub
		String[] tokens = dml.split(",");
		switch (tokens[0]) {
			case YCSBConstants.UPDATE_QUERY_USERTABLE:
				String[] value = new String[10];
				value[0] = tokens[2];
				value[1] = tokens[3];
				value[2] = tokens[4];
				value[3] = tokens[5];
				value[4] = tokens[6];
				value[5] = tokens[7];
				value[6] = tokens[8];
				value[7] = tokens[9];
				value[8] = tokens[10];
				value[9] = tokens[11];

				int result = updateRecord.run(conn, Integer.parseInt(tokens[1]), value );
				if (result == 0)
	                throw new RuntimeException("Error!! Cannot update ycsb_id ="+tokens[1]);
	            return true;
			case YCSBConstants.INSERT_QUERY_USERTABLE:
				value = new String[10];
				value[0] = tokens[2];
				value[1] = tokens[3];
				value[2] = tokens[4];
				value[3] = tokens[5];
				value[4] = tokens[6];
				value[5] = tokens[7];
				value[6] = tokens[8];
				value[7] = tokens[9];
				value[8] = tokens[10];
				value[9] = tokens[11];

				result =  insertRecord.run(conn, Integer.parseInt(tokens[1]), value );
				if (result == 0)
	                throw new RuntimeException("Error!! Cannot insert ycsb_id ="+tokens[1]);
	            return true;
			case YCSBConstants.DELETE_QUERY_USERTABLE:
				result =  deleteRecord.run(conn, Integer.parseInt(tokens[1]));
				if (result == 0)
	                throw new RuntimeException("Error!! Cannot delete ycsb_id ="+tokens[1]);
	            return true;
		}
		return false;
	}

	@Override
	public CacheEntry applyDelta(Delta delta, CacheEntry cacheEntry) {
		// TODO Auto-generated method stub
		 //throw new NotImplementedException("apply delta not implemented");
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
		//return null;
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
//		 if (obj instanceof String) {
//	            return new CacheEntry(key, obj, true);
//	        } else if (obj instanceof byte[] ){
//	            return new CacheEntry(key, new String((byte[]) obj), true);
//	        }
		byte[] bytes = (byte[]) obj;
        
        int offset = 0;
        ByteBuffer buff = ByteBuffer.wrap(bytes);
		
	        if(key.startsWith(YCSBConstants.MEM_UPDATE_USERTABLE)) {
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
//		throw new UserAbortException("serialize changes error");
	}

	@Override
	public int getHashCode(String key) {
		// TODO Auto-generated method stub
		//return Integer.parseInt(key.split(",")[1]);
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
	    public Map<Interval1D, List<Delta>> updatePoints(String dml) {
	        String[] tokens = dml.split(",");
	        
	        throw new NotImplementedException("update points in cachestore");
	    }

}
