package com.oltpbenchmark.benchmarks.ycsb;




import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
import com.oltpbenchmark.benchmarks.smallbank.SmallBankConstants;
import com.oltpbenchmark.benchmarks.ycsb.procedures.UpdateRecord;
import com.oltpbenchmark.benchmarks.ycsb.results.UserResult;
import com.oltpbenchmark.jdbc.AutoIncrementPreparedStatement;
import com.oltpbenchmark.types.DatabaseType;
import com.usc.dblab.cafe.Change;
import com.usc.dblab.cafe.QueryResult;
import com.usc.dblab.cafe.Session;
import com.usc.dblab.cafe.Stats;
import com.usc.dblab.cafe.WriteBack;

import edu.usc.dblab.intervaltree.Interval1D;

import static com.oltpbenchmark.benchmarks.tpcc.TPCCConfig.DML_DELETE_NEW_ORDER_PRE;
import static com.oltpbenchmark.benchmarks.tpcc.TPCCConstants.DATA_ITEM_DISTRICT;
import static com.oltpbenchmark.benchmarks.tpcc.TPCCConstants.DATA_ITEM_NEW_ORDER;
import static com.oltpbenchmark.benchmarks.ycsb.YCSBConstants.QUERY_USERTABLE;;

public class YCSBWriteBack extends WriteBack {
	
	public final SQLStmt updateAllStmt = new SQLStmt(
	        "UPDATE USERTABLE SET FIELD1=?,FIELD2=?,FIELD3=?,FIELD4=?,FIELD5=?," +
	        "FIELD6=?,FIELD7=?,FIELD8=?,FIELD9=?,FIELD10=? WHERE YCSB_KEY=?"
	    );
	public static final String DATA_ITEM_USER = "USER_TABLE"+",%s";
	public final SQLStmt stmtInsertSessionIds = new SQLStmt("INSERT INTO COMMITED_SESSION VALUES (?)");
    public final SQLStmt stmtDeleteSessionIds = new SQLStmt("DELETE FROM COMMITED_SESSION WHERE sessid=?");
    private static final String INSERT = "I";
    private static final String SET = "S";
    private static final String INCR = "A";
    private static final String DELETE = "D";
    final static UpdateRecord updateRecord = new UpdateRecord();
    private Statement stmt;
	private final Connection conn;

   	/**
	 * @param conn
	 */
	public YCSBWriteBack(Connection conn) {
		
		this.conn = conn;
		try {
            this.stmt = conn.createStatement();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
	}
	
	

	private DatabaseType dbType;
	private Map<String, SQLStmt> name_stmt_xref;
	private final Map<SQLStmt, String> stmt_name_xref = new HashMap<SQLStmt, String>();
	private final Map<SQLStmt, PreparedStatement> prepardStatements = new HashMap<SQLStmt, PreparedStatement>();
	
	@Override
	public Set<String> getMapping(String key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public LinkedHashMap<String, Change> bufferChanges(String dml, Set<String> buffKeys) {
		// TODO Auto-generated method stub
		throw new NotImplementedException("bufferchanges");
		//return null;
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
        // NewOrder
        case QUERY_USERTABLE:
            set.add(String.format(YCSBConstants.WB_UPDATE_USERTABLE_KEY, tokens[1]));
            break;
        
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
        	case YCSBConstants.UPDATE_QUERY_USERTABLE:
        		String s = String.format("%s,o_field1,%s;%s,o_field2,%s;%s,o_field3,%s;%s,o_field4,%s;%s,o_field5,%s;%s,o_field6,%s;%s,o_field7,%s;%s,o_field8,%s;%s,o_field9,%s;%s,o_field10,%s", SET, tokens[2], SET, tokens[3],SET, tokens[4],SET, tokens[5], SET, tokens[6], SET, tokens[7], SET, tokens[8],SET, tokens[9], SET, tokens[10], SET, tokens[11]);
        		c = new Change(Change.TYPE_SET,s);
                it = String.format(YCSBConstants.WB_UPDATE_USERTABLE_KEY, tokens[1]);
                break;
        	case YCSBConstants.INSERT_QUERY_USERTABLE:
        		String s1 = String.format(INSERT+"o_field1,%s;o_field2,%s;o_field3,%s;o_field4,%s;o_field5,%s;o_field6,%s;o_field7,%s;o_field8,%s;o_field9,%s;o_field10,%s", SET, tokens[2], SET, tokens[3],SET, tokens[4],SET, tokens[5], SET, tokens[6], SET, tokens[7], SET, tokens[8],SET, tokens[9], SET, tokens[10], SET, tokens[11]);
        		c = new Change(Change.TYPE_RMW,s1);
                it = String.format(YCSBConstants.WB_UPDATE_USERTABLE_KEY, tokens[1]);
                break;
        	case YCSBConstants.DELETE_QUERY_USERTABLE:
        		//String s2 = String.format(DELETE+"o_field1,%s;o_field2,%s;o_field3,%s;o_field4,%s;o_field5,%s;o_field6,%s;o_field7,%s;o_field8,%s;o_field9,%s;o_field10,%s", SET, tokens[2], SET, tokens[3],SET, tokens[4],SET, tokens[5], SET, tokens[6], SET, tokens[7], SET, tokens[8],SET, tokens[9], SET, tokens[10], SET, tokens[11]);
        		String s2 = String.format(DELETE+"o_field1,%s;o_field2,%s;o_field3,%s;o_field4,%s;o_field5,%s;o_field6,%s;o_field7,%s;o_field8,%s;o_field9,%s;o_field10,%s", SET, null, SET, null,SET, null,SET, null, SET, null, SET, null, SET, null,SET, null, SET, null, SET, null);
                c = new Change(Change.TYPE_RMW,s2);
                it = String.format(YCSBConstants.WB_UPDATE_USERTABLE_KEY, tokens[1]);
                break;
        }
        if (c != null) 
        	map.put(it, c);
        return map;
//		return null;
	}

	@Override
	public byte[] serialize(Change change) {
		// TODO Auto-generated method stub
//		 if (change.getType() != Change.TYPE_APPEND || change.getType() != Change.TYPE_RMW)
//	            throw new NotImplementedException("Should not have change of type different than RMW");
	        int len = 0;
//	        String sid = change.getSid();
//	        len += 4+sid.length();

//	        int sequenceId = change.getSequenceId();
//	        len += 4;

	        String val = (String) change.getValue();
	        len += 4+val.length();

	        ByteBuffer buff = ByteBuffer.allocate(len);
//	        buff.putInt(sid.length());
//	        buff.put(sid.getBytes());
//	        buff.putInt(sequenceId);
	        buff.putInt(val.length());
	        buff.put(val.getBytes());
	        return buff.array();
//		return null;
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
    public Map<Interval1D, String> getImpactedRanges(Session sess) {
//        List<Change> changes = sess.getChanges();
        Map<Interval1D, String> res = new HashMap<>();
//        for (Change c: changes) {
//            String str = (String)c.getValue();
//            if (str.contains("S_QUANTITY")) {
//                String[] fs = str.split(";");
//                for (String f: fs) {
//                    if (f.contains("S_QUANTITY")) {
//                        fs = f.split(",");
//                        int p1 = Integer.parseInt(fs[2]);
//                        res.put(new Interval1D(p1, p1), sess.getSid());
//                        int p2 = Integer.parseInt(fs[3]);
//                        res.put(new Interval1D(p2, p2), sess.getSid());
//                        return res;
//                    }
//                }
//            }
//        }
        
        return res;
    }

	@Override
	public boolean applySessions(List<Session> sessions, Connection conn, Statement stmt, PrintWriter sessionWriter,
			Stats stats) throws Exception {
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
		
		
        PreparedStatement prepStmt = null;
        for (String it: mergeMap.keySet()) {
            String val = mergeMap.get(it);
//            char op = val.charAt(0);
//            double amount = Double.parseDouble(val.substring(1));

            String[] token2 = it.split(",");
            String table = token2[0];
            switch (table) {
            	case YCSBConstants.WB_UPDATE_USERTABLE:{
            		String[] tokens = val.split(",");
            		String[] value = new String[10];
            		//value[0] = token2[1];
    				value[0] = tokens[2].split(";")[0];
    				value[1] = tokens[4].split(";")[0];
    				value[2] = tokens[6].split(";")[0];
    				value[3] = tokens[8].split(";")[0];
    				value[4] = tokens[10].split(";")[0];
    				value[5] = tokens[12].split(";")[0];
    				value[6] = tokens[14].split(";")[0];
    				value[7] = tokens[16].split(";")[0];
    				value[8] = tokens[18].split(";")[0];
    				value[9] = tokens[20];

    				int result = updateRecord.run(conn, Integer.parseInt(token2[1]), value );
    				if (result == 0)
    	                throw new RuntimeException("Error!! Cannot update ycsb_id ="+tokens[1]);
    				break;
    	            //return true;
            	}
            		
            }
//            System.out.println("apply sessions completed");
        }
        conn.commit();

        return true;
		
		
		
		//throw new NotImplementedException("applySessions");
		// TODO Auto-generated method stub
		//return false;
	}

	@Override
	public QueryResult merge(String query, QueryResult result, LinkedHashMap<String, List<Change>> buffVals) {
		// TODO Auto-generated method stub
//		System.out.println("Inside merge");
		if (buffVals == null || buffVals.size() == 0) 
			return result;
		
		
			String tokens[] = query.split(",");
        
        // since each query impacts only one data item, get the change list.
			List<Change> changes = buffVals.values().iterator().next();
			switch (tokens[0]) {
				case YCSBConstants.QUERY_USERTABLE:
					UserResult userResult = (UserResult)result;
					  for (Change c: changes) {
			                String val = (String)c.getValue();                
			                String[] fs = val.split(";");
			              
			                for (String f: fs) {
			                    tokens = f.split(",");
			                    switch (tokens[0]) {
			                    case SET:
			                        if (tokens[1].equals("o_field1")) {
			                        	userResult.setField_01(tokens[2]);
			                        } else if (tokens[1].equals("o_field2")) {
			                        	userResult.setField_02(tokens[2]);
			                        }else if (tokens[1].equals("o_field3")) {
			                        	userResult.setField_03(tokens[2]);
			                        }else if (tokens[1].equals("o_field4")) {
			                        	userResult.setField_04(tokens[2]);
			                        }else if (tokens[1].equals("o_field5")) {
			                        	userResult.setField_05(tokens[2]);
			                        }else if (tokens[1].equals("o_field5")) {
			                        	userResult.setField_05(tokens[2]);
			                        }else if (tokens[1].equals("o_field6")) {
			                        	userResult.setField_06(tokens[2]);
			                        }else if (tokens[1].equals("o_field7")) {
			                        	userResult.setField_07(tokens[2]);
			                        }else if (tokens[1].equals("o_field8")) {
			                        	userResult.setField_08(tokens[2]);
			                        }else if (tokens[1].equals("o_field9")) {
			                        	userResult.setField_09(tokens[2]);
			                        }else if (tokens[1].equals("o_field10")) {
			                        	userResult.setField_10(tokens[2]);
			                        }
			                        break;
			                    case INSERT:
			                    	if (tokens[1].equals("o_field1")) {
			                        	userResult.setField_01(tokens[2]);
			                        } else if (tokens[1].equals("o_field2")) {
			                        	userResult.setField_02(tokens[2]);
			                        }else if (tokens[1].equals("o_field3")) {
			                        	userResult.setField_03(tokens[2]);
			                        }else if (tokens[1].equals("o_field4")) {
			                        	userResult.setField_04(tokens[2]);
			                        }else if (tokens[1].equals("o_field5")) {
			                        	userResult.setField_05(tokens[2]);
			                        }else if (tokens[1].equals("o_field5")) {
			                        	userResult.setField_05(tokens[2]);
			                        }else if (tokens[1].equals("o_field6")) {
			                        	userResult.setField_06(tokens[2]);
			                        }else if (tokens[1].equals("o_field7")) {
			                        	userResult.setField_07(tokens[2]);
			                        }else if (tokens[1].equals("o_field8")) {
			                        	userResult.setField_08(tokens[2]);
			                        }else if (tokens[1].equals("o_field9")) {
			                        	userResult.setField_09(tokens[2]);
			                        }else if (tokens[1].equals("o_field10")) {
			                        	userResult.setField_10(tokens[2]);
			                        }
			                    	break;
//			                    	throw new NotImplementedException("implement insert in merge");
			                        
			                    }                    
			                }
					  }
//			                switch (fs[0]) {
//			                	case SET:
//			                		userResult.setField_01(fs[2]);
//			                		userResult.setField_02(fs[4]);
//			                		userResult.setField_03(fs[6]);
//			                		userResult.setField_04(fs[8]);
//			                		userResult.setField_05(fs[10]);
//			                		userResult.setField_06(fs[12]);
//			                		userResult.setField_07(fs[14]);
//			                		userResult.setField_08(fs[16]);
//			                		userResult.setField_09(fs[18]);
//			                		userResult.setField_10(fs[20]);
//			                	break;
//			                	case INSERT:
//			                		throw new NotImplementedException("implement insert in merge");
//			                }
//					  }
			}
			return result;
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
	
	@Override
    public boolean createSessionTable() {
//        if (com.usc.dblab.cafe.Config.storeCommitedSessions) {
            try {
                Statement statement = conn.createStatement();
                statement.execute("DROP TABLE IF EXISTS "
                        + "COMMITED_SESSION");
                statement.execute("CREATE TABLE IF NOT EXISTS "
                        + "COMMITED_SESSION(sessid VARCHAR(50) NOT NULL PRIMARY KEY)");
                conn.commit();
                statement.close();
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return false;
            }            
//        }
        
        return true;
    }
    
    @Override
    public boolean insertCommitedSessionRows(List<String> sessIds) {
        if (com.usc.dblab.cafe.Config.storeCommitedSessions) {
            try {
                PreparedStatement pStmt = getPreparedStatement(conn, stmtInsertSessionIds);
                for (String sessId: sessIds) {
                    pStmt.setString(1, sessId);
                    pStmt.addBatch();
                }
                pStmt.execute();
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return false;
            }
        }
        
        return true;
    }
    
    private PreparedStatement getPreparedStatement(Connection conn2, SQLStmt stmtInsertSessionIds2) {
		// TODO Auto-generated method stub
    	
		return null;
	}

	@Override
    public boolean cleanupSessionTable(List<String> sessIds) {
        if (com.usc.dblab.cafe.Config.storeCommitedSessions) {
            try {
                PreparedStatement pStmt = getPreparedStatement(conn, stmtDeleteSessionIds);
                for (String sessId: sessIds) {
                    pStmt.setString(1, sessId);
                    pStmt.addBatch();
                }
                pStmt.executeBatch();
                conn.commit();
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return false;
            }
        }
        
        return true;
    }
    
	public final PreparedStatement getPreparedStatement(Connection conn, SQLStmt stmt, Object...params) throws SQLException {
        PreparedStatement pStmt = this.getPreparedStatementReturnKeys(conn, stmt, null);
        for (int i = 0; i < params.length; i++) {
            pStmt.setObject(i+1, params[i]);
        } // FOR
        return (pStmt);
    }

    /**
     * Return a PreparedStatement for the given SQLStmt handle
     * The underlying Procedure API will make sure that the proper SQL
     * for the target DBMS is used for this SQLStmt. 
     * @param conn
     * @param stmt
     * @param is 
     * @return
     * @throws SQLException
     */
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
    public List<String> checkExists(List<String> toExec) {
        if (com.usc.dblab.cafe.Config.storeCommitedSessions) {
            String query = "SELECT sessid FROM COMMITED_SESSION WHERE sessid IN (";
            for (String sessid: toExec) {
                query += "'"+sessid+"',";
            }
            query = query.substring(0, query.length()-1) + ")";
            
            try {
                ResultSet res = stmt.executeQuery(query);
                List<String> exists = new ArrayList<>();
                while (res.next()) {
                    exists.add(res.getString(1));
                }
                res.close();
                return exists;
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return null;
            }
        } else {
            return null;
        }
    }

}
