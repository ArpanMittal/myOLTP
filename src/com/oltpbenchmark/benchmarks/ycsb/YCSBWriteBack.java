package com.oltpbenchmark.benchmarks.ycsb;




import java.io.PrintWriter;
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

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.smallbank.SmallBankConstants;
import com.oltpbenchmark.jdbc.AutoIncrementPreparedStatement;
import com.oltpbenchmark.types.DatabaseType;
import com.usc.dblab.cafe.Change;
import com.usc.dblab.cafe.QueryResult;
import com.usc.dblab.cafe.Session;
import com.usc.dblab.cafe.Stats;
import com.usc.dblab.cafe.WriteBack;

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
        // NewOrder
        case QUERY_USERTABLE:
            set.add(String.format(DATA_ITEM_USER, tokens[1]));
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
		
		
		return null;
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
		return null;
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
