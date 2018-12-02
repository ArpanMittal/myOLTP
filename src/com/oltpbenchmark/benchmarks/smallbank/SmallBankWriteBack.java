package com.oltpbenchmark.benchmarks.smallbank;

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
import com.oltpbenchmark.jdbc.AutoIncrementPreparedStatement;
import com.oltpbenchmark.types.DatabaseType;
import com.usc.dblab.cafe.Change;
import com.usc.dblab.cafe.Config;
import com.usc.dblab.cafe.Stats;
import com.usc.dblab.cafe.WriteBack;

import edu.usc.dblab.intervaltree.Interval1D;

import com.usc.dblab.cafe.QueryResult;
import com.usc.dblab.cafe.Session;

public class SmallBankWriteBack extends WriteBack {
    public static final String DATA_ITEM_CHECKING = SmallBankConstants.TABLENAME_CHECKING+",%s";
    public static final String DATA_ITEM_SAVINGS = SmallBankConstants.TABLENAME_SAVINGS+",%s";
    public final SQLStmt stmtInsertSessionIds = new SQLStmt("INSERT INTO COMMITED_SESSION VALUES (?)");
    public final SQLStmt stmtDeleteSessionIds = new SQLStmt("DELETE FROM COMMITED_SESSION WHERE sessid=?");
    private final Connection conn;

    private DatabaseType dbType;
    private Map<String, SQLStmt> name_stmt_xref;
    private final Map<SQLStmt, String> stmt_name_xref = new HashMap<SQLStmt, String>();
    private final Map<SQLStmt, PreparedStatement> prepardStatements = new HashMap<SQLStmt, PreparedStatement>();
    private Statement stmt;
    public final SQLStmt UpdateCheckingBalance = new SQLStmt(
            "UPDATE " + SmallBankConstants.TABLENAME_CHECKING + 
            "   SET bal = bal + ? " +
            " WHERE custid = ?"
            );

    public final SQLStmt UpdateSavingsBalance = new SQLStmt(
            "UPDATE " + SmallBankConstants.TABLENAME_SAVINGS + 
            "   SET bal = bal + ? " +
            " WHERE custid = ?"
            );

    public final SQLStmt SetCheckingBalance = new SQLStmt(
            "UPDATE " + SmallBankConstants.TABLENAME_CHECKING + 
            "   SET bal = ? " +
            " WHERE custid = ?"
            );

    public final SQLStmt SetSavingsBalance = new SQLStmt(
            "UPDATE " + SmallBankConstants.TABLENAME_SAVINGS + 
            "   SET bal = ? " +
            " WHERE custid = ?"
            );

    public SmallBankWriteBack(Connection conn) {
        this.conn = conn;
        try {
            this.stmt = conn.createStatement();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
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
    //For rangeqc only
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
    public boolean applyBufferedWrite(String arg0, Object arg1) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public LinkedHashMap<String, Change> bufferChanges(String dml, Set<String> buffKeys) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object convertToIdempotent(Object arg0) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Change deserialize(byte[] bytes) {
        return new Change(Change.TYPE_APPEND, new String(bytes));
    }

    @Override
    public Set<String> getMapping(String arg0) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isIdempotent(Object buffValue) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Set<String> rationalizeRead(String query) {
        String[] tokens = query.split(",");
        Set<String> set = new HashSet<>();
        switch (tokens[0]) {
        case SmallBankConstants.QUERY_CHECKING_PREFIX:
            set.add(String.format(DATA_ITEM_CHECKING, tokens[1]));
            break;
        case SmallBankConstants.QUERY_SAVINGS_PREFIX:
            set.add(String.format(DATA_ITEM_SAVINGS, tokens[1]));
            break;
        }
        return set;
    }

    @Override
    public LinkedHashMap<String, Change> rationalizeWrite(String dml) {
        String[] tokens = dml.split(",");
        LinkedHashMap<String, Change> changes = new LinkedHashMap<>();

        String obj = null;
        String amount = tokens[3];

        switch (tokens[2]) {
        case "incr":
            obj = "+"+amount;
            break;
        case "decr":
            obj = "-"+amount;
            break;
        case "set":
            obj = "="+amount;
            break;
        }

        if (tokens[0].equals(SmallBankConstants.UPDATE_CHECKING_PREFIX)) {
            changes.put(String.format(DATA_ITEM_CHECKING, tokens[1]), new Change(Change.TYPE_APPEND, obj));
        } else if (tokens[0].equals(SmallBankConstants.UPDATE_SAVINGS_PREFIX)) {
            changes.put(String.format(DATA_ITEM_SAVINGS, tokens[1]), new Change(Change.TYPE_APPEND, obj));
        }

        return changes;
    }

    @Override
    public byte[] serialize(Change change) {
        if (change.getType() != Change.TYPE_APPEND) {
            throw new NotImplementedException("Should an append.");
        }
        return ((String)change.getValue()).getBytes();
    }

    @Override
    public boolean applySessions(List<Session> sessions, Connection conn, 
            Statement stmt, PrintWriter sessionWriter, Stats stats) throws Exception {
        Map<String, String> mergeMap = new HashMap<>();

        for (Session sess: sessions) {
            List<String> its = sess.getIdentifiers();
            for (int i = 0; i < its.size(); ++i) {
                String identifier = its.get(i);
                Change change = sess.getChange(i);
                String val = mergeMap.get(identifier);
                if (val == null) {
                    mergeMap.put(identifier, (String)change.getValue());
                } else {
                    String cval = (String)change.getValue();
                    char c_op = cval.charAt(0);
                    double c_x = Double.parseDouble(cval.substring(1));
                    char op = val.charAt(0);
                    double x = Double.parseDouble(val.substring(1));
                    switch (c_op) {
                    case '=':
                        val = cval;
                        break;
                    case '-': c_x = -c_x;
                    case '+':
                        if (op == '-') x = -x;
                        x = x + c_x;
                        
                        if (op == '=') val = String.format("%c%.2f", op, x);
                        else if (x >= 0) val = String.format("+%.2f", x);
                        else val = String.format("-%.2f", x);
                        break;
                    }
                    
                    mergeMap.put(identifier, val);
                }
            }
        }

        PreparedStatement prepStmt = null;
        for (String it: mergeMap.keySet()) {
            String val = mergeMap.get(it);
            char op = val.charAt(0);
            double amount = Double.parseDouble(val.substring(1));

            String[] tokens = it.split(",");
            String table = tokens[0];
            long custId = Long.parseLong(tokens[1]);
            switch (table) {
            case SmallBankConstants.TABLENAME_CHECKING:
                switch (op) {
                case '+':
                    prepStmt = this.getPreparedStatement(conn, UpdateCheckingBalance, amount, custId);
                    break;
                case '-':
                    prepStmt = this.getPreparedStatement(conn, UpdateCheckingBalance, amount*-1d, custId);
                    break;
                case '=':
                    prepStmt = this.getPreparedStatement(conn, SetCheckingBalance, amount, custId);
                    break;
                }
                break;
            case SmallBankConstants.TABLENAME_SAVINGS:
                switch (op) {
                case '+':
                    prepStmt = this.getPreparedStatement(conn, UpdateSavingsBalance, amount, custId);
                    break;
                case '-':
                    prepStmt = this.getPreparedStatement(conn, UpdateSavingsBalance, amount*-1d, custId);
                    break;
                case '=':
                    prepStmt = this.getPreparedStatement(conn, SetSavingsBalance, amount, custId);
                    break;
                }
                break;
            }

            if (stmt != null) {
                int status = prepStmt.executeUpdate();
                assert(status == 1);
            }
        }
        conn.commit();

        return true;
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
    public QueryResult merge(String query, QueryResult result,
            LinkedHashMap<String, List<Change>> buffVals) {
    	System.out.println("hello inside merge");
    	if (buffVals == null || buffVals.size() == 0)
    		return result;
//    	
        return result;
    }

    @Override
    public byte[] serializeSessionChanges(Map<String, List<Change>> changesMap) {
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
        int hc = sessId.hashCode();
        if (hc < 0) {
            hc = -hc;
        }
        return hc % Config.NUM_PENDING_WRITES_LOGS;
    }

    @Override
    public Map<String, List<Change>> deserializeSessionChanges(byte[] bytes) {
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
