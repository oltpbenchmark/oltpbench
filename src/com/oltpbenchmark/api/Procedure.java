package com.oltpbenchmark.api;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

public abstract class Procedure {
    private static final Logger LOG = Logger.getLogger(Procedure.class);

    private final String procName;
    private Map<String, SQLStmt> name_stmt_xref;
    private final Map<SQLStmt, String> stmt_name_xref = new HashMap<SQLStmt, String>();
    private final Map<SQLStmt, PreparedStatement> prepardStatements = new HashMap<SQLStmt, PreparedStatement>();
    
    /**
     * Constructor
     */
    protected Procedure() {
        this.procName = this.getClass().getSimpleName();
    }
    
    /**
     * Initialize all of the SQLStmt handles. This must be called separately from
     * the constructor, otherwise we can't get access to all of our SQLStmts.
     * @param <T>
     * @return
     */
    @SuppressWarnings("unchecked")
    protected final <T extends Procedure> T initialize() {
        this.name_stmt_xref = Procedure.getStatments(this);
        for (Entry<String, SQLStmt> e : this.name_stmt_xref.entrySet()) {
            this.stmt_name_xref.put(e.getValue(), e.getKey());
        } // FOR
        if (LOG.isDebugEnabled())
            LOG.debug(String.format("Initialized %s with %d SQLStmts: %s",
                                    this, this.name_stmt_xref.size(), this.name_stmt_xref.keySet()));
        return ((T)this);
    }
    
    /**
     * Return the name of this Procedure
     * @return
     */
    protected final String getProcedureName() {
        return (this.procName);
    }
    
    /**
     * Return a PreparedStatement for the given SQLStmt handle
     * The underlying Procedure API will make sure that the proper SQL
     * for the target DBMS is used for this SQLStmt. 
     * @param conn
     * @param stmt
     * @param returnGeneratedKeys 
     * @return
     * @throws SQLException
     */
    public final PreparedStatement getPreparedStatement(Connection conn, SQLStmt stmt, Object...params) throws SQLException {
        PreparedStatement pStmt = this.getPreparedStatementReturnKeys(conn, stmt, null);
        for (int i = 0; i < params.length; i++) {
            pStmt.setObject(i+1, params);
        } // FOR
        return (pStmt);
    }
    
    /**
     * Return a PreparedStatement for the given SQLStmt handle
     * The underlying Procedure API will make sure that the proper SQL
     * for the target DBMS is used for this SQLStmt. 
     * @param conn
     * @param stmt
     * @param returnGeneratedKeys 
     * @return
     * @throws SQLException
     */
    public final PreparedStatement getPreparedStatementReturnKeys(Connection conn, SQLStmt stmt, Integer returnGeneratedKeys) throws SQLException {
        assert(this.name_stmt_xref != null) : "The Procedure " + this + " has not been initialized yet!";
        PreparedStatement pStmt = this.prepardStatements.get(stmt);
        if (pStmt == null) {
            assert(this.stmt_name_xref.containsKey(stmt)) :
                "Unexpected SQLStmt handle in " + this.getClass().getSimpleName() + "\n" + this.name_stmt_xref;
            String sql = stmt.getSQL();
            pStmt = (returnGeneratedKeys != null ? conn.prepareStatement(sql, returnGeneratedKeys) :
                                                   conn.prepareStatement(sql));
            this.prepardStatements.put(stmt, pStmt);
        }
        assert(pStmt != null) : "Unexpected null PreparedStatement for " + stmt;
        return (pStmt);
    }
    /**
     * Initialize all the PreparedStatements needed by this Procedure
     * @param conn
     * @throws SQLException
     */
    protected final void generateAllPreparedStatements(Connection conn) {
        for (Entry<String, SQLStmt> e : this.name_stmt_xref.entrySet()) { 
            SQLStmt stmt = e.getValue();
            try {
                this.getPreparedStatement(conn, stmt);
            } catch (Throwable ex) {
                throw new RuntimeException(String.format("Failed to generate PreparedStatements for %s.%s", this, e.getKey()), ex);
            }
        } // FOR
    }
    
    /**
     * Fetch the SQL from the dialect map
     * @param dialectMap 
     */
    protected final void loadSQLDialect(StatementDialects dialects) {
        assert(this.name_stmt_xref != null) :
            "Trying to access Procedure " + this.procName + " before it is initialized!";
        Collection<String> stmtNames = dialects.getStatementNames(this.procName);
        if (stmtNames == null) return;
        assert(this.name_stmt_xref.isEmpty() == false) :
            "There are no SQLStmts for Procedure " + this.procName + "?";
        for (String stmtName : stmtNames) {
            assert(this.name_stmt_xref.containsKey(stmtName)) :
                String.format("Unexpected Statement %s in dialects for Procedure %s\n%s",
                              stmtName, this.procName, this.stmt_name_xref.keySet());
			String sql = dialects.getSQL(this.procName, stmtName);
			assert(sql != null);
			
			SQLStmt stmt = this.name_stmt_xref.get(stmtName);
	        assert(stmt != null) :
	            String.format("Unexpected null SQLStmt handle for %s.%s",
	                          this.procName, stmtName);
	        if (LOG.isDebugEnabled())
	            LOG.debug(String.format("Setting %s SQL dialect for %s.%s",
	                                    dialects.getDatabaseType(), this.procName, stmtName));
	        stmt.setSQL(sql);
		} // FOR (stmt)
    }
    
    /**
     * Hook for testing
     * @return
     */
    protected final Map<String, SQLStmt> getStatments() {
        assert(this.name_stmt_xref != null) :
            "Trying to access Procedure " + this.procName + " before it is initialized!";
        return (Collections.unmodifiableMap(this.name_stmt_xref));
    }
    
    /**
     * Hook for testing to retrieve a SQLStmt based on its name
     * @param stmtName
     * @return
     */
    protected final SQLStmt getStatment(String stmtName) {
        assert(this.name_stmt_xref != null) :
            "Trying to access Procedure " + this.procName + " before it is initialized!";
        return (this.name_stmt_xref.get(stmtName));
    }
    
    protected static Map<String, SQLStmt> getStatments(Procedure proc) {
        Class<? extends Procedure> c = proc.getClass();
        Map<String, SQLStmt> stmts = new HashMap<String, SQLStmt>();
        for (Field f : c.getDeclaredFields()) {
            int modifiers = f.getModifiers();
            if (Modifier.isTransient(modifiers) == false &&
                Modifier.isPublic(modifiers) == true &&
                Modifier.isStatic(modifiers) == false) {
                try {
                    Object o = f.get(proc);
                    if (o instanceof SQLStmt) {
                        stmts.put(f.getName(), (SQLStmt)o);
                    }
                } catch (Exception ex) {
                    throw new RuntimeException("Failed to retrieve " + f + " from " + c.getSimpleName(), ex);
                }
            }
        } // FOR
        return (stmts);
    }
    
    @Override
    public String toString() {
        return (this.procName);
    }
    
    /**
     * Thrown from a Procedure to indicate to the Worker
     * that the procedure should be aborted and rolled back.
     */
    public static class UserAbortException extends RuntimeException {
        private static final long serialVersionUID = -1L;

        /**
         * Default Constructor
         * @param msg
         * @param ex
         */
        public UserAbortException(String msg, Throwable ex) {
            super(msg, ex);
        }
        
        /**
         * Constructs a new UserAbortException
         * with the specified detail message.
         */
        public UserAbortException(String msg) {
            this(msg, null);
        }
    } // END CLASS    
}
