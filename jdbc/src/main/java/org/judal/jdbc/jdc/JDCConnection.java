package org.judal.jdbc.jdc;

/**
 * This file is licensed under the Apache License version 2.0.
 * You may not use this file except in compliance with the license.
 * You may obtain a copy of the License at:
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 */

import java.util.Map;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.Executor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;

import java.sql.*;

import javax.sql.PooledConnection;
import javax.sql.ConnectionEventListener;
import javax.sql.StatementEventListener;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.jdo.datastore.JDOConnection;
import javax.sql.ConnectionEvent;

import com.knowgate.arrayutils.Arr;
import com.knowgate.debug.DebugFile;
import com.knowgate.gis.LatLong;

import org.judal.jdbc.HStore;
import org.judal.jdbc.RDBMS;
import org.judal.metadata.ColumnDef;
import org.judal.transaction.TransactionalResource;


/**
 * <p>JDBC Connection Wrapper</p>
 * This class is a wrapper over java.sql.Connection for connections managed by a pool
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class JDCConnection extends TransactionalResource implements Connection, PooledConnection, JDOConnection  {

	public static final String CURRENT_TIMESTAMP = "CURRENT_TIMESTAMP";
	public static final String DATETIME = "DATETIME";
	public static final String LONGVARBINARY = "LONGVARBINARY";
	public static final String LONGVARCHAR = "LONGVARCHAR";
	public static final String FLOAT_NUMBER = "FLOAT";
	public static final String NUMBER_6 = "SMALLINT";
	public static final String NUMBER_11 = "INTEGER";
	public static final String CHARACTER_VARYING = "VARCHAR";
	public static final String SERIAL = "SERIAL";
	public static final String BLOB = "BLOB";
	public static final String CLOB = "CLOB";

	private LinkedList<ConnectionEventListener> listeners;
	private long timestamp;
	private int dbms;
	private String schema;
	private String name;
	private long thid;

	private Connection conn;	
	private JDCConnectionPool pool;
	private boolean inuse;
	private boolean started;

	private static final String DBMSNAME_MSSQL = RDBMS.MSSQL.toString();
	private static final String DBMSNAME_POSTGRESQL = RDBMS.POSTGRESQL.toString();
	private static final String DBMSNAME_ORACLE = RDBMS.ORACLE.toString();
	private static final String DBMSNAME_MYSQL = RDBMS.MYSQL.toString();
	private static final String DBMSNAME_XBASE = RDBMS.XBASE.toString();
	private static final String DBMSNAME_ACCESS = RDBMS.ACCESS.toString();
	private static final String DBMSNAME_SQLITE = RDBMS.SQLITE.toString();
	private static final String DBMSNAME_HSQLDB = RDBMS.HSQLDB.toString();

	/**
	 * Wrap a connection managed by a pool
	 * @param conn Connection
	 * @param pool JDCConnectionPool
	 * @param schemaname String
	 */
	public JDCConnection(Connection conn, JDCConnectionPool pool, String schemaname) {
		this.dbms = RDBMS.UNKNOWN.intValue();
		this.conn=conn;
		this.pool=pool;
		this.inuse=false;
		this.started=false;
		this.timestamp=0;
		this.name = null;
		this.schema=schemaname;
		this.thid = -1l;
		listeners = new LinkedList<ConnectionEventListener>();
	}

	/**
	 * Wrap a connection managed by a pool
	 * @param conn Connection
	 * @param pool JDCConnectionPool
	 */
	public JDCConnection(Connection conn, JDCConnectionPool pool) {
		this.dbms = RDBMS.UNKNOWN.intValue();
		this.conn=conn;
		this.pool=pool;
		this.inuse=false;
		this.started=false;
		this.timestamp=0;
		this.name = null;
		this.schema=null;
		this.thid = -1l;
		listeners = new LinkedList<ConnectionEventListener>();
	}

	/**
	 * <p>Start this connection as a resource participating in a transaction.</p>
	 * Set autocommit off
	 * @throws XAException
	 */
	@Override
	protected void startResource(int flags) throws XAException {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin JDCConnection.startResource("+String.valueOf(flags)+")");
			if (getThreadId()!=-1l && getThreadId()!=Thread.currentThread().getId())
				throw new XAException("JDCConnection.startResource() JDCConnection " + getId() + " is already in use by thread " + getThreadId());
			DebugFile.incIdent();
			DebugFile.writeln("cid=" + getId().toString());
			DebugFile.writeln("JDCConnection.setAutoCommit(false)");
		}
		try {
			conn.setAutoCommit(false);
		} catch (SQLException sqle) {
			if (DebugFile.trace) DebugFile.decIdent();
			throw new XAException(sqle.getMessage() + "cid=" + getId().toString());
		}
		started = true;
		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End JDCConnection.startResource() : " + getId().toString());
		}
	}

	/**
	 * @return int XAResource.XA_RDONLY if this connection is in read-only mode else XAResource.XA_OK
	 */
	@Override
	protected int prepareResource() throws XAException {
		if (DebugFile.trace) {
			if (getThreadId()!=-1l && getThreadId()!=Thread.currentThread().getId())
				throw new XAException("JDCConnection.prepareResource() JDCConnection " + getId() + " is already in use by thread " + getThreadId());
		}
		try {
			return conn.isReadOnly() ? XAResource.XA_RDONLY : XAResource.XA_OK;
		} catch (SQLException sqle) {
			throw new XAException(sqle.getMessage()+ " cid=" + getId().toString());
		}
	}

	/**
	 * <p>Commit transaction.</p>
	 * @throws XAException if startResource() has not been previously called or connection is not in autocommit=true
	 */
	@Override
	protected void commitResource() throws XAException {
		if (DebugFile.trace) {
			if (getThreadId()!=-1l && getThreadId()!=Thread.currentThread().getId())
				throw new XAException("JDCConnection.commitResource() JDCConnection " + getId() + " is already in use by thread " + getThreadId());
		}
		if (!isStarted())
			throw new XAException("JDCConnection.commitResource() JDCConnection " + getId().toString() + " is not started");
		if (DebugFile.trace) {
			DebugFile.writeln("Begin JDCConnection.commitResource()");
			DebugFile.incIdent();
			DebugFile.writeln("cid=" + getId().toString());
		}
		try {
			commit();
		} catch (SQLException sqle) {
			if (DebugFile.trace) {
				DebugFile.writeln("SQLException " + sqle.getMessage());
				DebugFile.decIdent();
			}
			throw new XAException(sqle.getMessage());
		}
		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End JDCConnection.commitResource() : " + getId().toString());
		}
	}

	/**
	 * <p>Rollback transaction.</p>
	 * @throws XAException if startResource() has not been previously called or connection is not in autocommit=true
	 */
	@Override
	protected void rollbackResource() throws XAException {
		if (DebugFile.trace) {
			if (getThreadId()!=-1l && getThreadId()!=Thread.currentThread().getId())
				throw new XAException("JDCConnection.rollbackResource() JDCConnection " + getId() + " is already in use by thread " + getThreadId());
		}
		if (!started)
			throw new XAException("JDCConnection.rollbackResource() JDCConnection " + getId().toString() + " is not started");
		if (DebugFile.trace) {
			DebugFile.writeln("Begin JDCConnection.rollbackResource()");
			DebugFile.incIdent();
			DebugFile.writeln("cid=" + getId().toString());
		}
		try {
			rollback();
		} catch (SQLException sqle) {
			if (DebugFile.trace) {
				DebugFile.writeln("SQLException " + sqle.getMessage() + "cid=" + getId().toString());
				DebugFile.decIdent();
			}
			throw new XAException(sqle.getMessage());
		}
		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End JDCConnection.rollbackResource() : " + getId().toString());
		}
	}

	/**
	 * <p>End participation of this connection in a transaction.</p>
	 * Set autocommit on
	 * @throws XAException
	 */
	@Override
	protected void endResource(int flag) throws XAException {
		if (!started)
			throw new XAException("JDCConnection.endResource() JDCConnection " + getId().toString() + " is not started");
		if (DebugFile.trace) {
			DebugFile.writeln("Begin JDCConnection.endResource("+String.valueOf(flag)+")");
			if (getThreadId()!=-1l && getThreadId()!=Thread.currentThread().getId())
				throw new XAException("JDCConnection.endResource() JDCConnection " + getId() + " is already in use by thread " + getThreadId());
			DebugFile.incIdent();
			DebugFile.writeln("cid=" + getId().toString());
			try {
				DebugFile.writeln("autocommit = "+String.valueOf(conn.getAutoCommit()));
			} catch (SQLException ignore) { }
		}
		try {
			if (DebugFile.trace) DebugFile.writeln("JDCConnection.setAutoCommit(true)");
			conn.setAutoCommit(true);
		} catch (SQLException sqle) {
			if (DebugFile.trace) DebugFile.decIdent();
			throw new XAException(sqle.getMessage());
		}		
		started = false;
		thid = -1l;
		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End JDCConnection.endResource() : " + getId().toString());
		}
	}

	/**
	 * Each connection may be given a name useful to trace it in case there is a connection leak
	 * @return String Connection name
	 */
	public String getName() {
		return name;
	}

	/**
	 * Each connection may be given a name useful to trace it in case there is a connection leak
	 * @param name String
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @return boolean
	 */
	public boolean inUse() {
		return inuse;
	}	

	/**
	 * @param listener ConnectionEventListener
	 */
	@Override
	public void addConnectionEventListener(ConnectionEventListener listener) {
		listeners.add(listener);
	}

	/**
	 * @param listener ConnectionEventListener
	 */
	@Override
	public void removeConnectionEventListener(ConnectionEventListener listener) {
		listeners.remove(listener);
	} 

	protected void notifyClose() {
		if (listeners.size()>0) {
			Iterator<ConnectionEventListener> oIter = listeners.iterator();
			while (oIter.hasNext()) {
				ConnectionEventListener oCevl = oIter.next();
				oCevl.connectionClosed(new ConnectionEvent(this));
			} // wend
		} // fi        
	} // notifyClose

	/**
	 * This method is not supported and will always throw UnsupportedOperationException
	 * @param listener StatementEventListener
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void addStatementEventListener(StatementEventListener listener) {
		throw new UnsupportedOperationException("JDCConnection.addStatementEventListener() is not implemented");
	}

	/**
	 * This method is not supported and will always throw UnsupportedOperationException
	 * @param listener StatementEventListener
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void removeStatementEventListener(StatementEventListener listener) {
		throw new UnsupportedOperationException("JDCConnection.removeStatementEventListener() is not implemented");
	}

	/**
	 * <p>Set connection as in use.</p>
	 * @param sConnectionName String Connection name
	 * @return <b>true</b> if connection was set to in use, <b>false</b> if connection was already in use.
	 * @throws IllegalStateException If the connection was already leased by another thread and it is still participating in a transaction
	 */
	public boolean lease(String sConnectionName) {
		if (inuse) {
			return false;
		} else {
			final long cthid = Thread.currentThread().getId();
			if (thid!=-1l && thid!=cthid)
				throw new IllegalStateException("JDCConnection " + getId() + " was already leased by another thread " + thid + " and it is participating in a transaction");
			inuse = true;
			thid = cthid;
			name = sConnectionName;
			timestamp = System.currentTimeMillis();
			return true;
		}
	}

	/**
	 * <p>Call Connection.getMetaData()</p>
	 * @return boolean <b>true</b> if call was successful
	 */
	public boolean validate() {
		boolean bValid;

		try {
			conn.getMetaData();
			bValid = true;
		} catch (Exception e) {
			DebugFile.writeln(new Date().toString() + " JDCConnection.validate() " + e.getMessage());
			bValid = false;
		}

		return bValid;
	}

	/**
	 * @return JDCConnectionPool
	 */
	public JDCConnectionPool getPool() {
		return pool;
	}

	/**
	 * Get timestamp in milliseconds of last connection use
	 * @return long
	 */
	public long getLastUse() {
		return timestamp;
	}

	/**
	 * Get RDMBS int code for the underlying database
	 * Recognized RDBMS are: MySQL, PostgreSQL, SQLite, HSQL, Microsoft SQLServerand Oracle
	 * If RDBMS is not recognized then RDBMS.GENERIC.intValue() will be returned
	 * @param conn Connection
	 * @return RDBMS
	 * @throws SQLException
	 */
	public static int getDataBaseProduct(Connection conn) throws SQLException {
		DatabaseMetaData mdat;
		String prod;

		try {
			mdat = conn.getMetaData();
			prod = mdat.getDatabaseProductName();

			if (prod.equals(DBMSNAME_MSSQL))
				return RDBMS.MSSQL.intValue();
			else if (prod.equals(DBMSNAME_POSTGRESQL))
				return RDBMS.POSTGRESQL.intValue();
			else if (prod.equals(DBMSNAME_ORACLE))
				return RDBMS.ORACLE.intValue();
			else if (prod.equals(DBMSNAME_MYSQL))
				return RDBMS.MYSQL.intValue();
			else if (prod.equals(DBMSNAME_SQLITE))
				return RDBMS.SQLITE.intValue();
			else if (prod.equals(DBMSNAME_HSQLDB))
				return RDBMS.HSQLDB.intValue();
			else if (prod.equals(DBMSNAME_ACCESS))
				return RDBMS.ACCESS.intValue();
			else if (prod.equals(DBMSNAME_XBASE))
				return RDBMS.XBASE.intValue();
			else
				return RDBMS.GENERIC.intValue();
		}
		catch (NullPointerException npe) {
			if (DebugFile.trace) DebugFile.writeln("NullPointerException at JDCConnection.getDataBaseProduct()");
			return RDBMS.GENERIC.intValue();
		}
	}

	/**
	 * RDBMS code for connected database
	 * @return int
	 * @throws SQLException
	 */
	public int getDataBaseProduct() throws SQLException {
		if (RDBMS.UNKNOWN.intValue()==dbms)
			dbms = getDataBaseProduct(conn);
		return dbms;
	}

	/**
	 * @return String
	 * @throws SQLException
	 */
	public String getSchemaName() throws SQLException {
		String sname;

		if (null==schema) {
			DatabaseMetaData mdat = conn.getMetaData();
			ResultSet rset = mdat.getSchemas();

			if (rset.next())
				sname = rset.getString(1);
			else
				sname = null;

			rset.close();
		}
		else
			sname = schema;

		return sname;
	}

	/**
	 * @param sname String
	 */
	public void setSchemaName(String sname) {
		schema = sname;
	}

	/**
	 * <p>Close connection.</p>
	 * The behavior of this method depends on whether the connection is pooled and started.
	 * If the connection is not pooled then it will be set as not in used and closed. Listener will be notified of connection closing.
	 * If the connection is pooled and started within a transaction then it will be returned to the pool.
	 * If the connection is pooled and not started then autocommit will be set to off and read-only set to no.
	 * If the connection is pooled then listener will not be notified of closing.
	 */
	@Override
	public void close()  {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin JDCConnection.close()");
			DebugFile.incIdent();
		}

		if (pool==null) {
			try {
				conn.close();
			} catch (SQLException e) { }
			notifyClose();
			inuse = false;
			name = null;
		}
		else {
			if (!started) {
				try { setAutoCommit(true); }
				catch (SQLException sqle) { DebugFile.writeln("SQLException setAutoCommit(true) "+sqle.getMessage()); } 
				try { setReadOnly(false); }
				catch (SQLException sqle) { DebugFile.writeln("SQLException setReadOnly(false) "+sqle.getMessage()); } 
			}
			pool.returnConnection(this);
		}

		if (DebugFile.trace) {
			DebugFile.writeln("JDCConnection " + getId() + " " + (started ? "is" : "is not") + " started and " + (inuse ? "in use" : "not in use"));
			DebugFile.decIdent();
			DebugFile.writeln("End JDCConnection.close() : " + (getId()!=null ? getId().toString() : "no cid"));
		}
	}

	/**
	 * <p>Close connection.</p>
	 * The behavior of this method depends on whether the connection is pooled and started.
	 * If the connection is not pooled then it will be set as not in used and closed. Listener will be notified of connection closing.
	 * If the connection is pooled and started within a transaction then it will be returned to the pool.
	 * If the connection is pooled and not started then autocommit will be set to off and read-only set to no.
	 * If the connection is pooled then listener will not be notified of closing.
	 * @param sCaller String Connection Name
	 * @throws SQLException
	 */
	public void close(String sCaller) throws SQLException {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin JDCConnection.close("+sCaller+")");
			DebugFile.incIdent();
			DebugFile.writeln((getId()==null ? "No cid" : "cid=" + getId()) + " " + (started ? "started" : "non started") + " " + (pool!=null ? "pooled" : "non pooled") + " JDCConnection");
		}
		if (pool==null) {
			inuse = false;
			name = null;
			conn.close();
			notifyClose();
		}
		else {
			if (!started) {
				try { setAutoCommit(true); }
				catch (SQLException sqle) { DebugFile.writeln("SQLException setAutoCommit(true) "+sqle.getMessage()); } 
				try { setReadOnly(false); }
				catch (SQLException sqle) { DebugFile.writeln("SQLException setReadOnly(false) "+sqle.getMessage()); } 
			}
			pool.returnConnection(this, sCaller);
		}

		if (DebugFile.trace) {
			DebugFile.writeln("JDCConnection " + getId() + " " + (started ? "is" : "is not") + " started and " + (inuse ? "in use" : "not in use"));
			DebugFile.decIdent();
			DebugFile.writeln("End JDCConnection.close("+sCaller+") : " + (getId()!=null ? getId().toString() : "no cid"));
		}
	}

	/**
	 * <p>Dispose connection.</p>
	 * If autocommit is off then rollback.
	 * If connection is pooled then return connection to the pool and dispose it from the pool.
	 */
	public void dispose() {
		try { if (!getAutoCommit()) rollback(); } catch (SQLException ignore) { }		
		thid = -1l;
		started = false;
		if (pool!=null) {
			pool.disposeConnection(this);
		}
	}

	/**
	 * <p>Dispose connection.</p>
	 * If autocommit is off then rollback.
	 * If connection is pooled then return connection to the pool and dispose it from the pool.
	 * @param String Connection Name
	 */
	public void dispose(String sCaller) {
		try { if (!getAutoCommit()) rollback(); } catch (SQLException ignore) { }
		if (pool!=null) {
			pool.returnConnection(this, sCaller);
			pool.disposeConnection(this);
		}
	}

	protected void expireLease() {
		if (!isStarted())
			thid = -1l;
		inuse = false;
		name = null ;
	}

	/**
	 * <p>Get wrapped java.sql.Connection.</p>
	 * @return Connection
	 */
	@Override
	public Connection getConnection() {
		return conn;
	}

	/**
	 * <p>Get wrapped java.sql.Connection.</p>
	 * @return Connection
	 */
	@Override
	public Connection getNativeConnection() {
		return conn;		
	}

	/**
	 * <p>Get id of thread for which this connection was started or -1 if connection is not started for any thread.</p>
	 * @return long Thread Id
	 */
	public long getThreadId() {
		return thid;		
	}

	/**
	 * @param c Class
	 * @return boolean <b>true</b> if c is of class java.sql.Connection
	 * @throws NullPointerException if c is <b>null</b>
	 */
	@Override
	public boolean isWrapperFor(Class c) {
		return c.getClass().getName().equals("java.sql.Connection");
	}

	/**
	 * @param c Class
	 * @return If c is java.sql.Connection then the underlying wrapped connection is returned else the return value is <b>null</b>
	 * @throws NullPointerException if c is <b>null</b>
	 */
	@Override
	public Object unwrap(Class c) {
		return c.getClass().getName().equals("java.sql.Connection") ? conn : null;
	}

	/**
	 * @param typeName String
	 * @param attributes Object[]
	 * @return java.sql.Array
	 * @throws SQLException
	 */
	@Override
	public Array createArrayOf(String typeName, Object[] attributes) throws SQLException {
		return conn.createArrayOf(typeName, attributes);
	}

	/**
	 * @return java.sql.Blob
	 * @throws SQLException
	 */
	@Override
	public Blob createBlob() throws SQLException {
		return conn.createBlob();
	}

	/**
	 * @return java.sql.Clob
	 * @throws SQLException
	 */
	@Override
	public Clob createClob() throws SQLException {
		return conn.createClob();
	}

	/**
	 * @return java.sql.NClob
	 * @throws SQLException
	 */
	@Override
	public NClob createNClob() throws SQLException {
		return conn.createNClob();
	}

	/**
	 * @param typeName String
	 * @param attributes Object[]
	 * @return java.sql.Struct
	 * @throws SQLException
	 */
	@Override
	public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
		return conn.createStruct(typeName, attributes);
	}

	/**
	 * This method is not implemented and always throws SQLFeatureNotSupportedException
	 * @throws SQLFeatureNotSupportedException
	 */
	@Override
	public SQLXML createSQLXML() throws SQLException, SQLFeatureNotSupportedException {
		throw new SQLFeatureNotSupportedException("JDCConnection.createSQLXML() not implemented");
	}

	/**
	 * <p>Creates a Statement object that will generate ResultSet objects with the given type and concurrency.</p>
	 * This method is the same as the parameterless createStatement method,
	 * but it allows the default result set type and concurrency to be overridden.
	 * The holdability of the created result sets can be determined by calling getHoldability().
	 * @param resultSetType int one of ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE, or ResultSet.TYPE_SCROLL_SENSITIVE
	 * @param resultSetConcurrency int one of ResultSet.CONCUR_READ_ONLY or ResultSet.CONCUR_UPDATABLE
	 * @return Statement a new Statement object that will generate ResultSet objects with the given type and concurrency
	 * @throws SQLException if a database access error occurs, this method is called on a closed connection or the given parameters are not ResultSet constants indicating type and concurrency
	 */
	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
		return conn.createStatement(resultSetType,resultSetConcurrency);
	}

	/**
	 * <p>Creates a Statement object that will generate ResultSet objects with the given type, concurrency and holdability.</p>
	 * This method is the same as the parameterless createStatement method,
	 * but it allows the default result set type, concurrency and holdability to be overridden.
	 * @param resultSetType int one of ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE, or ResultSet.TYPE_SCROLL_SENSITIVE
	 * @param resultSetConcurrency int one of ResultSet.CONCUR_READ_ONLY or ResultSet.CONCUR_UPDATABLE
	 * @param resultSetHoldability int one of the following ResultSet constants: ResultSet.HOLD_CURSORS_OVER_COMMIT or ResultSet.CLOSE_CURSORS_AT_COMMIT
	 * @return Statement a new Statement object that will generate ResultSet objects with the given type, concurrency and holdability
	 * @throws SQLException if a database access error occurs,
	 * this method is called on a closed connection or the given parameters
	 * are not ResultSet constants indicating type, concurrency and holdability
	 */
	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		return conn.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
	}

	/**
	 * <p>Creates a PreparedStatement object for sending parameterized SQL statements to the database.</p>
	 * A SQL statement with or without IN parameters can be pre-compiled and stored in a PreparedStatement object. This object can then be used to efficiently execute this statement multiple times.
	 * Result sets created using the returned PreparedStatement object will by default be type TYPE_FORWARD_ONLY and have a concurrency level of CONCUR_READ_ONLY. The holdability of the created result sets can be determined by calling getHoldability().
	 * @param sql String an SQL statement that may contain one or more '?' IN parameter placeholders
	 * @return PreparedStatement a new default PreparedStatement object containing the pre-compiled SQL statement
	 * @throws SQLException if a database access error occurs or this method is called on a closed connection
	 */
	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin JDCConnection.prepareStatement()");
			if (getThreadId()!=-1l && getThreadId()!=Thread.currentThread().getId())
				throw new SQLException("JDCConnection.prepareStatement() JDCConnection " + getId() + " is already in use by thread " + getThreadId());
		}
		return conn.prepareStatement(sql);
	}

	/**
	 * <p>Creates a default PreparedStatement object capable of returning the auto-generated keys designated by the given array.</p>
	 * This array contains the names of the columns in the target table that contain the auto-generated keys that should be returned. The driver will ignore the array if the SQL statement is not an INSERT statement, or an SQL statement able to return auto-generated keys (the list of such statements is vendor-specific).
	 * An SQL statement with or without IN parameters can be pre-compiled and stored in a PreparedStatement object. This object can then be used to efficiently execute this statement multiple times.
	 * Result sets created using the returned PreparedStatement object will by default be type TYPE_FORWARD_ONLY and have a concurrency level of CONCUR_READ_ONLY. The holdability of the created result sets can be determined by calling getHoldability().
	 * @param sql String an SQL statement that may contain one or more '?' IN parameter placeholders
	 * @param columnNames String an array of column names indicating the columns that should be returned from the inserted row or rows
	 * @return a new PreparedStatement object, containing the pre-compiled statement, that is capable of returning the auto-generated keys designated by the given array of column names
	 * @throws SQLException if a database access error occurs or this method is called on a closed connection
	 */
	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin JDCConnection.prepareStatement()");
			if (getThreadId()!=-1l && getThreadId()!=Thread.currentThread().getId())
				throw new SQLException("JDCConnection.prepareStatement() JDCConnection " + getId() + " is already in use by thread " + getThreadId());
		}
		return conn.prepareStatement(sql,columnNames);
	}

	/**
	 * <p>Creates a default PreparedStatement object that has the capability to retrieve auto-generated keys.</p>
	 * The given constant tells the driver whether it should make auto-generated keys available for retrieval. This parameter is ignored if the SQL statement is not an INSERT statement, or an SQL statement able to return auto-generated keys (the list of such statements is vendor-specific).
	 * Result sets created using the returned PreparedStatement object will by default be type TYPE_FORWARD_ONLY and have a concurrency level of CONCUR_READ_ONLY. The holdability of the created result sets can be determined by calling getHoldability().
	 * @param sql String an SQL statement that may contain one or more '?' IN parameter placeholders
	 * @param autoGeneratedKeys int a flag indicating whether auto-generated keys should be returned; one of Statement.RETURN_GENERATED_KEYS or Statement.NO_GENERATED_KEYS
	 * @return a new PreparedStatement object, containing the pre-compiled statement, that is capable of returning the auto-generated keys
	 * @throws SQLException if a database access error occurs or this method is called on a closed connection
	 */
	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin JDCConnection.prepareStatement()");
			if (getThreadId()!=-1l && getThreadId()!=Thread.currentThread().getId())
				throw new SQLException("JDCConnection.prepareStatement() JDCConnection " + getId() + " is already in use by thread " + getThreadId());
		}
		return conn.prepareStatement(sql,autoGeneratedKeys);
	}

	/**
	 * <p>Creates a PreparedStatement object that will generate ResultSet objects with the given type and concurrency.</p>
	 * This method is the same as the prepareStatement method above, but it allows the default result set type and concurrency to be overridden. The holdability of the created result sets can be determined by calling getHoldability().
	 * @param sql String an SQL statement that may contain one or more '?' IN parameter placeholders
	 * @param resultSetType int one of ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE, or ResultSet.TYPE_SCROLL_SENSITIVE
	 * @param resultSetConcurrency int one of ResultSet.CONCUR_READ_ONLY or ResultSet.CONCUR_UPDATABLE
	 * @return a new PreparedStatement object containing the pre-compiled SQL statement that will produce ResultSet objects with the given type and concurrency
	 * @throws SQLException if a database access error occurs or this method is called on a closed connection
	 */
	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		return conn.prepareStatement(sql,resultSetType,resultSetConcurrency);
	}

	/**
	 * <p>Creates a PreparedStatement object that will generate ResultSet objects with the given type, concurrency, and holdability.</p>
	 * This method is the same as the prepareStatement without int parameters, but it allows the default result set type, concurrency, and holdability to be overridden.
	 * @param sql String an SQL statement that may contain one or more '?' IN parameter placeholders
	 * @param resultSetType int one of ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE, or ResultSet.TYPE_SCROLL_SENSITIVE
	 * @param resultSetConcurrency int one of ResultSet.CONCUR_READ_ONLY or ResultSet.CONCUR_UPDATABLE
	 * @param resultSetHoldability int one of the following ResultSet constants: ResultSet.HOLD_CURSORS_OVER_COMMIT or ResultSet.CLOSE_CURSORS_AT_COMMIT
	 * @return a new PreparedStatement object containing the pre-compiled SQL statement that will produce ResultSet objects with the given type, concurrency and holdability
	 * @throws SQLException if a database access error occurs or this method is called on a closed connection
	 */
	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin JDCConnection.prepareStatement()");
			if (getThreadId()!=-1l && getThreadId()!=Thread.currentThread().getId())
				throw new SQLException("JDCConnection.prepareStatement() JDCConnection " + getId() + " is already in use by thread " + getThreadId());
		}
		return conn.prepareStatement(sql,resultSetType, resultSetConcurrency, resultSetHoldability);
	}

	/**
	 * <p>Creates a default PreparedStatement object capable of returning the auto-generated keys designated by the given array.</p>
	 * This array contains the indexes of the columns in the target table that contain the auto-generated keys that should be made available. The driver will ignore the array if the SQL statement is not an INSERT statement, or an SQL statement able to return auto-generated keys (the list of such statements is vendor-specific).
	 * Result sets created using the returned PreparedStatement object will by default be type TYPE_FORWARD_ONLY and have a concurrency level of CONCUR_READ_ONLY. The holdability of the created result sets can be determined by calling getHoldability().
	 * @param sql String an SQL statement that may contain one or more '?' IN parameter placeholders
	 * @param columnIndexes int[] an array of column indexes indicating the columns that should be returned from the inserted row or rows
	 * @return a new PreparedStatement object, containing the pre-compiled statement, that is capable of returning the auto-generated keys designated by the given array of column indexes
	 * @throws SQLException if a database access error occurs or this method is called on a closed connection
	 */
	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
		return conn.prepareStatement(sql,columnIndexes);
	}

	/**
	 * <p>Creates a CallableStatement object for calling database stored procedures.</p>
	 * The CallableStatement object provides methods for setting up its IN and OUT parameters, and methods for executing the call to a stored procedure. 
	 * Result sets created using the returned CallableStatement object will by default be type TYPE_FORWARD_ONLY and have a concurrency level of CONCUR_READ_ONLY. The holdability of the created result sets can be determined by calling getHoldability().
	 * @param sql String an SQL statement that may contain one or more '?' parameter placeholders. Typically this statement is specified using JDBC call escape syntax "{ call stored_procedure_name(?,?,?) }"
	 * @return CallableStatement object containing the pre-compiled SQL statement
	 * @throws SQLException if a database access error occurs or this method is called on a closed connection
	 */
	@Override
	public CallableStatement prepareCall(String sql) throws SQLException {
		return conn.prepareCall(sql);
	}

	/**
	 * <p>Creates a CallableStatement object that will generate ResultSet objects with the given type and concurrency.</p>
	 * This method is the same as the parameterless prepareCall method, but it allows the default result set type and concurrency to be overridden. The holdability of the created result sets can be determined by calling getHoldability().
	 * @param sql String an SQL statement that may contain one or more '?' parameter placeholders. Typically this statement is specified using JDBC call escape syntax "{ call stored_procedure_name(?,?,?) }"
	 * @param resultSetType int one of ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE, or ResultSet.TYPE_SCROLL_SENSITIVE
	 * @param resultSetConcurrency one of ResultSet.CONCUR_READ_ONLY or ResultSet.CONCUR_UPDATABLE
	 * @return CallableStatement object containing the pre-compiled SQL statement that will produce ResultSet objects with the given type and concurrency
	 * @throws SQLException if a database access error occurs, this method is called on a closed connection or the given parameters are not ResultSet constants indicating type and concurrency
	 */
	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		return conn.prepareCall(sql, resultSetType, resultSetConcurrency);
	}

	/**
	 * <p>Creates a CallableStatement object that will generate ResultSet objects with the given type, concurrency and holdability.</p>
	 * This method is the same as the parameterless prepareCall method, but it allows the default result set type, concurrency and holdability to be overridden.
	 * @param sql String an SQL statement that may contain one or more '?' parameter placeholders. Typically this statement is specified using JDBC call escape syntax "{ call stored_procedure_name(?,?,?) }"
	 * @param resultSetType int one of ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE, or ResultSet.TYPE_SCROLL_SENSITIVE
	 * @param resultSetConcurrency int one of ResultSet.CONCUR_READ_ONLY or ResultSet.CONCUR_UPDATABLE
	 * @param resultSetHoldability int one of the following ResultSet constants: ResultSet.HOLD_CURSORS_OVER_COMMIT or ResultSet.CLOSE_CURSORS_AT_COMMIT
	 * @return CallableStatement object containing the pre-compiled SQL statement that will produce ResultSet objects with the given type, concurrency and holdability
	 * @throws SQLException if a database access error occurs, this method is called on a closed connection or the given parameters are not ResultSet constants indicating type, concurrency and holdability
	 */
	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		return conn.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
	}

	/**
	 * <p>Creates a Statement object for sending SQL statements to the database.</p>
	 * SQL statements without parameters are normally executed using Statement objects. If the same SQL statement is executed many times, it may be more efficient to use a PreparedStatement object.
	 * Result sets created using the returned Statement object will by default be type TYPE_FORWARD_ONLY and have a concurrency level of CONCUR_READ_ONLY.
	 * The holdability of the created result sets can be determined by calling getHoldability().
	 * @return Statement 
	 * @throws SQLException if a database access error occurs or this method is called on a closed connection
	 */
	@Override
	public Statement createStatement() throws SQLException {
		return conn.createStatement();
	}

	/**
	 * <p>Converts the given SQL statement into the system's native SQL grammar.</p>
	 * A driver may convert the JDBC SQL grammar into its system's native SQL grammar prior to sending it. This method returns the native form of the statement that the driver would have sent.
	 * @param sql String an SQL statement that may contain one or more '?' parameter placeholders
	 * @return String native form of the statement
	 * @throws SQLException if a database access error occurs or this method is called on a closed connection
	 */
	@Override
	public String nativeSQL(String sql) throws SQLException {
		return conn.nativeSQL(sql);
	}

	/**
	 * <p>Returns a list containing the name and current value of each client info property supported by the driver.</p>
	 * The value of a client info property may be null if the property has not been set and does not have a default value. 
	 * @return A Properties object that contains the name and current value of each of the client info properties supported by the driver. 
	 * @throws SQLException if a database access error occurs or this method is called on a closed connection
	 */
	@Override
	public java.util.Properties getClientInfo() throws SQLException {
		return conn.getClientInfo();
	}

	/**
	 * <p>Sets the value of the connection's client info properties.</p>
	 * The Properties object contains the names and values of the client info properties to be set. The set of client info properties contained in the properties list replaces the current set of client info properties on the connection. If a property that is currently set on the connection is not present in the properties list, that property is cleared. Specifying an empty properties list will clear all of the properties on the connection. See setClientInfo (String, String) for more information.
	 * If an error occurs in setting any of the client info properties, a SQLClientInfoException is thrown. The SQLClientInfoException contains information indicating which client info properties were not set. The state of the client information is unknown because some databases do not allow multiple client info properties to be set atomically. For those databases, one or more properties may have been set before the error occurred.
	 * @param props Properties the list of client info properties to set 
	 * @throws SQLClientInfoException f the database server returns an error while setting the clientInfo values on the database server or this method is called on a closed connection
	 */
	@Override
	public void setClientInfo(java.util.Properties props) throws SQLClientInfoException {
		conn.setClientInfo(props);
	}

	/**
	 * <p>Returns the value of the client info property specified by name.</p>
	 * This method may return null if the specified client info property has not been set and does not have a default value. This method will also return null if the specified client info property name is not supported by the driver.
	 * Applications may use the DatabaseMetaData.getClientInfoProperties method to determine the client info properties supported by the driver.
	 * @param name String The name of the client info property to retrieve
	 * @return The value of the client info property specified
	 * @throws SQLException  if the database server returns an error when fetching the client info value from the database or this method is called on a closed connection
	 */
	@Override
	public String getClientInfo(String name) throws SQLException {
		return conn.getClientInfo(name);
	}

	/**
	 * <p>Sets the value of the client info property specified by name to the value specified by value.</p>
	 *  Applications may use the DatabaseMetaData.getClientInfoProperties method to determine the client info properties supported by the driver and the maximum length that may be specified for each property.
	 *  The driver stores the value specified in a suitable location in the database. For example in a special register, session parameter, or system table column. For efficiency the driver may defer setting the value in the database until the next time a statement is executed or prepared. Other than storing the client information in the appropriate place in the database, these methods shall not alter the behavior of the connection in anyway. The values supplied to these methods are used for accounting, diagnostics and debugging purposes only.
	 *  The driver shall generate a warning if the client info name specified is not recognized by the driver.
	 *  If the value specified to this method is greater than the maximum length for the property the driver may either truncate the value and generate a warning or generate a SQLClientInfoException. If the driver generates a SQLClientInfoException, the value specified was not set on the connection.
	 *  The following are standard client info properties. Drivers are not required to support these properties however if the driver supports a client info property that can be described by one of the standard properties, the standard property name should be used.
	 *  <ul>
	 *  <li>ApplicationName - The name of the application currently utilizing the connection.</li>
	 *  <li>ClientUser - The name of the user that the application using the connection is performing work for. This may not be the same as the user name that was used in establishing the connection.</li>
	 *  <li>ClientHostname - The hostname of the computer the application using the connection is running on.</li>
	 *  </ul>
	 *  @param name String The name of the client info property to set
	 *  @param value String The value to set the client info property to. If the value is null, the current value of the specified property is cleared.
	 * @throws SQLClientInfoException if the database server returns an error while setting the client info value on the database server or this method is called on a closed connection 
	 *  
	 */
	@Override
	public void setClientInfo(String name, String value) throws SQLClientInfoException {
		conn.setClientInfo(name, value);
	}

	/**
	 * <p>Sets this connection's auto-commit mode to the given state.</p>
	 * If a connection is in auto-commit mode, then all its SQL statements will be executed and committed as individual transactions. Otherwise, its SQL statements are grouped into transactions that are terminated by a call to either the method commit or the method rollback. By default, new connections are in auto-commit mode.
	 * The commit occurs when the statement completes. The time when the statement completes depends on the type of SQL Statement:
	 * <ul>
	 * <li>For DML statements, such as Insert, Update or Delete, and DDL statements, the statement is complete as soon as it has finished executing.</li>
	 * <li>For Select statements, the statement is complete when the associated result set is closed.</li>
	 * For CallableStatement objects or for statements that return multiple results, the statement is complete when all of the associated result sets have been closed, and all update counts and output parameters have been retrieved.</li> 
	 * </ul>
	 *  If this method is called during a transaction and the auto-commit mode is changed, the transaction is committed. If setAutoCommit is called and the auto-commit mode is not changed, the call is a no-op.
	 *  @param autoCommit boolean true to enable auto-commit mode; false to disable it
	 *  @throws SQLException  if a database access error occurs, setAutoCommit(true) is called while participating in a distributed transaction, or this method is called on a closed connection
	 */
	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		if (DebugFile.trace)
			DebugFile.writeln("JDCConnection.setAutoCommit(" + String.valueOf(autoCommit)+ ") " + (this.name!=null ? this.name: "") + " " + (getId()!=null ? getId().toString() : "no Xid"));
		conn.setAutoCommit(autoCommit);
	}

	/**
	 * <p>Retrieves the current auto-commit mode for this Connection object.</p>
	 * @return boolean
	 * @throws SQLException if a database access error occurs or this method is called on a closed connection
	 */
	@Override
	public boolean getAutoCommit() throws SQLException {
		return conn.getAutoCommit();
	}

	/**
	 * <p>Retrieves the current holdability of ResultSet objects created using this Connection object.</p>
	 * @return int one of ResultSet.HOLD_CURSORS_OVER_COMMIT or ResultSet.CLOSE_CURSORS_AT_COMMIT
	 * @throws SQLException if a database access error occurs or this method is called on a closed connection
	 */
	@Override
	public int getHoldability() throws SQLException {
		return conn.getHoldability();
	}

	/**
	 * <p>Changes the default holdability of ResultSet objects created using this Connection object to the given holdability.</p>
	 * The default holdability of ResultSet objects can be be determined by invoking DatabaseMetaData.getResultSetHoldability().
	 * @param int one of ResultSet.HOLD_CURSORS_OVER_COMMIT or ResultSet.CLOSE_CURSORS_AT_COMMIT
	 * @throws SQLException if a database access error occurs or this method is called on a closed connection
	 */
	@Override
	public void setHoldability(int h) throws SQLException {
		conn.setHoldability(h);
	}

	/**
	 * <p>Creates an unnamed savepoint in the current transaction and returns the new Savepoint object that represents it.</p>
	 * If setSavepoint is invoked outside of an active transaction, a transaction will be started at this newly created savepoint.
	 * @return Savepoint the new Savepoint object
	 * @throws SQLException if a database access error occurs, this method is called while participating in a distributed transaction, this method is called on a closed connection or this Connection object is currently in auto-commit mode
	 */
	@Override
	public Savepoint setSavepoint() throws SQLException {
		return conn.setSavepoint();
	}

	/**
	 * <p>Creates a savepoint with the given name in the current transaction and returns the new Savepoint object that represents it.</p>
	 * If setSavepoint is invoked outside of an active transaction, a transaction will be started at this newly created savepoint.
	 * @param name String
	 * @return Savepoint the new Savepoint object
	 * @throws SQLException if a database access error occurs, this method is called while participating in a distributed transaction, this method is called on a closed connection or this Connection object is currently in auto-commit mode
	 * 
	 */
	@Override
	public Savepoint setSavepoint(String name) throws SQLException {
		return conn.setSavepoint(name);
	}

	/**
	 * <p>Makes all changes made since the previous commit/rollback permanent and releases any database locks currently held by this Connection object.</p>
	 * This method should be used only when auto-commit mode has been disabled.
	 * @throws SQLException if a database access error occurs, this method is called while participating in a distributed transaction, if this method is called on a closed conection or this Connection object is in auto-commit mode
	 */
	@Override
	public void commit() throws SQLException {
		if (conn.getAutoCommit())
			throw new SQLException("Can't commit Connection when autocommit is true");

		conn.commit();
	}

	/**
	 * <p>Undoes all changes made in the current transaction and releases any database locks currently held by this Connection object.</p>
	 * This method should be used only when auto-commit mode has been disabled.
	 * @throws SQLException if a database access error occurs, this method is called while participating in a distributed transaction, this method is called on a closed connection or this Connection object is in auto-commit mode
	 */
	@Override
	public void rollback() throws SQLException {
		if (conn.getAutoCommit())
			throw new SQLException("Can't rollback Connection when autocommit is true");

		if (DebugFile.trace) {
			DebugFile.writeln("Begin JDCConnection.rollback()");
			DebugFile.incIdent();
		}

		conn.rollback();

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End JDCConnection.rollback()");
		}
	}

	/**
	 * <p>Undoes all changes made after the given Savepoint object was set.</p>
	 * This method should be used only when auto-commit has been disabled.
	 * @param p Savepoint the Savepoint object to roll back to
	 * @throws SQLException if a database access error occurs, this method is called while participating in a distributed transaction, this method is called on a closed connection, the Savepoint object is no longer valid, or this Connection object is currently in auto-commit mode
	 */
	@Override
	public void rollback(Savepoint p) throws SQLException {
		if (conn.getAutoCommit())
			throw new SQLException("Can't rollback Connection when autocommit is true");

		if (DebugFile.trace) {
			DebugFile.writeln("Begin JDCConnection.rollback(Savepoint)");
			DebugFile.incIdent();
		}

		conn.rollback(p);

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End JDCConnection.rollback()");
		}
	}

	/**
	 * <p>Retrieves whether this Connection object has been closed.</p>
	 *  A connection is closed if the method close has been called on it or if certain fatal errors have occurred. This method is guaranteed to return true only when it is called after the method Connection.close has been called.
	 *  This method generally cannot be called to determine whether a connection to a database is valid or invalid. A typical client can determine that a connection is invalid by catching any exceptions that might be thrown when an operation is attempted.
	 * @return boolean true if this Connection object is closed; false if it is still open
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public boolean isClosed() throws SQLException {
		return conn.isClosed();
	}

	/**
	 * <p>Get whether this connection has been started as a transaction resource.</p>
	 * @return boolean
	 */
	public boolean isStarted() {
		return started;
	}

	/**
	 * <p>Returns true if the connection has not been closed and is still valid.</p>
	 * The driver shall submit a query on the connection or use some other mechanism that positively verifies the connection is still valid when this method is called.
	 * The query submitted by the driver to validate the connection shall be executed in the context of the current transaction.
	 * @param int The time in seconds to wait for the database operation used to validate the connection to complete. If the timeout period expires before the operation completes, this method returns false. A value of 0 indicates a timeout is not applied to the database operation.
	 * @return boolean true if the connection is valid, false otherwise
	 * @throws SQLException if the value supplied for timeout is less then 0
	 */
	@Override
	public boolean isValid(int timeout) throws SQLException {
		return conn.isValid(timeout);
	}

	/**
	 * <p>Retrieves a DatabaseMetaData object that contains metadata about the database to which this Connection object represents a connection.</p>
	 * The metadata includes information about the database's tables, its supported SQL grammar, its stored procedures, the capabilities of this connection, and so on.
	 * @return DatabaseMetaData object for this Connection object
	 * @throws SQLException if a database access error occurs or this method is called on a closed connection
	 */
	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		return conn.getMetaData();
	}

	/**
	 * <p>Puts this connection in read-only mode as a hint to the driver to enable database optimizations.</p>
	 * This method cannot be called during a transaction.
	 * @param readOnly boolean true enables read-only mode; false disables it
	 * @throws SQLException  if a database access error occurs, this method is called on a closed connection or this method is called during a transaction
	 */
	@Override
	public void setReadOnly(boolean readOnly) throws SQLException {
		conn.setReadOnly(readOnly);
	}

	/**
	 * <p>Retrieves whether this Connection object is in read-only mode.</p>
	 * @return true if this Connection object is read-only; false otherwise
	 * @throws SQLException if a database access error occurs or this method is called on a closed connection
	 */
	@Override
	public boolean isReadOnly() throws SQLException {
		return conn.isReadOnly();
	}

	/**
	 * <p>Sets the given catalog name in order to select a subspace of this Connection object's database in which to work.</p>
	 * If the driver does not support catalogs, it will silently ignore this request.
	 * Calling setCatalog has no effect on previously created or prepared Statement objects. It is implementation defined whether a DBMS prepare operation takes place immediately when the Connection method prepareStatement or prepareCall is invoked. For maximum portability, setCatalog should be called before a Statement is created or prepared.
	 * @param catalog String the name of a catalog (subspace in this Connection object's database) in which to work
	 * @throws SQLException if a database access error occurs or this method is called on a closed connection
	 */
	@Override
	public void setCatalog(String catalog) throws SQLException {
		conn.setCatalog(catalog);
	}

	/**
	 * <p>Retrieves this Connection object's current catalog name.</p>
	 * @return String the current catalog name or null if there is none
	 * @throws SQLException if a database access error occurs or this method is called on a closed connection
	 */
	@Override
	public String getCatalog() throws SQLException {
		return conn.getCatalog();
	}

	/**
	 * <p>Attempts to change the transaction isolation level for this Connection object to the one given.</p>
	 * The constants defined in the interface Connection are the possible transaction isolation levels.
	 * If this method is called during a transaction, the result is implementation-defined.
	 * @param level int  one of the following Connection constants: Connection.TRANSACTION_READ_UNCOMMITTED, Connection.TRANSACTION_READ_COMMITTED, Connection.TRANSACTION_REPEATABLE_READ, or Connection.TRANSACTION_SERIALIZABLE. (Note that Connection.TRANSACTION_NONE cannot be used because it specifies that transactions are not supported.)
	 * @throws SQLException if a database access error occurs, this method is called on a closed connection or the given parameter is not one of the Connection constants
	 */
	@Override
	public void setTransactionIsolation(int level) throws SQLException {
		conn.setTransactionIsolation(level);
	}

	/**
	 * <p>Retrieves this Connection object's current transaction isolation level.</p>
	 * @return int the current transaction isolation level, which will be one of the following constants: Connection.TRANSACTION_READ_UNCOMMITTED, Connection.TRANSACTION_READ_COMMITTED, Connection.TRANSACTION_REPEATABLE_READ, Connection.TRANSACTION_SERIALIZABLE, or Connection.TRANSACTION_NONE.
	 * @throws SQLException if a database access error occurs or this method is called on a closed connection
	 */
	@Override
	public int getTransactionIsolation() throws SQLException {
		return conn.getTransactionIsolation();
	}

	/**
	 * <p>Retrieves the Map object associated with this Connection object.</p>
	 * Unless the application has added an entry, the type map returned will be empty. 
	 * You must invoke setTypeMap after making changes to the Map object returned from getTypeMap as a JDBC driver may create an internal copy of the Map object passed to setTypeMap
	 * @return the java.util.Map object associated with this Connection object
	 * @throws SQLException  if a database access error occurs or this method is called on a closed connection
	 */
	@Override
	public Map<String,Class<?>> getTypeMap() throws SQLException {
		return conn.getTypeMap();
	}

	/**
	 * <p>Installs the given TypeMap object as the type map for this Connection object.</p>
	 * The type map will be used for the custom mapping of SQL structured types and distinct types.
	 * You must set the the values for the TypeMap prior to callng setMap as a JDBC driver may create an internal copy of the TypeMap
	 * @param typemap Map&lt;String,Class&lt;?&gt;&gt;
	 * @throws SQLException  if a database access error occurs or this method is called on a closed connection
	 */
	@Override
	public void setTypeMap(Map<String,Class<?>> typemap) throws SQLException {
		conn.setTypeMap(typemap);
	}

	/**
	 * <p>Retrieves the first warning reported by calls on this Connection object.</p>
	 * If there is more than one warning, subsequent warnings will be chained to the first one and can be retrieved by calling the method SQLWarning.getNextWarning on the warning that was retrieved previously.
	 * Subsequent warnings will be chained to this SQLWarning.
	 * @return SQLWarning the first SQLWarning object or null if there are none
	 * @throws SQLException if a database access error occurs or this method is called on a closed connection
	 */
	@Override
	public SQLWarning getWarnings() throws SQLException {
		return conn.getWarnings();
	}

	/**
	 * <p>Clears all warnings reported for this Connection object.</p>
	 * After a call to this method, the method getWarnings returns null until a new warning is reported for this Connection object.
	 * @throws SQLException if a database access error occurs or this method is called on a closed connection
	 */
	@Override
	public void clearWarnings() throws SQLException {
		conn.clearWarnings();
	}

	/**
	 * <p>Removes the specified Savepoint and subsequent Savepoint objects from the current transaction.</p>
	 * Any reference to the savepoint after it have been removed will cause an SQLException to be thrown.
	 * @param p the Savepoint object to be removed
	 * @throws SQLException if a database access error occurs, this method is called on a closed connection or the given Savepoint object is not a valid savepoint in the current transaction
	 */
	@Override
	public void releaseSavepoint(Savepoint p) throws SQLException {
		conn.releaseSavepoint(p);
	}


	/**
	 * <p>Get operating system process identifier for this connection</p>
	 * @return String
	 * <ul>
	 * <li>For PostgreSQL the id of the UNIX process attending this connection.</li>
	 * <li>For Oracle a Session Id.</li>
	 * <li>For any other RDBMS "unknown"</li>
	 * </ul>
	 * @throws SQLException
	 */
	public String pid() throws SQLException {
		Statement oStmt;
		ResultSet oRSet;
		String sPId = "unknown";
		final int rdbms = getDataBaseProduct();

		if (RDBMS.POSTGRESQL.intValue()==rdbms) {
			oStmt = createStatement();
			oRSet = oStmt.executeQuery("SELECT pg_backend_pid()");
			if (oRSet.next())
				sPId = String.valueOf(oRSet.getInt(1));
			oRSet.close();
			oStmt.close();			
		} else if (RDBMS.ORACLE.intValue()==rdbms) {
			oStmt = createStatement();
			oRSet = oStmt.executeQuery("SELECT SYS_CONTEXT('USERENV','SESIONID') FROM DUAL");
			if (oRSet.next())
				sPId = oRSet.getString(1);
			oRSet.close();
			oStmt.close();
		}

		return sPId;
	} // pid

	// ---------------------------------------------------------------------------

	private int bindParameterOrcl (PreparedStatement oStmt, int iParamIndex, Object oParamValue, int iSQLType) throws SQLException {

		if (oParamValue!=null) {

			Class oParamClass = oParamValue.getClass();

			if (oParamClass.equals(Short.class) || oParamClass.equals(Integer.class) || oParamClass.equals(Float.class) || oParamClass.equals(Double.class))
				oStmt.setBigDecimal (iParamIndex, new java.math.BigDecimal(oParamValue.toString()));

			else if ((oParamClass.getName().equals("java.sql.Timestamp") ||
					oParamClass.getName().equals("java.util.Date") || oParamClass.getName().equals("java.util.Calendar"))    &&
					iSQLType==Types.DATE) {
				try {
					Class[] aTimestamp = new Class[1];
					aTimestamp[0] = Class.forName("java.sql.Timestamp");
					Class cDATE = Class.forName("oracle.sql.DATE");
					java.lang.reflect.Constructor cNewDATE = cDATE.getConstructor(aTimestamp);
					Object oDATE;
					if (oParamClass.getName().equals("java.sql.Timestamp")) {
						oDATE = cNewDATE.newInstance(new Object[]{oParamValue});
					} else if (oParamValue instanceof Date) {
						oDATE = cNewDATE.newInstance(new Object[]{new Timestamp(((java.util.Date)oParamValue).getTime())});
					} else {
						oDATE = cNewDATE.newInstance(new Object[]{new Timestamp(((java.util.Calendar)oParamValue).getTimeInMillis())});
					}
					oStmt.setObject (iParamIndex, oDATE, iSQLType);
				} catch (ClassNotFoundException cnf) {
					throw new SQLException("ClassNotFoundException oracle.sql.DATE " + cnf.getMessage());
				} catch (NoSuchMethodException nsm) {
					throw new SQLException("NoSuchMethodException " + nsm.getMessage());
				} catch (IllegalAccessException iae) {
					throw new SQLException("IllegalAccessException " + iae.getMessage());
				} catch (InstantiationException ine) {
					throw new SQLException("InstantiationException " + ine.getMessage());
				} catch (java.lang.reflect.InvocationTargetException ite) {
					throw new SQLException("InvocationTargetException " + ite.getMessage());
				}
			}
			else if (oParamClass.getName().equals("java.util.Date") && iSQLType==Types.TIMESTAMP) {
				oStmt.setTimestamp(iParamIndex, new Timestamp(((java.util.Date)oParamValue).getTime()));
			}
			else {
				oStmt.setObject (iParamIndex, oParamValue, iSQLType);
			}
		}
		else
			oStmt.setNull(iParamIndex, iSQLType);

		return 1;
	}

	// ---------------------------------------------------------------------------

	private int bindPgGeography(String oParamValue, PreparedStatement oStmt, int iParamIndex, int iSQLType)
		throws NumberFormatException, SQLException {
		int nBinded;
		String sParamValue = (String) oParamValue;
		if (sParamValue.matches("-?\\d+(\\x2E\\d+)? -?\\d+(\\x2E\\d+)?")) {
			String[] aLatLng = sParamValue.split(" ");
			if (DebugFile.trace) DebugFile.writeln("binding parameter " + String.valueOf(iParamIndex) + " String lattitude "+ aLatLng[0] +" as SQL DOUBLE");
			oStmt.setDouble(iParamIndex, new Double(aLatLng[0]).doubleValue());
			if (DebugFile.trace) DebugFile.writeln("binding parameter " + String.valueOf(iParamIndex+1) + " String longitude "+ aLatLng[1] +" as SQL DOUBLE");
			oStmt.setDouble(iParamIndex+1, new Double(aLatLng[1]).doubleValue());
			nBinded = 2;              		
		} else if (sParamValue.equalsIgnoreCase("NULL NULL")) {
			if (DebugFile.trace) DebugFile.writeln("binding parameter " + String.valueOf(iParamIndex) + " String lattitude null as SQL DOUBLE");
			oStmt.setNull(iParamIndex, Types.DOUBLE);
			if (DebugFile.trace) DebugFile.writeln("binding parameter " + String.valueOf(iParamIndex+1) + " String longitude null as SQL DOUBLE");
			oStmt.setNull(iParamIndex+1, Types.DOUBLE);
			nBinded = 2;
		} else {
			oStmt.setObject(iParamIndex, oParamValue, iSQLType);
			nBinded = 1;
		}
		return nBinded;
	}
	
	// ---------------------------------------------------------------------------

	private int bindPgGeography(LatLong oParamValue, PreparedStatement oStmt, int iParamIndex, int iSQLType) throws SQLException {
		if (DebugFile.trace) DebugFile.writeln("binding parameter " + String.valueOf(iParamIndex) + " LatLong lattitude "+ String.valueOf(oParamValue.getLattitude()) +" as SQL DOUBLE");
		if (Double.isNaN(oParamValue.getLattitude()))
			oStmt.setNull(iParamIndex, Types.DOUBLE);
		else
			oStmt.setDouble(iParamIndex, oParamValue.getLattitude());
		if (DebugFile.trace) DebugFile.writeln("binding parameter " + String.valueOf(iParamIndex+1) + " LatLong longitude "+ String.valueOf(oParamValue.getLongitude()) +" as SQL DOUBLE");
		if (Double.isNaN(oParamValue.getLongitude()))
			oStmt.setNull(iParamIndex+1, Types.DOUBLE);
		else
			oStmt.setDouble(iParamIndex+1, oParamValue.getLongitude());
		return 2;
	}

	// ---------------------------------------------------------------------------

	private int bindPgHStore(Map oParamValue, PreparedStatement oStmt, int iParamIndex, int iSQLType) throws SQLException {
		try {
			Class<?> cPgObj = Class.forName("org.postgresql.util.PGobject");
			Object oPgObj = cPgObj.newInstance();
			Method oSetType = cPgObj.getMethod("setType", new Class[]{String.class});
			oSetType.invoke(oPgObj, new Object[]{"hstore"});
			Method oSetValue = cPgObj.getMethod("setValue", new Class[]{String.class});
			oSetValue.invoke(oPgObj, new Object[]{new HStore((Map<String,String>) oParamValue).getValue()});
			if (DebugFile.trace) DebugFile.writeln("binding parameter " + String.valueOf(iParamIndex) + " " + oPgObj + " as HSTORE");
			oStmt.setObject(iParamIndex, oPgObj, Types.OTHER);
		} catch (ClassNotFoundException cnf) {
			throw new SQLException("ClassNotFoundException org.postgresql.util.PGobject");
		} catch (IllegalAccessException iae) {
			throw new SQLException("IllegalAccessException PGobject "+iae.getMessage());
		} catch (InstantiationException ine) {
			throw new SQLException("InstantiationException PGobject "+ine.getMessage());
		} catch (NoSuchMethodException nsm) {
			throw new SQLException("NoSuchMethodException PGobject "+nsm.getMessage());						  
		} catch (InvocationTargetException ite) {
			throw new SQLException("InvocationTargetException PGobject "+ite.getMessage());						  
		}
		return 1;
	}

	// ---------------------------------------------------------------------------

	private int bindPgObject(Object oParamValue, PreparedStatement oStmt, int iParamIndex, int iSQLType) throws SQLException {
		int nBinded;
		try {
			Method getType = oParamValue.getClass().getMethod("getType");
			String objType = (String) getType.invoke(oParamValue);
			if (objType.toLowerCase().indexOf("hstore")>=0) {
				if (DebugFile.trace) DebugFile.writeln("binding parameter " + String.valueOf(iParamIndex) + " " + oParamValue + " as HSTORE");
				oStmt.setObject(iParamIndex, oParamValue, Types.OTHER);
				nBinded = 1;
			} else {
				Method getValue = oParamValue.getClass().getMethod("getValue");
				String objValue = (String) getValue.invoke(oParamValue);
				nBinded = bindPgGeography(objValue, oStmt, iParamIndex, iSQLType);
			}
		} catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			throw new SQLException(e.getClass().getName() + " at org.postgresql.util.PGobject");
		}
		return nBinded;
	}
	

	// ---------------------------------------------------------------------------

	/**
	 * <p>Bind parameter into a PreparedStatement</p>
	 * @param oStmt PreparedStatement where values is to be binded
	 * @param iParamIndex int Starting with 1
	 * @param oParamValue Object
	 * @param iSQLType int
	 * @throws SQLException
	 * @return int Count of actually binded parameters
	 * @throws SQLException
	 */
	public int bindParameter (PreparedStatement oStmt, int iParamIndex, Object oParamValue, int iSQLType) throws SQLException {

		int nBinded = 1;

		if (RDBMS.ORACLE.intValue()==getDataBaseProduct()) {

			nBinded = bindParameterOrcl (oStmt, iParamIndex, oParamValue, iSQLType);

		} else {

			if (DebugFile.trace) {
				DebugFile.writeln("JDCConnection.bindParameter("+iParamIndex+"," +
						(null==oParamValue ? "null" : oParamValue.getClass().getName()+" "+oParamValue.toString()) + ","+ 
						ColumnDef.typeName(iSQLType)+")");
			}

			String sParamClassName;
			if (null!=oParamValue)
				sParamClassName = oParamValue.getClass().getName();
			else
				sParamClassName = "null";

			if ((Types.TIMESTAMP==iSQLType) && (oParamValue!=null)) {
				if (sParamClassName.equals("java.util.Date"))
					oStmt.setTimestamp(iParamIndex, new Timestamp(((java.util.Date)oParamValue).getTime()));
				else if (oParamValue instanceof Calendar)
					oStmt.setTimestamp(iParamIndex, new Timestamp(((java.util.Calendar)oParamValue).getTimeInMillis()));
				else
					oStmt.setObject(iParamIndex, oParamValue, iSQLType);
			}
			else if ((Types.DATE==iSQLType) && (oParamValue!=null)) {
				if (sParamClassName.equals("java.util.Date"))
					oStmt.setDate(iParamIndex, new java.sql.Date(((java.util.Date)oParamValue).getTime()));
				else if (oParamValue instanceof Calendar)
					oStmt.setDate(iParamIndex, new java.sql.Date(((java.util.Calendar)oParamValue).getTimeInMillis()));
				else
					oStmt.setObject(iParamIndex, oParamValue, iSQLType);
			}
			else if ((Types.ARRAY==iSQLType) && (oParamValue!=null)) {
				oStmt.setArray(iParamIndex, toArray(oParamValue));
			}
			else if (Types.OTHER==iSQLType) { // PostgreSQL PGObject (may be interval, geoposition or hstore)
				if (oParamValue!=null) {
					if (oParamValue instanceof LatLong) {
						nBinded = bindPgGeography((LatLong) oParamValue, oStmt, iParamIndex, iSQLType);
					} else if (oParamValue instanceof String) {
						nBinded = bindPgGeography((String) oParamValue, oStmt, iParamIndex, iSQLType);
					} else if (oParamValue instanceof Map) {
						nBinded = bindPgHStore((Map) oParamValue, oStmt, iParamIndex, iSQLType);
					} else if (oParamValue.getClass().getName().equals("org.postgresql.util.PGobject")) {
						nBinded = bindPgObject(oParamValue, oStmt, iParamIndex, iSQLType);						
					} else {
						if (DebugFile.trace) DebugFile.writeln("binding "+oParamValue.getClass().getName()+" as PGObject");
						oStmt.setObject(iParamIndex, oParamValue, iSQLType);                  
					}
				} else {
					oStmt.setNull(iParamIndex, iSQLType);
					nBinded = 1;
				}
			}
			else {
				if (oParamValue!=null) {
					oStmt.setObject(iParamIndex, oParamValue, iSQLType);
				} else {
					oStmt.setNull(iParamIndex, iSQLType);
				}
			}
		}

		if (DebugFile.trace) DebugFile.writeln("JDCConnection.bindParameter("+String.valueOf(iParamIndex)+","+oParamValue+","+String.valueOf(iSQLType)+") : "+String.valueOf(nBinded));

		return nBinded;
	} // bindParameter

	// ---------------------------------------------------------------------------

	/**
	 * <p>Bind parameter to a PreparedStatement.</p>
	 * @param oStmt PreparedStatement
	 * @param iParamIndex int
	 * @param oParamValue Object
	 * @return
	 * @throws SQLException
	 */
	public int bindParameter (PreparedStatement oStmt, int iParamIndex, Object oParamValue) throws SQLException {

		if (getDataBaseProduct()==RDBMS.ACCESS.intValue()) {
			if (oParamValue.getClass().equals(Integer.class) ||
					oParamValue.getClass().equals(Short.class) ||
					oParamValue.getClass().equals(Float.class) ||
					oParamValue.getClass().equals(Double.class)) {
				bindParameter(oStmt, iParamIndex, oParamValue, Types.NUMERIC);
			}
			else if (oParamValue.getClass().getName().equals("java.util.Date") ||
					oParamValue.getClass().getName().equals("java.sql.Timestamp") ) {
				bindParameter(oStmt, iParamIndex, oParamValue, Types.DATE);
			}
			else {
				oStmt.setObject(iParamIndex, oParamValue);
			}
		} else {
			oStmt.setObject(iParamIndex, oParamValue);
		}
		return 1;
	} // bindParameter

	// ---------------------------------------------------------------------------

	/**
	 * <p>Checks if an object exists.</p>
	 * Checking is done directly against database catalog tables,
	 * if current user does not have enough privileges for reading
	 * database catalog tables methods may fail or return a wrong result.
	 * @param sObjectName Object name
	 * @param sObjectType Object type
	 *        C = CHECK constraint
	 *        D = Default or DEFAULT constraint
	 *        F = FOREIGN KEY constraint
	 *        L = Log
	 *        P = Stored procedure
	 *        PK = PRIMARY KEY constraint (type is K)
	 *        RF = Replication filter stored procedure
	 *        S = System table
	 *        TR = Trigger
	 *        U = User table
	 *        UQ = UNIQUE constraint (type is K)
	 *        V = View
	 *        X = Extended stored procedure
	 * @return <b>true</b> if object exists, <b>false</b> otherwise
	 * @throws SQLException
	 * @throws UnsupportedOperationException If current database management system is not supported for this method
	 */
	public boolean exists(String sObjectName, String sObjectType) throws SQLException, UnsupportedOperationException {
		boolean bRetVal;
		PreparedStatement oStmt;
		ResultSet oRSet;

		if (DebugFile.trace) {
			DebugFile.writeln("Begin JDCConnection.exists([Connection], " + sObjectName + ", " + sObjectType + ")");
			DebugFile.incIdent();
			DebugFile.writeln("RDBMS is "+dbms);
		}

		final int dbms = getDataBaseProduct();

		if (dbms==RDBMS.MSSQL.intValue()) {
			if (DebugFile.trace)
				DebugFile.writeln ("Connection.prepareStatement(SELECT id FROM sysobjects WHERE name='" + sObjectName + "' AND xtype='" + sObjectType + "' OPTION (FAST 1))");

			oStmt = this.prepareStatement("SELECT id FROM sysobjects WHERE name=? AND xtype=? OPTION (FAST 1)", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			oStmt.setString(1, sObjectName);
			oStmt.setString(2, sObjectType);
			oRSet = oStmt.executeQuery();
			bRetVal = oRSet.next();
			oRSet.close();
			oStmt.close();

		} else if (dbms==RDBMS.POSTGRESQL.intValue()) {
			if (DebugFile.trace)
				DebugFile.writeln ("Conenction.prepareStatement(SELECT relname FROM pg_class WHERE relname='" + sObjectName + "')");

			oStmt = this.prepareStatement("SELECT tablename FROM pg_tables WHERE tablename=?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			oStmt.setString(1, sObjectName);
			oRSet = oStmt.executeQuery();
			bRetVal = oRSet.next();
			oRSet.close();
			oStmt.close();

		} else if (dbms==RDBMS.ORACLE.intValue()) {
			if (DebugFile.trace)
				DebugFile.writeln ("Conenction.prepareStatement(SELECT TABLE_NAME FROM USER_TABLES WHERE TABLE_NAME='" + sObjectName + "')");

			oStmt = this.prepareStatement("SELECT TABLE_NAME FROM USER_TABLES WHERE TABLE_NAME=?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			oStmt.setString(1, sObjectName.toUpperCase());
			oRSet = oStmt.executeQuery();
			bRetVal = oRSet.next();
			oRSet.close();
			oStmt.close();

		} else if (dbms==RDBMS.MYSQL.intValue()) {
			if (DebugFile.trace)
				DebugFile.writeln ("Conenction.prepareStatement(SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_name='"+sObjectName+"')");

			oStmt = prepareStatement("SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_name=?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			oStmt.setString(1, sObjectName);
			oRSet = oStmt.executeQuery();
			bRetVal = oRSet.next();
			oRSet.close();
			oStmt.close();

		} else if (dbms==RDBMS.HSQLDB.intValue()) {
			String sTableType;
			if (sObjectType.equals("U"))
				sTableType = "TABLE";
			else if (sObjectType.equals("S"))
				sTableType = "SYSTEM TABLE";
			else
				throw new UnsupportedOperationException("Type "+sObjectType+" is not supported by exists() for "+RDBMS.HSQLDB.toString());

			if (DebugFile.trace)
				DebugFile.writeln ("Conenction.prepareStatement(SELECT TABLE_NAME,TABLE_TYPE FROM INFORMATION_SCHEMA.SYSTEM_TABLES WHERE TABLE_NAME='"+sObjectName.toUpperCase()+"' AND TABLE_TYPE='"+sTableType+"')");

			oStmt = prepareStatement("SELECT TABLE_NAME,TABLE_TYPE FROM INFORMATION_SCHEMA.SYSTEM_TABLES WHERE TABLE_NAME=? AND TABLE_TYPE=?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			oStmt.setString(1, sObjectName.toUpperCase());
			oStmt.setString(2, sTableType);
			oRSet = oStmt.executeQuery();
			bRetVal = oRSet.next();
			oRSet.close();
			oStmt.close();

		} else {
			throw new UnsupportedOperationException ("Unsupported DBMS");
		} // end switch()

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End JDCConnection.exists() : " + String.valueOf(bRetVal));
		}

		return bRetVal;
	} // exists()

	// ---------------------------------------------------------------------------

	/**
	 * <p>Terminates an open connection.</p>
	 * Calling abort results in: 
	 * <ul>
	 * <li>The connection marked as closed</li>
	 * <li>Closes any physical connection to the database</li>
	 * <li>Releases resources used by the connection</li>
	 * <li>Insures that any thread that is currently accessing the connection will either progress to completion or throw an SQLException</li>
	 * <li>If the connection is pooled the it is removed from the pool</li>
	 * </ul>
	 * Calling abort marks the connection closed and releases any resources. Calling abort on a closed connection is a no-op.
	 * It is possible that the aborting and releasing of the resources that are held by the connection can take an extended period of time. When the abort method returns, the connection will have been marked as closed and the Executor that was passed as a parameter to abort may still be executing tasks to release resources.
	 * This method checks to see that there is an SQLPermission object before allowing the method to proceed. If a SecurityManager exists and its checkPermission method denies calling abort, this method throws a java.lang.SecurityException.
	 * @param executor The Executor implementation which will be used by abort.
	 * @throws SQLException if a database access error occurs or the executor is null
	 * @throws SecurityException if a security manager exists and its checkPermission method denies calling abort
	 */
	@Override
	public void abort(Executor oExec) throws SQLException, SecurityException {
		conn.abort(oExec);
		if (pool!=null) {
			pool.disposeConnection(this);
		}
		inuse = false;
		name = null;
	}

	// ---------------------------------------------------------------------------

	/**
	 * <p>Retrieves the number of milliseconds the driver will wait for a database request to complete.</p>
	 * @return int 
	 * @throws SQLException if a database access error occurs or this method is called on a closed Connection
	 */
	@Override
	public int getNetworkTimeout() throws SQLException {
		return conn.getNetworkTimeout();
	}

	// ---------------------------------------------------------------------------

	/**
	 * <p>Sets the maximum period a Connection or objects created from the Connection will wait for the database to reply to any one request.</p>
	 * If any request remains unanswered, the waiting method will return with a SQLException, and the Connection or objects created from the Connection will be marked as closed. Any subsequent use of the objects, with the exception of the close, isClosed or Connection.isValid methods, will result in a SQLException.
	 * This method is intended to address a rare but serious condition where network partitions can cause threads issuing JDBC calls to hang uninterruptedly in socket reads, until the OS TCP-TIMEOUT (typically 10 minutes). This method is related to the abort() method which provides an administrator thread a means to free any such threads in cases where the JDBC connection is accessible to the administrator thread. The setNetworkTimeout method will cover cases where there is no administrator thread, or it has no access to the connection. This method is severe in it's effects, and should be given a high enough value so it is never triggered before any more normal timeouts, such as transaction timeouts.
	 * JDBC driver implementations may also choose to support the setNetworkTimeout method to impose a limit on database response time, in environments where no network is present.
	 * Drivers may internally implement some or all of their API calls with multiple internal driver-database transmissions, and it is left to the driver implementation to determine whether the limit will be applied always to the response to the API call, or to any single request made during the API call.
	 * This method can be invoked more than once, such as to set a limit for an area of JDBC code, and to reset to the default on exit from this area. Invocation of this method has no impact on already outstanding requests.
	 * The Statement.setQueryTimeout() timeout value is independent of the timeout value specified in setNetworkTimeout. If the query timeout expires before the network timeout then the statement execution will be canceled. If the network is still active the result will be that both the statement and connection are still usable. However if the network timeout expires before the query timeout or if the statement timeout fails due to network problems, the connection will be marked as closed, any resources held by the connection will be released and both the connection and statement will be unusable. 
	 * When the driver determines that the setNetworkTimeout timeout value has expired, the JDBC driver marks the connection closed and releases any resources held by the connection. 
	 * This method checks to see that there is an SQLPermission object before allowing the method to proceed. If a SecurityManager exists and its checkPermission method denies calling setNetworkTimeout, this method throws a java.lang.SecurityException.
	 * @param oExec The Executor implementation which will be used by setNetworkTimeout.
	 * @param iTimeout int The time in milliseconds to wait for the database operation to complete. If the JDBC driver does not support milliseconds, the JDBC driver will round the value up to the nearest second. If the timeout period expires before the operation completes, a SQLException will be thrown. A value of 0 indicates that there is not timeout for database operations.
	 * @throws SQLException if a database access error occurs, this method is called on a closed connection, the executor is null, or the value specified for seconds is less than 0.
	 * @throws SecurityException  if a security manager exists and its checkPermission method denies calling setNetworkTimeout
	 */
	@Override
	public void setNetworkTimeout(Executor oExec, int iTimeout) throws SQLException, SecurityException {
		conn.setNetworkTimeout(oExec, iTimeout);
	}

	// ---------------------------------------------------------------------------

	/**
	 * <p>Retrieves this Connection object's current schema name.</p>
	 * @return String
	 * @throws SQLException if a database access error occurs or this method is called on a closed Connection
	 */
	@Override
	public String getSchema() throws SQLException {
		return conn.getSchema();
	}

	// ---------------------------------------------------------------------------

	/**
	 * <p>Set this Connection object's current schema name.</p>
	 * @param schemaName String
	 * @throws SQLException if a database access error occurs or this method is called on a closed Connection
	 */
	@Override
	public void setSchema(String schemaName) throws SQLException {
		conn.setSchema(schemaName);
	}

	// ---------------------------------------------------------------------------

	/**
	 * <p>This methods does nothing and always return false.</p>
	 * @param seconds int
	 * @return boolean false
	 */
	@Override
	public boolean setTransactionTimeout(int seconds)  {
		return false;
	}

	// ---------------------------------------------------------------------------

	private Array toArray(Object oObj) throws SQLException {
		if (oObj instanceof Array)
			return (Array) oObj;
		if (oObj instanceof Boolean[])
			return createArrayOf("boolean", (Boolean[]) oObj);
		if (oObj instanceof Short[])
			return createArrayOf("smallint", (Short[]) oObj);
		if (oObj instanceof Integer[])
			return createArrayOf("integer", (Integer[]) oObj);
		if (oObj instanceof Long[])
			return createArrayOf("bigint", (Long[]) oObj);
		if (oObj instanceof Float[])
			return createArrayOf("float", (Float[]) oObj);
		if (oObj instanceof Double[])
			return createArrayOf("double", (Double[]) oObj);
		if (oObj instanceof BigDecimal[])
			return createArrayOf("decimal", (BigDecimal[]) oObj);
		if (oObj instanceof String[])
			return createArrayOf("varchar", (String[]) oObj);
		if (oObj instanceof boolean[])
			return createArrayOf("boolean", Arr.toObject((boolean[]) oObj));
		if (oObj instanceof short[])
			return createArrayOf("smallint", Arr.toObject((short[]) oObj));
		if (oObj instanceof int[])
			return createArrayOf("integer", Arr.toObject((int[]) oObj));
		if (oObj instanceof long[])
			return createArrayOf("bigint", Arr.toObject((long[]) oObj));
		if (oObj instanceof float[])
			return createArrayOf("float", Arr.toObject((float[]) oObj));
		if (oObj instanceof double[])
			return createArrayOf("double", Arr.toObject((double[]) oObj));
		if (oObj instanceof Date[]) {
			Date[] oDts = (Date[]) oObj;
			Timestamp[] oTms = new Timestamp[oDts.length];
			for (int d=0; d<oDts.length; d++)
				oTms[d] = new Timestamp(oDts[d].getTime());
			return createArrayOf("timestamp", oTms);
		}
		if (oObj instanceof Timestamp[])
			return createArrayOf("timestamp", (Timestamp[]) oObj);
		throw new ClassCastException("Cannot cast from "+oObj.getClass().getName()+" to java.sql.Array");
	}
}
