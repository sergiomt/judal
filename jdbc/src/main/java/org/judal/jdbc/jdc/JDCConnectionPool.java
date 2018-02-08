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

import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.Enumeration;
import java.util.Date;
import java.util.Map;
import java.util.ConcurrentModificationException;
import java.util.logging.Logger;
import java.text.SimpleDateFormat;
import java.sql.CallableStatement;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;

import org.judal.jdbc.RDBMS;

import javax.jdo.JDOException;

import com.knowgate.debug.Chronometer;
import com.knowgate.debug.DebugFile;
import com.knowgate.debug.StackTraceUtil;

import org.judal.storage.Env;
import org.judal.storage.table.TableDataSource;


/**
* <p>Implementation of a standard JDBC connection pool.</p>
* @version 1.0
*/
public class JDCConnectionPool implements ConnectionPoolDataSource {

	private Vector<JDCConnection> connections;
	private int openconns;
	private ConcurrentHashMap<String,Long> callers;
	private String url, user, password;
	private JDCConnectionReaper reaper;
	private ConcurrentLinkedQueue<String> errorlog;
	private int maxerrors = 100;
	
	/**
	 * Staled connection threshold (10 minutes)
	 * The maximum time that any SQL single statement may last.
	 */
	private long timeout = 600000l;

	/**
	 * Soft limit for maximum open connections
	 */
	private int poolsize = 32;

	/**
	 * Hard absolute limit for maximum open connections
	 */
	private int hardlimit = 100;

	// ---------------------------------------------------------

	/**
	 * <p>Constructor</p>
	 * @throws IllegalArgumentException
	 * @throws NumberFormatException
	 */
	public JDCConnectionPool(Map<String,String> properties) throws IllegalArgumentException,NumberFormatException {
		final String uri = properties.get(TableDataSource.URI);
		
		if (null==uri)
			throw new IllegalArgumentException("JDCConnectionPool : uri cannot be null");

		if (uri.length()==0)
			throw new IllegalArgumentException("JDCConnectionPool : uri value not set");

		this.url = uri;
		this.user = properties.get(TableDataSource.USER);
		this.password = properties.get(TableDataSource.PASSWORD);
		this.openconns = 0;
		this.poolsize = Env.getPositiveInteger(properties, TableDataSource.MAXPOOLSIZE, Integer.parseInt(TableDataSource.DEFAULT_MAXPOOLSIZE));
		this.hardlimit = Env.getPositiveInteger(properties, TableDataSource.MAXCONNECTIONS, Integer.parseInt(TableDataSource.DEFAULT_MAXCONNECTIONS));
		this.timeout = Env.getPositiveInteger(properties, TableDataSource.CONNECTIONTIMEOUT, Integer.parseInt(TableDataSource.DEFAULT_CONNECTIONTIMEOUT));

		DriverManager.setLoginTimeout(Env.getPositiveInteger(properties, TableDataSource.LOGINTIMEOUT, Integer.parseInt(TableDataSource.DEFAULT_LOGINTIMEOUT)));

		connections = new Vector<JDCConnection>(this.poolsize<=hardlimit ? this.poolsize : hardlimit);
		reaper = new JDCConnectionReaper(this);
		reaper.start();

		if (DebugFile.trace) callers = new ConcurrentHashMap<String,Long>(1023);

		errorlog = new ConcurrentLinkedQueue<String>();
	}
	

	// ---------------------------------------------------------

	private synchronized void modifyMap (String sCaller, int iAction)
			throws NullPointerException {

		if (null==callers) callers = new ConcurrentHashMap<String,Long>(1023);

		if (callers.containsKey(sCaller)) {
			Long iRefCount = new Long(((Long) callers.get(sCaller)).longValue()+iAction);
			callers.remove(sCaller);
			callers.put(sCaller, iRefCount);
			DebugFile.writeln(sCaller + " reference count is " + iRefCount.toString());
		}
		else {
			if (1==iAction) {
				callers.put(sCaller, new Long(1l));
				DebugFile.writeln(sCaller + " reference count is 1");
			}
			else {
				DebugFile.writeln("ERROR: JDCConnectionPool get/close connection mismatch for " + sCaller);
			}
		}
	} // modifyMap


	// ---------------------------------------------------------

	/**
	 * Close all connections and stop connection reaper
	 */
	public void close() {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin ConnectionPool.close()");
			DebugFile.incIdent();
		}

		if (null!=reaper) reaper.halt();

		reaper = null;

		closeConnections();

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End ConnectionPool.close()");
		}
	} // close

	// ---------------------------------------------------------

	/**
	 * @return boolean
	 */
	public boolean isClosed() {
		return (reaper==null);
	}

	// ---------------------------------------------------------

	/**
	 * <p>Get preferred open connections limit</p>
	 * <p>Additional connections beyond PoolSize may be opened but they
	 * will closed immediately after use and not pooled.<br>The default value is 32.</p>
	 * @return open connections soft limit
	 */
	public int getPoolSize() {
		return poolsize;
	}

	// ---------------------------------------------------------

	/**
	 * <p>Set preferred open connections limit</p>
	 * <p>Additional connections beyond PoolSize may be opened but they
	 * will closed immediately after use and not pooled.<br>The default value is 32.<br>
	 * Connections not being used can only be in the pool for a maximum of five minutes.<br>
	 * After a connection is not used for over 5 minutes it will be closed so the actual
	 * pool size will eventually go down to zero after a period of inactivity.</p>
	 * @param iPoolSize Maximum pooled connections
	 */

	public void setPoolSize(int iPoolSize) {

		if (iPoolSize>hardlimit)
			throw new IllegalArgumentException("prefered pool size must be less than or equal to max pool size ");

		reapConnections();
		poolsize = iPoolSize;
	}

	// ---------------------------------------------------------

	/**
	 * <p>Set maximum concurrent open connections limit</p>
	 * The default value is 100.<br>
	 * If iMaxConnections is set to zero then the connection pool is effectively
	 * turned off and no pooling occurs.
	 * @param iMaxConnections Absolute maximum for opened connections
	 */
	public void setMaxPoolSize(int iMaxConnections) {

		if (iMaxConnections==0) {
			reapConnections();
			poolsize = hardlimit = 0;
		} else {
			if (iMaxConnections<poolsize)
				throw new IllegalArgumentException("max pool size must be greater than or equal to prefered pool size ");

			reapConnections();
			hardlimit = iMaxConnections;
		}
	}

	// ---------------------------------------------------------

	/**
	 * <p>Absolute maximum allowed for concurrent opened connections.</p>
	 * The default value is 100.
	 */
	public int getMaxPoolSize() {
		return hardlimit;
	}

	// ---------------------------------------------------------

	/**
	 * Get LogWriter from java.sql.DriverManager
	 */
	public PrintWriter getLogWriter() throws SQLException {
		return DriverManager.getLogWriter();
	}

	// ---------------------------------------------------------

	/**
	 * Set LogWriter for java.sql.DriverManager
	 */
	public void setLogWriter(PrintWriter printwrt) throws SQLException {
		DriverManager.setLogWriter(printwrt);
	}

	// ---------------------------------------------------------

	/**
	 * Get login timeout from java.sql.DriverManager
	 */
	public int getLoginTimeout() throws SQLException {
		return DriverManager.getLoginTimeout();
	}

	// ---------------------------------------------------------

	/**
	 * Set login timeout for java.sql.DriverManager
	 */
	public void setLoginTimeout(int seconds) throws SQLException {
		DriverManager.setLoginTimeout(seconds);
	}

	// ---------------------------------------------------------

	/**
	 * <p>Get staled connection threshold</p>
	 * The default value is 600000ms (10 mins.)<br>
	 * This implies that all database operations done using connections
	 * obtained from the pool must be completed before 10 minutes or else
	 * they can be closed by the connection reaper before their normal finish.
	 * @return The maximum amount of time in miliseconds that a JDCConnection
	 * can be opened and not used before considering it staled.
	 */
	public long getTimeout() {
		return timeout;
	}

	// ---------------------------------------------------------

	/**
	 * <p>Set staled connection threshold</p>
	 * @param miliseconds The maximum amount of time in milliseconds that a JDCConnection
	 * can be opened and not used before considering it staled.<BR>
	 * Default value is 600000ms (10 mins.) Minimum value is 1000.
	 * @throws IllegalArgumentException If milliseconds<1000
	 */

	public void setTimeout(long miliseconds)
			throws IllegalArgumentException {

		if (miliseconds<1000l)
			throw new IllegalArgumentException("Connection timeout must be at least 1000 miliseconds");

		timeout = miliseconds;
	}

	/**
	 * Delay between connection reaper executions
	 * @return long Number of milliseconds
	 */
	public long getReaperDaemonDelay() {
		if (reaper!=null)
			return reaper.getDelay();
		else
			return 0l;
	}

	/**
	 * Set delay between connection reaper executions (default value is 5 mins)
	 * @param lDelayMs long Milliseconds
	 * @throws IllegalArgumentException if lDelayMs is less than 1000
	 */
	public void setReaperDaemonDelay(long lDelayMs) throws IllegalArgumentException {
		if (lDelayMs>0l) {
			if (reaper==null) reaper = new JDCConnectionReaper(this);
			reaper.setDelay(lDelayMs);
		}
		else {
			if (reaper!=null) reaper.halt();
			reaper=null;
		}
	}

	// ---------------------------------------------------------

	/**
	 * Close and remove one connection from the pool
	 * @param conn Connection to close
	 */
	public void disposeConnection(JDCConnection conn) {
		boolean bClosed;
		String sCaller = "";

		try {

			sCaller = conn.getName();
			if (!conn.isClosed()) {         	
				conn.getConnection().close();
				conn.notifyClose();
			}
			conn.expireLease();
			if (DebugFile.trace && (null!=sCaller)) modifyMap(sCaller,-1);
			bClosed = true;
		}
		catch (SQLException e) {
			bClosed = false;

			if (errorlog.size()>100) errorlog.poll();
			
			errorlog.add(new Date().toString() + " " + sCaller + " Connection.close() " + e.getMessage());

			if (DebugFile.trace) DebugFile.writeln("SQLException at JDCConnectionPool.disposeConnection() : " + e.getMessage());
		}

		if (bClosed) {
			if (DebugFile.trace) DebugFile.writeln("connections.removeElement(" + String.valueOf(openconns) + ")");
			connections.removeElement(conn);
			openconns--;
		}
	} // disposeConnection()

	// ---------------------------------------------------------

	/**
	 * Called from the connection reaper daemon thread every n-minutes for maintaining the pool clean
	 */
	synchronized void reapConnections() {

		if (DebugFile.trace) {
			DebugFile.writeln("Begin JDCConnectionPool.reapConnections()");
			DebugFile.incIdent();
		}

		long stale = System.currentTimeMillis() - timeout;
		Enumeration<JDCConnection> connlist = connections.elements();
		JDCConnection conn;

		while((connlist != null) && (connlist.hasMoreElements())) {
			conn = (JDCConnection) connlist.nextElement();

			// Remove each connection that is not in use and not participating in a transaction
			// or is stalled for more than maximum usage timeout (default 10 mins)
			if (!conn.inUse() && !conn.isStarted())
				disposeConnection(conn);
			else if (stale>conn.getLastUse()) {
				if (DebugFile.trace) DebugFile.writeln("Connection "+conn.getName()+" was staled since "+new Date(conn.getLastUse()).toString());
				if (errorlog.size()>maxerrors) errorlog.poll();
				errorlog.add(new Date().toString()+" Connection "+conn.getName()+" was staled since "+new Date(conn.getLastUse()).toString());
				disposeConnection(conn);
			}
		} // wend

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End JDCConnectionPool.reapConnections() : " + new Date().toString());
		}
	} // reapConnections()

	// ---------------------------------------------------------

	/**
	 * Close all connections from the pool regardless of their current state
	 */
	public void closeConnections() {

		Enumeration connlist;

		if (DebugFile.trace) {
			DebugFile.writeln("Begin JDCConnectionPool.closeConnections()");
			DebugFile.incIdent();
		}

		connlist = connections.elements();

		if (connlist != null) {
			while (connlist.hasMoreElements()) {
				disposeConnection ((JDCConnection) connlist.nextElement());
			} // wend
		} // fi ()

		if (DebugFile.trace) callers.clear();

		connections.clear();

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End JDCConnectionPool.closeConnections() : " + String.valueOf(openconns));
		}

		openconns = 0;
	} // closeConnections()

	// ---------------------------------------------------------

	/**
	 * Close connections from the pool not used for a longer time
	 * @return Count of staled connections closed
	 */
	public int closeStaledConnections() {

		JDCConnection conn;
		Enumeration connlist;

		if (DebugFile.trace) {
			DebugFile.writeln("Begin JDCConnectionPool.closeStaledConnections()");
			DebugFile.incIdent();
		}

		int staled = 0;
		final long stale = System.currentTimeMillis() - timeout;

		connlist = connections.elements();

		if (connlist != null) {
			while (connlist.hasMoreElements()) {
				conn = (JDCConnection) connlist.nextElement();
				if (stale>conn.getLastUse()) {
					staled++;
					disposeConnection (conn);
				}
			} // wend
		} // fi ()

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End JDCConnectionPool.closeStaledConnections() : " + String.valueOf(staled));
		}

		return staled;
	} // closeStaledConnections()

	// ---------------------------------------------------------

	/**
	 * Get an array with references to all pooled connections
	 */
	public synchronized JDCConnection[] getAllConnections() {
		int iConnections = connections.size();
		JDCConnection[] aConnections = new JDCConnection[iConnections];

		for (int c=0; c<iConnections; c++)
			aConnections[c] = (JDCConnection) connections.get(c);

		return aConnections;
	} // getAllConnections

	// ---------------------------------------------------------

	private JDCConnection leaseExistingConnection(String sCaller) {
		final int s = connections.size();
		final long tid = Thread.currentThread().getId();
		if (DebugFile.trace) {
			StringBuilder pcnns = new StringBuilder();
			pcnns.append("Pooled connections for " + sCaller + " [");
			for (int i = 0; i < s; i++) {
				JDCConnection j = (JDCConnection) connections.elementAt(i);
				boolean closed = true;
				try {
					closed = j.isClosed();
				} catch (SQLException e) { }
				if (i>0) pcnns.append(",");
				pcnns.append("{").append(j.getId()).append(" : ").append(j.isStarted() ? "started" : "not started").append(" ").append(j.inUse() ? "in use" : "not in use").append(" ").append(closed ? "closed" : "open").append("}");
			}
			pcnns.append("]");
			DebugFile.writeln(pcnns.toString());
		}
		for (int i = 0; i < s; i++) {
			JDCConnection j = (JDCConnection) connections.elementAt(i);
			if (j.isStarted() && j.getThreadId()==tid) {
				if (j.lease(sCaller)) {
					if (DebugFile.trace) DebugFile.writeln("leaseExistingConnection(" + sCaller + ") cid=" + j.getId() + " " + (j.isStarted() ? "started" : "not started") + " for thread " + j.getThreadId());
					return j;
				}
			}
		}
		for (int i = 0; i < s; i++) {
			JDCConnection j = (JDCConnection) connections.elementAt(i);
			if (j.getThreadId()==-1l) {
				if (j.lease(sCaller)) {
					if (DebugFile.trace) DebugFile.writeln("leaseExistingConnection(" + sCaller + ") cid=" + j.getId() + " " + (j.isStarted() ? "started" : "not started") + " for thread " + j.getThreadId());
					return j;
				}
			}
		}
		return null;
	}

    // ---------------------------------------------------------

	private JDCConnection leaseNewConnection(String sCaller) throws SQLException {
		JDCConnection j;
		Connection c;

		if (openconns==hardlimit) {
			if (DebugFile.trace) DebugFile.decIdent();
			throw new SQLException ("Maximum number of " + String.valueOf(hardlimit) + " concurrent connections exceeded","08004");
		}

		if (DebugFile.trace) DebugFile.writeln("  DriverManager.getConnection(" + url + ", ...)");

		if (user==null && password==null)
			c = DriverManager.getConnection(url);
		else
			c = DriverManager.getConnection(url, user, password);

		if (null!=c) {
			j = new JDCConnection(c, this);
			j.lease(sCaller);

			if (DebugFile.trace) {
				DebugFile.writeln("  JDCConnectionPool miss for (" + url + ", ...)");
				DebugFile.writeln("  Vector<JDCConnection>.addElement("+j+")");
			}

			connections.addElement(j);
			c = null;

			if (DebugFile.trace) DebugFile.writeln("leaseNewConnection " + j.getId() +  " " + (j.isStarted()? "started" : "not started"));

		} else {

			if (DebugFile.trace) DebugFile.writeln("JDCConnectionPool.getConnection() DriverManager.getConnection() returned null value");
			j = null;

		}

		if (null!=j) openconns++;
		
		return j;
	}

	// ---------------------------------------------------------

	/**
	 * Get a connection from the pool
	 * @param sCaller This is just an information parameter used for open/closed
	 * mismatch tracking and other benchmarking and statistical purposes.
	 * @return Opened JDCConnection
	 * @throws SQLException If getMaxPoolSize() opened connections is reached an
	 * SQLException with SQLState="08004" will be raised upon calling getConnection().<br>
	 * <b>Microsoft SQL Server</b>: Connection reuse requires that SelectMethod=cursor was
	 * specified at connection string.
	 */

	public synchronized JDCConnection getConnection(String sCaller) throws SQLException {

		JDCConnection j;
		Connection c;
		Chronometer oChn = null;

		if (DebugFile.trace) {
			oChn = new Chronometer();
			DebugFile.writeln("Begin JDCConnectionPool.getConnection(" + (sCaller!=null ? sCaller : "") + ")");
			DebugFile.incIdent();
			DebugFile.writeln("Thread Id = " + Thread.currentThread().getId());
		}

		if (hardlimit==0) {

			// If hardlimit==0 Then connection pool is turned off so return a connection
			// directly from the DriverManager
			if (user==null && password==null)
				c = DriverManager.getConnection(url);
			else
				c = DriverManager.getConnection(url, user, password);
			j = new JDCConnection(c,null);

		} else {

			j = leaseExistingConnection(sCaller);

			if (null==j)
				j = leaseNewConnection(sCaller);

			if (DebugFile.trace ) {
				if (sCaller!=null) modifyMap(sCaller, 1);
			} // DebugFile()
		} // fi (hardlimit==0)

		if (DebugFile.trace ) {
			DebugFile.writeln("got " + (j.isStarted() ? "started" : "not started") + " connection in "+String.valueOf(oChn.stop())+" ms");
			DebugFile.decIdent();
			DebugFile.writeln("End JDCConnectionPool.getConnection() : " + j.getId());
		} // DebugFile()

		return j;
	} // getConnection()

	// ---------------------------------------------------------

	/**
	 * Get a connection from the pool
	 * @return Opened PooledConnection
	 * @throws SQLException If getMaxPoolSize() opened connections is reached an
	 * SQLException with SQLState="08004" will be raised upon calling getConnection().<br>
	 * <b>Microsoft SQL Server</b>: Connection reuse requires that SelectMethod=cursor was
	 * specified at connection string.
	 */

	public synchronized PooledConnection getPooledConnection() throws SQLException {
		return (PooledConnection) getConnection(null);
	}

	// ---------------------------------------------------------

	/**
	 * Get a connection bypassing the pool and connection directly to the database with the given user and password
	 * @param sUser
	 * @param sPasswd
	 * @return Opened Connection
	 * @throws SQLException
	 */

	public synchronized PooledConnection getPooledConnection(String sUser, String sPasswd) throws SQLException {
		if (sUser==null && sPasswd==null)
			return (PooledConnection) new JDCConnection(DriverManager.getConnection(url),null);
		else
			return (PooledConnection) new JDCConnection(DriverManager.getConnection(url, sUser, sPasswd),null);
	}

	// ---------------------------------------------------------------------------

	/**
	 * This method is added for compatibility with Java 7 and it is not implemented
	 * @return null
	 */
	public Logger getParentLogger() {
		return null;
	}

	// ---------------------------------------------------------

	/**
	 * Get connection for a server process identifier
	 * @param sPId String Operating system process identifier at server side
	 * @return JDCConnection or <b>null</b> if no connection for such pid was found
	 * @throws SQLException
	 * @since 2.2
	 */
	public JDCConnection getConnectionForPId(String sPId) throws SQLException {
		String pid;
		JDCConnection conn;
		Enumeration connlist = connections.elements();
		if (connlist != null) {
			while(connlist.hasMoreElements()) {
				conn = (JDCConnection) connlist.nextElement();
				try {
					pid = conn.pid();
				} catch (Exception ignore) { pid=null; }
				if (sPId.equals(pid))
					return conn;
			} // wend
		} // fi ()
		return null;
	} // getConnectionForPId

	// ---------------------------------------------------------

	/**
	 * Return a connection to the pool
	 * @param conn JDCConnection returned to the pool
	 */

	public synchronized void returnConnection(JDCConnection conn) {
		if (DebugFile.trace) {
			DebugFile.writeln("JDCConnectionPool.returnConnection(["+conn.getId()+"])");
			if (!connections.contains(conn))
				DebugFile.writeln("Warning: JDCConnection "+conn.getId()+" "+(conn.getName()==null ? "" : conn.getName())+" is not pooled");
			DebugFile.writeln("JDCConnection.expireLease() on connection " + conn.getId() + (conn.getThreadId()==-1l ? "" : " for thread " + conn.getThreadId()));
		}
		
		conn.expireLease();

		if (DebugFile.trace) {
			if (null!=conn.getName())
				if (conn.getName().length()>0)
				  modifyMap(conn.getName(), -1);
		}
	} // returnConnection()

	// ---------------------------------------------------------

	/**
	 * Return a connection to the pool
	 * @param conn JDCConnection returned to the pool
	 * @param sCaller Must be the same String passed as parameter at getConnection()
	 */

	public synchronized void returnConnection(JDCConnection conn, String sCaller) {

		if (DebugFile.trace) {
			DebugFile.writeln("JDCConnectionPool.returnConnection(["+conn.getId()+"], "+sCaller+")");
			if (!connections.contains(conn))
				DebugFile.writeln("Warning: JDCConnection "+conn.getId()+" "+sCaller+" is not pooled");
			DebugFile.writeln("JDCConnection.expireLease()  on connection " + conn.getId() + (conn.getThreadId()==-1l ? "" : " for thread " + conn.getThreadId()));
		}
		
		conn.expireLease();

		if (DebugFile.trace) {
			if (null!=sCaller) modifyMap(sCaller, -1);
		}
	}

	// ---------------------------------------------------------

	/**
	 * @return Actual connection pool size
	 */

	public int size() {
		return openconns;
	}

	// ---------------------------------------------------------
	
	/**
	 * Get information of current activity at database to which this pool is connected
	 * @return JDCActivityInfo
	 * @throws SQLException
	 */
	public JDCActivityInfo getActivityInfo() throws SQLException {
		JDCActivityInfo oInfo;
		try {
			oInfo = new JDCActivityInfo(this);
		} catch (Exception xcpt) {
			throw new SQLException ("JDCActivityInfo.getActivityInfo() "+xcpt.getClass().getName()+" "+xcpt.getMessage());
		}
		return oInfo;
	} // getActivityInfo()
	
	// ---------------------------------------------------------

	/**
	 * <p>Get next value for a sequence</p>
	 * @param sSequenceName Sequence name.
	 * In MySQL and SQL Server sequences are implemented using row locks at k_sequences table.
	 * @return long Next sequence value
	 * @throws SQLException
	 * @throws UnsupportedOperationException Not all databases support sequences.
	 * On Oracle and PostgreSQL, native SEQUENCE objects are used,
	 * on Microsoft SQL Server the stored procedure k_sp_nextval simulates sequences,
	 * this function is not supported on other DataBase Management Systems.
	 */   
	public long nextVal(String sSequenceName) throws JDOException {
		long iNextVal;
		JDCConnection oConn = null;
		try {
			oConn = getConnection("nextVal."+sSequenceName);
			Statement oStmt;
			ResultSet oRSet;
			CallableStatement oCall;
			final int iDBMS = oConn.getDataBaseProduct();
			
			if (iDBMS==RDBMS.MYSQL.intValue() || iDBMS==RDBMS.MSSQL.intValue()) {

				oCall = oConn.prepareCall("{call k_sp_nextval (?,?)}");
				oCall.setString(1, sSequenceName);
				oCall.registerOutParameter(2, java.sql.Types.INTEGER);
				oCall.execute();
				iNextVal = oCall.getInt(2);
				oCall.close();
				oCall = null;

			} else if (iDBMS==RDBMS.POSTGRESQL.intValue()) {
				
				oStmt = oConn.createStatement();
				oRSet = oStmt.executeQuery("SELECT nextval('" + sSequenceName + "')");
				oRSet.next();
				iNextVal = oRSet.getInt(1);
				oRSet.close();
				oStmt.close();

			} else if (iDBMS==RDBMS.ORACLE.intValue()) {
				oStmt = oConn.createStatement();
				oRSet = oStmt.executeQuery("SELECT " + sSequenceName + ".NEXTVAL FROM dual");
				oRSet.next();
				iNextVal = oRSet.getInt(1);
				oRSet.close();
				oStmt.close();

			} else {
				throw new UnsupportedOperationException("function nextVal() not supported on current DBMS");
			}
		} catch (Exception xcpt) {
			throw new JDOException(xcpt.getClass().getName()+" "+xcpt.getMessage(), xcpt);
		} finally {
			try {
				if (oConn!=null)
					if (!oConn.isClosed())
						oConn.close("nextVal."+sSequenceName);
			} catch (SQLException sqle) { }
		}
		return iNextVal;
	}

	// ---------------------------------------------------------

	/**
	 * <p>Human readable usage statistics</p>
	 * @return Connection pool usage statistics string
	 * @throws ConcurrentModificationException If pool is modified while iterating
	 * through connection collection
	 */
	public String dumpStatistics()
			throws ConcurrentModificationException {
		String sDump;
		String sPId;
		Object sKey;
		Object iVal;
		JDCConnection oCinf = null;
		int iConnOrdinal, iStaled;
		long stale = System.currentTimeMillis() - timeout;
		SimpleDateFormat oFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		if (DebugFile.trace) {
			DebugFile.writeln("Begin JDCConnectionPool.dumpStatistics()");
			DebugFile.incIdent();
		}

		Enumeration connlist = connections.elements();
		JDCConnection conn;

		sDump = "Maximum Pool Size=" + String.valueOf(poolsize) + "\n";
		sDump += "Maximum Connections=" + String.valueOf(hardlimit) + "\n";
		sDump += "Connection Timeout=" + String.valueOf(timeout) + " ms\n";
		sDump += "Reaper Daemon Delay=" + String.valueOf(getReaperDaemonDelay()) + " ms\n";
		sDump += "\n";

		iStaled = iConnOrdinal = 0;

		if (connlist != null) {
			while (connlist.hasMoreElements()) {
				conn = (JDCConnection) connlist.nextElement();

				if (stale>conn.getLastUse()) iStaled++;

				try {
					sPId = conn.pid();
				} catch (Exception ignore) { sPId=null; }

				sDump += "#" + String.valueOf(++iConnOrdinal) + (conn.inUse() ? " in use, " : " vacant, ") + (stale>conn.getLastUse() ? " staled," : " ready,") + (conn.validate() ?  "validate=yes" : " validate=no") + ", last use=" + new Date(conn.getLastUse()).toString() + ", " + (conn.inUse() ? "caller=" + conn.getName() : "") + (sPId==null ? "" : " pid="+sPId) + "\n";
			}
		} // fi ()

		sDump += "\n";

		if (DebugFile.trace) {
			Iterator oCallersIterator = callers.keySet().iterator();

			while (oCallersIterator.hasNext()) {
				sKey = oCallersIterator.next();
				iVal = callers.get(sKey);
				if (!iVal.toString().equals("0")) sDump += sKey + " , " + iVal.toString() + " named open connections\n";
			}
			sDump += "\n\n";
		} // fi (DebugFile.trace)

		sDump += String.valueOf(iStaled) + " staled connections\n";

		sDump += "Actual pool size " + String.valueOf(size()) + "\n\n";

		
		try {
			oCinf = new JDCConnection(DriverManager.getConnection(url, user, password), this, "");
			JDCActivityInfo oAinf = new JDCActivityInfo(oCinf);
			if (oAinf==null) {
				sDump += "no activity info available";
			} else {
				JDCProcessInfo[] oPinfo = oAinf.processesInfo();
				if (oPinfo!=null) {
					sDump += "Activity information:\n";
					for (int p=0; p<oPinfo.length; p++) {
						sDump += new Date().toString()+" user "+oPinfo[p].getUserName()+" running process "+oPinfo[p].getProcessId();
						conn = getConnectionForPId(oPinfo[p].getProcessId());
						if (conn!=null) {
							sDump += " on connection "+conn.getName();
						}
						if (oPinfo[p].getQueryText()!=null) {
							if (oPinfo[p].getQueryText().length()>0) {
								if (oPinfo[p].getQueryText().equals("<IDLE>"))
									sDump += " for idle query";
								else
									sDump += " for query "+oPinfo[p].getQueryText();
							} // fi (getQueryText()!="")
						} // fi (getQueryText()!=null)
						if (oPinfo[p].getQueryStart()!=null) {
							sDump += " since "+oFmt.format(oPinfo[p].getQueryStart());
						}
						sDump += "\n";
					} // next
					JDCLockConflict[] oLocks = oAinf.lockConflictsInfo();
					if (oLocks!=null) {
						sDump += "Locks information:\n";
						for (int l=0; l<oLocks.length; l++) {
							sDump += "PID "+String.valueOf(oLocks[l].getPID())+ " query "+oLocks[l].getQuery()+" is waiting on PID "+String.valueOf(oLocks[l].getWaitingOnPID())+" query "+oLocks[l].getWaitingOnQuery()+"\n";
						} // next           
					} // fi
				}
			}
			oCinf.close();
			oCinf=null;
		} catch (Exception xcpt) {
			sDump += xcpt.getClass().getName()+" trying to get activity information "+xcpt.getMessage()+"\n";
			try { sDump += StackTraceUtil.getStackTrace(xcpt); } catch (Exception ignore) { }
			try { if (oCinf!=null) if (!oCinf.isClosed()) oCinf.close(); } catch (Exception ignore) { }
		}

		sDump += "\n";

		if (errorlog.size()>0) {
			sDump += "Fatal error log:\n";
			Iterator<String> oErrIterator = errorlog.iterator();
			while (oErrIterator.hasNext()) sDump += oErrIterator.next()+"\n";
		} // fi

		DebugFile.writeln(sDump);

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End JDCConnectionPool.dumpStatistics()");
		}

		return sDump;
	} // dumpStatistics

	// ============================================================================
	// com.knowgate.storage.DataSource interface implementation

	public boolean isReadOnly() {
		return false;
	}


	// ============================================================================

} // JDCConnectionPool
