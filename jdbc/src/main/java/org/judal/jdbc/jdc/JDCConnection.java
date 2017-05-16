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
import java.util.List;
import java.util.concurrent.Executor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
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
* JDBC Connection Wrapper
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

	private Connection conn;	
	private JDCConnectionPool pool;
	private boolean inuse;
		
	private static final String DBMSNAME_MSSQL = RDBMS.MSSQL.toString();
	private static final String DBMSNAME_POSTGRESQL = RDBMS.POSTGRESQL.toString();
	private static final String DBMSNAME_ORACLE = RDBMS.ORACLE.toString();
	private static final String DBMSNAME_MYSQL = RDBMS.MYSQL.toString();
	private static final String DBMSNAME_XBASE = RDBMS.XBASE.toString();
	private static final String DBMSNAME_ACCESS = RDBMS.ACCESS.toString();
	private static final String DBMSNAME_SQLITE = RDBMS.SQLITE.toString();
	private static final String DBMSNAME_HSQLDB = RDBMS.HSQLDB.toString();

	public JDCConnection(Connection conn, JDCConnectionPool pool, String schemaname) {
		this.dbms = RDBMS.UNKNOWN.intValue();
		this.conn=conn;
		this.pool=pool;
		this.inuse=false;
		this.timestamp=0;
		this.name = null;
		this.schema=schemaname;
		listeners = new LinkedList<ConnectionEventListener>();
	}

	public JDCConnection(Connection conn, JDCConnectionPool pool) {
		this.dbms = RDBMS.UNKNOWN.intValue();
		this.conn=conn;
		this.pool=pool;
		this.inuse=false;
		this.timestamp=0;
		this.name = null;
		this.schema=null;
		listeners = new LinkedList<ConnectionEventListener>();
	}

	@Override
	protected void startResource(int flags) throws XAException {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin JDCConnection.startResource("+String.valueOf(flags)+")");
			DebugFile.incIdent();
			DebugFile.writeln("JDCConnection.setAutoCommit(false)");
		}
		try {
			conn.setAutoCommit(false);
		} catch (SQLException sqle) {
			if (DebugFile.trace) DebugFile.decIdent();
			throw new XAException(sqle.getMessage());
		}
		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End JDCConnection.startResource()");
		}
	}
	
	@Override
	protected int prepareResource() throws XAException {
		try {
			return conn.isReadOnly() ? XAResource.XA_RDONLY : XAResource.XA_OK;
		} catch (SQLException sqle) {
			throw new XAException(sqle.getMessage());
		}
	}

	@Override
	protected void commitResource() throws XAException {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin JDCConnection.commitResource()");
			DebugFile.incIdent();
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
			DebugFile.writeln("End JDCConnection.commitResource()");
		}
	}

	@Override
	protected void rollbackResource() throws XAException {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin JDCConnection.rollbackResource()");
			DebugFile.incIdent();
		}
		try {
			rollback();
		} catch (SQLException sqle) {
			if (DebugFile.trace) {
				DebugFile.writeln("SQLException " + sqle.getMessage());
				DebugFile.decIdent();
			}
			throw new XAException(sqle.getMessage());
		}
		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End JDCConnection.rollbackResource()");
		}
	}
	
	@Override
	protected void endResource(int flag) throws XAException {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin JDCConnection.endResource("+String.valueOf(flag)+")");
			DebugFile.incIdent();
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
		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End JDCConnection.endResource()");
		}
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	public boolean inUse() {
		return inuse;
	}	
	
	@Override
	public void addConnectionEventListener(ConnectionEventListener listener) {
		listeners.add(listener);
	}

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

	@Override
	public void addStatementEventListener(StatementEventListener listener) {
		throw new UnsupportedOperationException("JDCConnection.addStatementEventListener() is not implemented");
	}

	@Override
	public void removeStatementEventListener(StatementEventListener listener) {
		throw new UnsupportedOperationException("JDCConnection.removeStatementEventListener() is not implemented");
	}

	public boolean lease(String sConnectionName) {
		if (inuse) {
			return false;
		} else {
			inuse=true;
			name = sConnectionName;
			timestamp=System.currentTimeMillis();
			return true;
		}
	}

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

	public JDCConnectionPool getPool() {
		return pool;
	}

	public long getLastUse() {
		return timestamp;
	}

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
			else
				return RDBMS.GENERIC.intValue();
		}
		catch (NullPointerException npe) {
			if (DebugFile.trace) DebugFile.writeln("NullPointerException at JDCConnection.getDataBaseProduct()");
			return RDBMS.GENERIC.intValue();
		}
	}

	public int getDataBaseProduct() throws SQLException {
		if (RDBMS.UNKNOWN.intValue()==dbms)
			dbms = getDataBaseProduct(conn);
		return dbms;
	}

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

	public void setSchemaName(String sname) {
		schema = sname;
	}

	@Override
	public void close()  {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin JDCConnection.close()");
			DebugFile.incIdent();
		}

		if (pool==null) {
			inuse = false;
			name = null;
			try {
	      conn.close();
    } catch (SQLException e) { }
			notifyClose();
		}
		else {
			try { setAutoCommit(true); }
			catch (SQLException sqle) { DebugFile.writeln("SQLException setAutoCommit(true) "+sqle.getMessage()); } 
			try { setReadOnly(false); }
			catch (SQLException sqle) { DebugFile.writeln("SQLException setReadOnly(false) "+sqle.getMessage()); } 
			pool.returnConnection(this);
		}

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End JDCConnection.close()");
		}
	}
	
	public void close(String sCaller) throws SQLException {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin JDCConnection.close("+sCaller+")");
			DebugFile.incIdent();
		}
		if (pool==null) {
			inuse = false;
			name = null;
			conn.close();
			notifyClose();
		}
		else {
			try { setAutoCommit(true); }
			catch (SQLException sqle) { DebugFile.writeln("SQLException setAutoCommit(true) "+sqle.getMessage()); } 
			try { setReadOnly(false); }
			catch (SQLException sqle) { DebugFile.writeln("SQLException setReadOnly(false) "+sqle.getMessage()); } 
			pool.returnConnection(this, sCaller);
		}

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End JDCConnection.close("+sCaller+")");
		}
	}

	public void dispose() {
		try { if (!getAutoCommit()) rollback(); } catch (SQLException ignore) { }
		pool.returnConnection(this);
		pool.disposeConnection(this);
	}

	public void dispose(String sCaller) {
		try { if (!getAutoCommit()) rollback(); } catch (SQLException ignore) { }
		pool.returnConnection(this, sCaller);
		pool.disposeConnection(this);
	}

	protected void expireLease() {
		inuse=false;
		name =null ;
	}

	@Override
	public Connection getConnection() {
		return conn;
	}

	@Override
	public Connection getNativeConnection() {
		return conn;		
	}
	
	@Override
	public boolean isWrapperFor(Class c) {
		return c.getClass().getName().equals("java.sql.Connection");
	}

	@Override
	public Object unwrap(Class c) {
		return c.getClass().getName().equals("java.sql.Connection") ? conn : null;
	}

	@Override
	public Array createArrayOf(String typeName, Object[] attributes) throws SQLException {
		return conn.createArrayOf(typeName, attributes);
	}

	@Override
	public Blob createBlob() throws SQLException {
		return conn.createBlob();
	}

	@Override
	public Clob createClob() throws SQLException {
		return conn.createClob();
	}

	@Override
	public NClob createNClob() throws SQLException {
		return conn.createNClob();
	}

	@Override
	public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
		return conn.createStruct(typeName, attributes);
	}

	@Override
	public SQLXML createSQLXML() throws SQLException, SQLFeatureNotSupportedException {
		throw new SQLFeatureNotSupportedException("JDCConnection.createSQLXML() not implemented");
	}

	@Override
	public Statement createStatement(int i, int j) throws SQLException {
		return conn.createStatement(i,j);
	}

	@Override
	public Statement createStatement(int i, int j, int k) throws SQLException {
		return conn.createStatement(i,j,k);
	}

	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		return conn.prepareStatement(sql);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] params) throws SQLException {
		return conn.prepareStatement(sql,params);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int i) throws SQLException {
		return conn.prepareStatement(sql,i);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int i, int j) throws SQLException {
		return conn.prepareStatement(sql,i,j);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int i, int j, int k) throws SQLException {
		return conn.prepareStatement(sql,i,j,k);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] params) throws SQLException {
		return conn.prepareStatement(sql,params);
	}

	@Override
	public CallableStatement prepareCall(String sql) throws SQLException {
		return conn.prepareCall(sql);
	}

	@Override
	public CallableStatement prepareCall(String sql, int i, int j) throws SQLException {
		return conn.prepareCall(sql, i , j);
	}

	@Override
	public CallableStatement prepareCall(String sql, int i, int j, int k) throws SQLException {
		return conn.prepareCall(sql, i , j, k);
	}

	@Override
	public Statement createStatement() throws SQLException {
		return conn.createStatement();
	}

	@Override
	public String nativeSQL(String sql) throws SQLException {
		return conn.nativeSQL(sql);
	}

	@Override
	public java.util.Properties getClientInfo() throws SQLException {
		return null;
	}

	@Override
	public void setClientInfo(java.util.Properties props) throws SQLClientInfoException {
		throw new UnsupportedOperationException("JDCConnection.setClientInfo() Not implemented");
	}

	@Override
	public String getClientInfo(String name) throws SQLException {
		return null;
	}

	@Override
	public void setClientInfo(String name, String value) {
		throw new UnsupportedOperationException("JDCConnection.setClientInfo() Not implemented");
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		conn.setAutoCommit(autoCommit);
	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		return conn.getAutoCommit();
	}

	@Override
	public int getHoldability() throws SQLException {
		return conn.getHoldability();
	}

	@Override
	public void setHoldability(int h) throws SQLException {
		conn.setHoldability(h);
	}

	@Override
	public Savepoint setSavepoint() throws SQLException {
		return conn.setSavepoint();
	}

	@Override
	public Savepoint setSavepoint(String s) throws SQLException {
		return conn.setSavepoint(s);
	}

	@Override
	public void commit() throws SQLException {
		if (conn.getAutoCommit())
			throw new SQLException("Can't commit Connection when autocommit is true");

		conn.commit();
	}

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

	@Override
	public boolean isClosed() throws SQLException {
		return conn.isClosed();
	}

	@Override
	public boolean isValid(int timeout) throws SQLException {
		return conn.isClosed();
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		return conn.getMetaData();
	}

	@Override
	public void setReadOnly(boolean readOnly) throws SQLException {
		conn.setReadOnly(readOnly);
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		return conn.isReadOnly();
	}

	@Override
	public void setCatalog(String catalog) throws SQLException {
		conn.setCatalog(catalog);
	}

	@Override
	public String getCatalog() throws SQLException {
		return conn.getCatalog();
	}

	@Override
	public void setTransactionIsolation(int level) throws SQLException {
		conn.setTransactionIsolation(level);
	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		return conn.getTransactionIsolation();
	}

	@Override
	public Map getTypeMap() throws SQLException {
		return conn.getTypeMap();
	}

	@Override
	public void setTypeMap(Map typemap) throws SQLException {
		conn.setTypeMap(typemap);
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		return conn.getWarnings();
	}

	@Override
	public void clearWarnings() throws SQLException {
		conn.clearWarnings();
	}

	@Override
	public void releaseSavepoint(Savepoint p) throws SQLException {
		conn.releaseSavepoint(p);
	}


	/**
	 * <p>Get operating system process identifier for this connection</p>
	 * @return String For PostgreSQL the id of the UNIX process attending this connection.
	 * For Oracle a Session Id.
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
			
			// if (DebugFile.trace) {
			// DebugFile.writeln("JDCConnection.bindParameter("+iParamIndex+"," +
			// 		(null==oParamValue ? "null" : oParamValue.getClass().getName()+" "+oParamValue.toString()) + ","+ 
			// 		ColumnDef.typeName(iSQLType)+")");
			// }

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
						LatLong oLatLng = (LatLong) oParamValue;
						if (DebugFile.trace) DebugFile.writeln("binding parameter " + String.valueOf(iParamIndex) + " LatLong lattitude "+ String.valueOf(oLatLng.getLattitude()) +" as SQL DOUBLE");
						if (Double.isNaN(oLatLng.getLattitude()))
						  oStmt.setNull(iParamIndex, Types.DOUBLE);
						else
						  oStmt.setDouble(iParamIndex, oLatLng.getLattitude());
						if (DebugFile.trace) DebugFile.writeln("binding parameter " + String.valueOf(iParamIndex+1) + " LatLong longitude "+ String.valueOf(oLatLng.getLongitude()) +" as SQL DOUBLE");
						if (Double.isNaN(oLatLng.getLongitude()))
					      oStmt.setNull(iParamIndex+1, Types.DOUBLE);
						else
						  oStmt.setDouble(iParamIndex+1, oLatLng.getLongitude());
						nBinded = 2;              		
					} else if (oParamValue instanceof Map) {
					  try {
					    Class cPgObj = Class.forName("org.postgresql.util.PGobject");
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
					} else if (oParamValue instanceof String) {
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
						}
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

	@Override
	public void abort(Executor oExec) throws SQLException {
		conn.abort(oExec);
	}

	// ---------------------------------------------------------------------------

	@Override
	public int getNetworkTimeout() throws SQLException {
		return conn.getNetworkTimeout();
	}

	// ---------------------------------------------------------------------------

	@Override
	public void setNetworkTimeout(Executor oExec, int iTimeout) throws SQLException {
		conn.setNetworkTimeout(oExec, iTimeout);
	}

	// ---------------------------------------------------------------------------

	@Override
	public String getSchema() throws SQLException {
		return conn.getSchema();
	}

	// ---------------------------------------------------------------------------

	@Override
	public void setSchema(String sSchema) throws SQLException {
		conn.setSchema(sSchema);
	}

	@Override
	public boolean setTransactionTimeout(int seconds) {
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
