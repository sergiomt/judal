package org.judal.jdbc;

/**
 * © Copyright 2016 the original author.
 * This file is licensed under the Apache License version 2.0.
 * You may not use this file except in compliance with the license.
 * You may obtain a copy of the License at:
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.
 */

import java.io.PrintWriter;
import java.security.AccessControlException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Logger;

import javax.jdo.JDOException;
import javax.jdo.JDOUnsupportedOptionException;
import javax.jdo.datastore.JDOConnection;
import javax.jdo.datastore.Sequence;

import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import org.judal.jdbc.hsql.HsqlSequenceGenerator;
import org.judal.jdbc.jdc.JDCConnection;
import org.judal.jdbc.jdc.JDCConnectionPool;
import org.judal.jdbc.metadata.SQLFunctions;
import org.judal.jdbc.metadata.SQLTableDef;
import org.judal.jdbc.metadata.SQLViewDef;
import org.judal.jdbc.oracle.OrclSequenceGenerator;
import org.judal.jdbc.postgresql.PgSequenceGenerator;

import org.judal.metadata.SchemaMetaData;
import org.judal.metadata.TableDef;
import org.judal.metadata.ViewDef;
import org.judal.storage.Env;
import org.judal.storage.Param;
import org.judal.storage.DataSource;

import com.knowgate.debug.DebugFile;
import com.knowgate.debug.StackTraceUtil;
import com.knowgate.stringutils.Str;

/**
 * <p>Abstract base class for JDBC DataSource implementations.</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public abstract class JDBCDataSource extends JDCConnectionPool implements DataSource, javax.sql.DataSource {

	// *****************
	// Private Variables

	private Exception connectXcpt;
	public SQLFunctions Functions;
	private HashMap<String,String> props;
	private TransactionManager transactMan;

	protected String databaseProductName;
	protected RDBMS databaseProductId;
	protected SchemaMetaData metaData;
	protected boolean useDatabaseMetadata;
	protected boolean autocommit;

	private static final String VERSION = "1.0.0";

	/**
	 * 
	 * @param properties Map&lt;String,String&gt; Valid property names are listed at DataSource.PropertyNames
	 * @param transactManager TransactionManager
	 * @throws SQLException
	 * @throws AccessControlException
	 * @throws NumberFormatException
	 * @throws ClassNotFoundException
	 * @throws NullPointerException
	 * @throws UnsatisfiedLinkError
	 */
	public JDBCDataSource(Map<String,String> properties, TransactionManager transactManager)
			throws SQLException, AccessControlException, NumberFormatException,
			ClassNotFoundException, NullPointerException, UnsatisfiedLinkError {
		super(properties);
		props = new HashMap<String,String>(17);
		props.putAll(properties);
		if (DebugFile.trace) {
			StringBuilder b = new StringBuilder();
			for (Entry<String,String> e : props.entrySet())
				b.append(e.getKey()).append("=").append(e.getValue()).append(",");
			if (b.length()>0) b.setLength(b.length()-1);
			DebugFile.writeln("JDBCDataSource({" + b.toString() + "} "+ transactManager + " )");
		}
		String autoCommit = props.getOrDefault(AUTOCOMMIT, DEFAULT_AUTOCOMMIT);
		autocommit = autoCommit.equalsIgnoreCase("true")  || autoCommit.equalsIgnoreCase("yes") || autoCommit.equalsIgnoreCase("1");
		String metaDataFromDb = props.getOrDefault(USE_DATABASE_METADATA, DEFAULT_USE_DATABASE_METADATA);
		useDatabaseMetadata = metaDataFromDb.equalsIgnoreCase("true") || metaDataFromDb.equalsIgnoreCase("yes") || metaDataFromDb.equalsIgnoreCase("1") || metaDataFromDb.equalsIgnoreCase("");
		transactMan = transactManager;
		metaData = new SchemaMetaData();
		initialize();
	}

	private Object resultSetToListOfMap(Object result) throws SQLException {
		if (result instanceof ResultSet) {
			List<Map<String,Object>> results = new LinkedList<Map<String,Object>>();
			ResultSet rset = (ResultSet) result;
			ResultSetMetaData mdata = rset.getMetaData();
			int colCount = mdata.getColumnCount();
			String[] colNames = new String[colCount];
			for (int c=0; c<colCount;  c++)
				colNames[c] = mdata.getColumnName(c+1);
			while (rset.next()) {
				HashMap<String,Object> row = new HashMap<String,Object>(colCount*2);
				for (int c=0; c<colCount;  c++)
					row.put(colNames[c], rset.getObject(c+1));
				results.add(row);
			}
			rset.close();
			return results;
		} else {
			return result;
		}
	}
	
	/**
	 * <p>Call a stored procedure or PL/pgSQL function in the case of PostgreSQL</p>.
	 * The procedure will be invoked using JDBC syntax { call procedureName('param1.,.'para2') }
	 * except when using PostgreSQL in which case a SELECT procedureName('param1.,.'para2')
	 * will be performed.
	 * @param procedureName String Procedure name
	 * @param parameters Param...
	 * @return Object Procedure return value.
	 * If the procedure returns a ResultSet then it will be converted to List&lt;Map&lt;String,Object&gt;&gt;
	 */
	@Override
	public Object call(String procedureName, Param... parameters) throws JDOException {
		JDCConnection conn = null;
		Object retval;
		try {
			conn = getConnection("JDBCDataSource.call");
			String paramHolders = "";
			if (parameters!=null && parameters.length>0) {
				String[] qMarks = new String[parameters.length];
				Arrays.fill(qMarks, "?");
				paramHolders = String.join(",", qMarks);
			}
			if (conn.getDataBaseProduct()==RDBMS.POSTGRESQL.intValue()) {
				PreparedStatement stmt = conn.prepareStatement("SELECT "+procedureName+"("+paramHolders+")");
		        if (paramHolders.length()>0)
		        	for (int p=0; p<parameters.length; p++)
		        		stmt.setObject(p+1, parameters[p].getValue(), parameters[p].getType());
		        	ResultSet rset = stmt.executeQuery();
		        	if (rset.next())
		        		retval = resultSetToListOfMap(rset.getObject(1));
		        	else
		        		retval = null;
		        	rset.close();
		        	stmt.close();
			} else {
				CallableStatement call = conn.prepareCall("{ call "+procedureName+"("+paramHolders+") }");
		        if (paramHolders.length()>0)
		        	for (int p=0; p<parameters.length; p++)
		        		call.setObject(p+1, parameters[p].getValue(), parameters[p].getType());
		        	retval = resultSetToListOfMap(call.execute());
		        	call.close();				
			}
		} catch (SQLException sqle) {
			throw new JDOException(sqle.getMessage(), sqle);
		} finally {
			try { if (conn!=null) conn.close(); } catch (Exception ignore) { }
		}		
		return retval;
	}

	@Override
	public TransactionManager getTransactionManager() {
		return transactMan;
	}

	/**
	 * @return <b>false</b> if this DataSource is not using a transaction manager,
	 * otherwise <b>true</b> if getTransactionManager().getTransaction().getStatus() is not
	 * STATUS_NO_TRANSACTION, STATUS_COMMITTED nor STATUS_ROLLEDBACK
	 */
	@Override
	public boolean inTransaction() throws JDOException {
		try {
			if (getTransactionManager()==null)
				return false;
			else if (getTransactionManager().getTransaction()==null)
				return false;
			else {
				final int status = getTransactionManager().getTransaction().getStatus();				
				if (DebugFile.trace)
					DebugFile.writeln("JDBCBucketDataSource.inTransaction() transaction status is " + String.valueOf(status));
				if (status==Status.STATUS_UNKNOWN)
					throw new JDOException("Transaction status is unknown");
				return status!=Status.STATUS_NO_TRANSACTION && status!=Status.STATUS_COMMITTED && status!=Status.STATUS_ROLLEDBACK;
			}
		} catch (SystemException e) {
			throw new JDOException(e.getMessage(), e);
		}
	}
			
	public Map<String,String> getProperties() {
		return props;
	}

	// ----------------------------------------------------------
	
	/**
	 * <p>Get the autoCommit behavior that connections obtained from this DataSource will have.</p>
	 * @return boolean
	 */
	public boolean getDefaultAutoCommit() {
		return autocommit;
	}	

	// ----------------------------------------------------------
	
	/**
	 * <p>Get the autoCommit behavior that connections obtained from this DataSource will have.</p>
	 * @param autoCommit boolean 
	 */
	public void setDefaultAutoCommit(final boolean autoCommit) {
		if (DebugFile.trace)
			DebugFile.writeln("JDBCDataSource.setDefaultAutoCommit(" + String.valueOf(autoCommit) + ")");
		this.autocommit = autoCommit;
	}	

	// ----------------------------------------------------------

	/**
	 * <p>Get a single property.</p>
	 * @param sVarName Property Name
	 * @return Value of property or <b>null</b> if no property with such name was found.
	 */
	public String getProperty(String sVarName) {
		return props.get(sVarName);
	}

	// ----------------------------------------------------------

	/**
	 * <p>Get a single property.</p>
	 * @param sVarName Property Name
	 * @param sDefault Default Value
	 * @return Value of property or sDefault if no property with such name was found.
	 */
	public String getProperty(String sVarName, String sDefault) {
		return props.getOrDefault(sVarName, sDefault);
	}

	// ----------------------------------------------------------

	/**
	 * <p>Get a boolean property.</p>
	 * @param sVarName Property Name
	 * @param bDefault Default Value
	 * @return If no property named sVarName is found then bDefault value is returned.
	 * If sVarName is one of {true , yes, on, 1} then return value is <b>true</b>.
	 * If sVarName is one of {false, no, off, 0} then return value is <b>false</b>.
	 * If sVarName is any other value then then return value is bDefault
	 */
	public boolean getPropertyBool(String sVarName, boolean bDefault) {
		return Env.getBoolean(props, sVarName, bDefault);
	}

	// ----------------------------------------------------------

	/**
	 * <p>Get a property representing a file path from the .CNF file for this DBBind.</p>
	 * @param sVarName Property Name
	 * @return Value of property or <b>null</b> if no property with such name was found.
	 */
	public String getPropertyPath(String sVarName) {
		return Env.getPath(props, sVarName);
	}

	/**
	 * <P>Close DBDataSource</P>
	 * Close connections from pool.<BR>
	 * Stop connection reaper.<BR>
	 */
	@Override
	public void close() {

		if (DebugFile.trace)  {
			DebugFile.writeln("Begin DBDataSource.close()");
			DebugFile.incIdent();
		}

		connectXcpt = null;

		if (metaData!=null) {
			metaData.clear();
			metaData = null;
		}

		super.close();

		if (DebugFile.trace)  {
			DebugFile.decIdent();
			DebugFile.writeln("End DBDataSource.close()");
		}
	} // close

	// ----------------------------------------------------------

	/**
	 * Close and reopen the connection pool and reload the table map cache
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	public void restart()
			throws SQLException, ClassNotFoundException {

		if (DebugFile.trace)  {
			DebugFile.writeln("Begin DBDataSource.restart()");
			DebugFile.incIdent();
		}

		connectXcpt = null;

		if (metaData!=null) {
			metaData.clear();
			metaData = null;
		}

		try {
			super.close();
		}
		catch (Exception e) {
			if (DebugFile.trace)  DebugFile.writeln(e.getClass().getName() + " " + e.getMessage());
		}

		initialize ();

		if (DebugFile.trace)  {
			DebugFile.incIdent();
			DebugFile.writeln("End DBDataSource.restart()");
		}
	} // restart

	// ----------------------------------------------------------

	private void loadDriver()
			throws ClassNotFoundException, NullPointerException  {

		if (DebugFile.trace) DebugFile.writeln("Begin JDBCDataSource.loadDriver()" );

		final String sDriver = getProperty(DRIVER);

		if (DebugFile.trace) DebugFile.writeln("  " + DRIVER + "=" +  sDriver);

		if (null==sDriver)
			throw new NullPointerException("Could not find property " + DRIVER);

		Class.forName(sDriver);

		if (DebugFile.trace) DebugFile.writeln("End JDBCDataSource.loadDriver()" );
	} // loadDriver()

	// ----------------------------------------------------------

	protected boolean addTableToCache(TableDef tableDef) {
		boolean retval;
		final String tableName = tableDef.getName().toLowerCase();
		
		if (DebugFile.trace) {
			DebugFile.writeln("Begin JDBCDataSource.addTableToCache("+tableDef.getName()+")");
			DebugFile.incIdent();
		}

		if (!Str.in(tableName, Functions.systemTables())) {
			metaData.removeTable(tableName, null);
			metaData.addTable(tableDef, null);
			if (useDatabaseMetadata)
				metaData.getPackage("default").addClass(tableDef);
			if (DebugFile.trace) DebugFile.writeln("Table " + tableDef.getName()+ " added to cache");
			retval = true;
		} else {
			if (DebugFile.trace) DebugFile.writeln("Table " + tableDef.getName()+ " not added to cache because it's a system table");
			retval = false;
		}

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End JDBCDataSource.addTableToCache() : "+String.valueOf(retval));
		}
		return retval;
	}

	// ----------------------------------------------------------

	protected boolean addColumnsToCache(SQLTableDef oTable, Connection oConn, DatabaseMetaData oMData) {
		boolean added = true;

		if (DebugFile.trace) {
			DebugFile.writeln("Begin JDBCDataSource.addColumnsToCache("+oTable.getName()+")");
			DebugFile.incIdent();
		}
		
		try {
			oTable.readCols(oConn, oMData);
			oTable.readIndexes(oConn, oMData);
		} catch (SQLException sqle) {
			added = false;
			if (DebugFile.trace) {
				DebugFile.writeln("Could not read columns of table "+oTable.getName());
				try { DebugFile.writeln(StackTraceUtil.getStackTrace(sqle)); } catch (Exception ignore) {}
			}
		}

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End JDBCDataSource.addColumnsToCache() : "+String.valueOf(added));
		}
		
		return added;
	}
	
	// ----------------------------------------------------------

	protected void readMetadataFromDatabase(Connection oConn, DatabaseMetaData oMData) throws SQLException {

		if (DebugFile.trace) {
			DebugFile.writeln("Begin JDBCDataSource.readMetadataFromDatabase(Connection, DatabaseMetaData)");
			DebugFile.incIdent();
		}
		
		TableDef oTable;
		String sTableSchema;
		Statement oAcct = null;
		ResultSet oRSet;
		Iterator<TableDef> oTableIterator;
		final String TableTypes[] = new String[]{"TABLE"};
		final String sCatalog = oConn.getCatalog();

		if (RDBMS.ORACLE.equals(databaseProductId)) {

			if (DebugFile.trace) {
				ResultSet oSchemas = null;
				try {
					int iSchemaCount = 0;
					oSchemas = oMData.getSchemas();
					while (oSchemas.next()) {
						DebugFile.writeln("schema name = " + oSchemas.getString(1));
						iSchemaCount++;
					}
					oSchemas.close();
					oSchemas = null;
					if (0==iSchemaCount) DebugFile.writeln("no schemas found");
				}
				catch (Exception sqle) {
					try { if (null!=oSchemas) oSchemas.close();} catch (Exception ignore) {}
					DebugFile.writeln("SQLException at DatabaseMetaData.getSchemas() " + sqle.getMessage());
				}
				DebugFile.writeln("DatabaseMetaData.getTables(" + sCatalog + ", null, %, {TABLE})");
			}

			oRSet = oMData.getTables(sCatalog, null, "%", TableTypes);

			while (oRSet.next()) {

				if (oRSet.getString(3).indexOf('$')<0 && !Str.in(oRSet.getString(3).toUpperCase(), Functions.systemTables())) {
					sTableSchema = oRSet.getString(2);
					if (oRSet.wasNull()) sTableSchema = getProperty(SCHEMA, "");
					oTable = new SQLTableDef(getDatabaseProductId(), sCatalog, sTableSchema, oRSet.getString(3));
					addTableToCache(oTable);
				}
				else if (DebugFile.trace)
					DebugFile.writeln("Skipping table " + oRSet.getString(3));
			} // wend

		}
		else  {

			if (DebugFile.trace)
				DebugFile.writeln("DatabaseMetaData.getTables(" + sCatalog + ", " + getProperty(SCHEMA, "") + ", %, {TABLE})");

			if (RDBMS.ACCESS.equals(databaseProductId)) {
				oAcct = oConn.createStatement();
				oRSet = oAcct.executeQuery("SELECT NULL,NULL,Name FROM MSysObjects WHERE Type=1 AND Flags<>-2147483648");
			} else {
				oRSet = oMData.getTables(sCatalog, getProperty(SCHEMA, ""), "%", TableTypes);
			}

			// For each table, keep its name in a memory map

			if (getProperty(SCHEMA, "").length()>0) {

				while (oRSet.next()) {

					String sTableName = oRSet.getString(3);

					if (!oRSet.wasNull()) {
						if (DebugFile.trace) DebugFile.writeln("Processing table " + sTableName);
						sTableSchema = oRSet.getString(2);
						if (oRSet.wasNull()) sTableSchema = getProperty(SCHEMA, "dbo");
						oTable = new SQLTableDef (getDatabaseProductId(), sCatalog, sTableSchema, sTableName);
						addTableToCache(oTable);
					} // fi (!oRSet.wasNull())
				} // wend
			}
			else { // sSchema == ""
				while (oRSet.next()) {

					sTableSchema = oRSet.getString(2);
					if (oRSet.wasNull()) sTableSchema = "";

					String sTableName = oRSet.getString(3);

					if (!oRSet.wasNull()) {
						oTable = new SQLTableDef (getDatabaseProductId(), sCatalog, sTableSchema, sTableName);
						addTableToCache(oTable);						
					} // fi (!oRSet.wasNull())
				} // wend
			} // fi (sSchema == "")
		} // fi (DBMS_ORACLE!=iDatabaseProductId)

		oRSet.close();

		if (RDBMS.ACCESS.equals(databaseProductId)) oAcct.close();

		if (DebugFile.trace && metaData.getTablesCount()==0) DebugFile.writeln("No tables found");

		oTableIterator = metaData.tables().iterator();

		// For each table, read its column structure and keep it in memory
		int nWarnings = 0;
		LinkedList<String> oUnreadableTables = new LinkedList<String>();

		while (oTableIterator.hasNext()) {
			oTable = oTableIterator.next();
			boolean success = addColumnsToCache((SQLTableDef) oTable, oConn, oMData);
			if (!success) {
				nWarnings++;
				oUnreadableTables.add(oTable.getName());
			}
			oTable.setUnmodifiable();
		} // wend

		for (String t : oUnreadableTables) metaData.removeTable(t, null);

		if (DebugFile.trace) {
			if (nWarnings==0)
				DebugFile.writeln("Table scan finished with "+String.valueOf(nWarnings)+" warnings");
			else
				DebugFile.writeln("Table scan succesfully completed" );
			DebugFile.decIdent();
			DebugFile.writeln("End JDBCDataSource.readMetadataFromDatabase()");
		}
	}

	// ----------------------------------------------------------
	
	protected void initialize()
			throws ClassNotFoundException, SQLException, NullPointerException,
			AccessControlException,UnsatisfiedLinkError,NumberFormatException {

		Connection oConn;
		DatabaseMetaData oMData;

		if (useDatabaseMetadata) {
			metaData = new SchemaMetaData();
			metaData.addPackage("default");
		} else {
			metaData = null;
		}

		if (DebugFile.trace)
		{
			DebugFile.writeln("JDBCDataSource build " + JDBCDataSource.VERSION);
			DebugFile.envinfo();

			DebugFile.writeln("Begin JDBCDataSource.initialize()");
			DebugFile.incIdent();
		}

		if (DebugFile.trace) DebugFile.writeln("Load Driver " + getProperty(DRIVER) + " : OK\n" );

		loadDriver();

		if (DebugFile.trace) DebugFile.writeln("Trying to connect to " + getProperty(URI) + " with user " + getProperty(USER));

		try {
			DriverManager.setLoginTimeout(Integer.parseInt(getProperty(LOGINTIMEOUT, "20")));
		} catch (Exception x) {
			if (DebugFile.trace) DebugFile.writeln("DriverManager.setLoginTimeout() "+x.getClass().getName()+" "+x.getMessage());
		}

		try {
			if (getProperty(USER)==null && getProperty(PASSWORD)==null)
				oConn = DriverManager.getConnection(getProperty(URI));
			else
				oConn = DriverManager.getConnection(getProperty(URI),
						getProperty(USER),
						getProperty(PASSWORD));
		}
		catch (SQLException e) {
			if (DebugFile.trace) DebugFile.writeln("DriverManager.getConnection("+getProperty(URI)+","+getProperty(USER)+", ...) SQLException [" + e.getSQLState() + "]:" + String.valueOf(e.getErrorCode()) + " " + e.getMessage());
			connectXcpt = new SQLException("DriverManager.getConnection("+getProperty(URI)+","+getProperty(USER)+", ...) "+e.getMessage(), e.getSQLState(), e.getErrorCode());
			throw (SQLException) connectXcpt;
		}

		if (DebugFile.trace) {
			DebugFile.writeln("Database Connection to " + getProperty(URI) + " : OK\n" );
			DebugFile.writeln("Calling Connection.getMetaData()");
		}

		oMData = oConn.getMetaData();

		if (DebugFile.trace) DebugFile.writeln("Calling DatabaseMetaData.getDatabaseProductName()");

		databaseProductName = oMData.getDatabaseProductName();

		if (databaseProductName.equals(RDBMS.POSTGRESQL.toString()))
			databaseProductId = RDBMS.POSTGRESQL;
		else if (databaseProductName.equals(RDBMS.MSSQL.toString()))
			databaseProductId = RDBMS.MSSQL;
		else if (databaseProductName.equals(RDBMS.ORACLE.toString()))
			databaseProductId = RDBMS.ORACLE;
		else if (databaseProductName.equals(RDBMS.MYSQL.toString()))
			databaseProductId = RDBMS.MYSQL;
		else if (databaseProductName.equals(RDBMS.ACCESS.toString()))
			databaseProductId = RDBMS.ACCESS;
		else if (databaseProductName.equals(RDBMS.SQLITE.toString()))
			databaseProductId = RDBMS.SQLITE;
		else if (databaseProductName.equals(RDBMS.HSQLDB.toString()))
			databaseProductId = RDBMS.HSQLDB;
		else if (databaseProductName.equals("StelsDBF JDBC driver") ||
				databaseProductName.equals("HXTT DBF"))
			databaseProductId = RDBMS.XBASE;
		else
			databaseProductId = RDBMS.GENERIC;

		if (DebugFile.trace) {
			DebugFile.writeln("Database is \"" + databaseProductName + "\"");
			DebugFile.writeln("Product version " + oMData.getDatabaseProductVersion());
			DebugFile.writeln(oMData.getDriverName() + " " + oMData.getDriverVersion());
			DebugFile.writeln("Max connections " + String.valueOf(oMData.getMaxConnections()));
			DebugFile.writeln("Max statements " + String.valueOf(oMData.getMaxStatements()));
		}

		Functions = SQLFunctions.DB.get(databaseProductId);

		if (useDatabaseMetadata) {
			readMetadataFromDatabase(oConn, oMData);
		}

		oConn.close();
		oConn=null;

		if (DebugFile.trace)
		{
			DebugFile.decIdent();
			DebugFile.writeln("End JDBCDataSource.initialize()");
		}
	} // initialize

	// ----------------------------------------------------------

	/**
	 * Get the name of Database Management System Connected
	 * @return one of { "Microsoft SQL Server", "Oracle", "PostgreSQL", "MySQL" }
	 * @throws SQLException
	 */
	public String getDatabaseProductName()
			throws SQLException {

		if (null!=connectXcpt) throw (SQLException) connectXcpt;

		return databaseProductName;
	}

	// ----------------------------------------------------------

	/**
	 * Get the Id of Database Management System Connected
	 * @return RDBMS enum value
	 * @throws SQLException
	 */
	public RDBMS getDatabaseProductId()
			throws SQLException {

		if (null!=connectXcpt) throw (SQLException) connectXcpt;

		return databaseProductId;
	}

	// ----------------------------------------------------------

	/**
	 * Checks if an object exists at database
	 * Checking is done directly against database catalog tables,
	 * if current user does not have enough privileges for reading
	 * database catalog tables methods may fail or return a wrong result.
	 * @param oConn Database connection
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
	public boolean exists(JDCConnection oConn, String sObjectName, String sObjectType)
			throws SQLException, UnsupportedOperationException {
		return oConn.exists(sObjectName, sObjectType);
	} // exists()

	// ----------------------------------------------------------
	
	@Override
	public boolean exists(String sObjectName, String sObjectType) throws JDOException {
		JDCConnection oConn = null;
		boolean retval;
		try {
			oConn = this.getConnection("exists");
			retval = oConn.exists(sObjectName, sObjectType);
		}  catch (SQLException | UnsupportedOperationException sqle) {
			throw new JDOException(sqle.getMessage(), sqle);
		} finally {
			if (oConn!=null) oConn.close();
		}
		return retval;
	}

	// ----------------------------------------------------------

	/**
	 * <p>Format Date in ODBC escape sequence style</p>
	 * @param dt Date to be formated
	 * @param sFormat Format Type "d" or "ts" or "shortTime".
	 * Use d for { d 'yyyy-mm-dd' }, use ts for { ts 'ts=yyyy-mm-dd hh:nn:ss' }<br>
	 * use shortTime for hh:mm<br>
	 * use shortDate for yyyy-mm-dd<br>
	 * use dateTime for yyyy-mm-dd hh:mm:ss<br>
	 * @return Formated date
	 * @throws IllegalArgumentException if dt is of type java.sql.Date
	 */

	@SuppressWarnings("deprecation")
	public String escape(java.util.Date dt, String sFormat)
			throws IllegalArgumentException {
		String str = "";
		String sMonth, sDay, sHour, sMin, sSec;

		if (sFormat.equalsIgnoreCase("ts") || sFormat.equalsIgnoreCase("d")) {
			str = Functions.escape(dt, sFormat);
		}
		else if (sFormat.equalsIgnoreCase("shortTime")) {
			sHour = (dt.getHours()<10 ? "0" + String.valueOf(dt.getHours()) : String.valueOf(dt.getHours()));
			sMin = (dt.getMinutes()<10 ? "0" + String.valueOf(dt.getMinutes()) : String.valueOf(dt.getMinutes()));
			str += sHour + ":" + sMin;
		}
		else if (sFormat.equalsIgnoreCase("shortDate")) {
			sMonth = (dt.getMonth()+1<10 ? "0" + String.valueOf((dt.getMonth()+1)) : String.valueOf(dt.getMonth()+1));
			sDay = (dt.getDate()<10 ? "0" + String.valueOf(dt.getDate()) : String.valueOf(dt.getDate()));

			str += String.valueOf(dt.getYear()+1900) + "-" + sMonth + "-" + sDay;
		} else {
			sMonth = (dt.getMonth()+1<10 ? "0" + String.valueOf((dt.getMonth()+1)) : String.valueOf(dt.getMonth()+1));
			sDay = (dt.getDate()<10 ? "0" + String.valueOf(dt.getDate()) : String.valueOf(dt.getDate()));
			sHour = (dt.getHours()<10 ? "0" + String.valueOf(dt.getHours()) : String.valueOf(dt.getHours()));
			sMin = (dt.getMinutes()<10 ? "0" + String.valueOf(dt.getMinutes()) : String.valueOf(dt.getMinutes()));
			sSec = (dt.getSeconds()<10 ? "0" + String.valueOf(dt.getSeconds()) : String.valueOf(dt.getSeconds()));

			str += String.valueOf(dt.getYear()+1900)+"-"+sMonth+"-"+sDay+" "+sHour+":"+sMin+":"+sSec;
		}

		return str;
	} // escape()

	// ----------------------------------------------------------

	/**
	 * Format Timestamp in ODBC escape sequence style
	 * @param ts Timestamp to be formated
	 * @param sFormat Format Type "d" or "ts" or "shortTime".
	 * Use d for { d 'yyyy-mm-dd' }, use ts for { ts 'ts=yyyy-mm-dd hh:nn:ss' }<br>
	 * use shortTime for hh:mm<br>
	 * use shortDate for yyyy-mm-dd<br>
	 * use dateTime for yyyy-mm-dd hh:mm:ss<br>
	 * @return Formated date
	 */

	public String escape(Timestamp ts, String sFormat) {
		return escape(new java.util.Date(ts.getTime()), sFormat);
	}

	// ----------------------------------------------------------

	/**
	 * <p>Get {@link SQLViewDef} object by name</p>
	 * @param sTable Table name
	 * @return SQLViewDef object or <b>null</b> if no view was found with given name.
	 * @throws NullPointerException if sTable is <b>null</b>
	 */
	public SQLViewDef getViewDef(String sView) throws JDOException {

		if (null==sView) throw new NullPointerException("JDBCDataSource.getViewDef() view name cannot be null");

		if (null==metaData)
			throw new JDOException("DBDataSource internal table map not initialized, call DBDataSource constructor first");

		return (SQLViewDef) metaData.getView(sView.toLowerCase());
	} 

	// ----------------------------------------------------------

	/**
	 * <p>Get {@link SQLTableDef} object by name</p>
	 * @param sTable Table name
	 * @return SQLTableDef object or <b>null</b> if no table was found with given name.
	 * @throws NullPointerException if sTable is <b>null</b>
	 */
	public SQLTableDef getTableDef(String sTable) throws JDOException {

		if (null==sTable) throw new NullPointerException("JDBCDataSource.getTableDef() table name cannot be null");

		if (null==metaData)
			throw new JDOException("JDBCDataSource internal table map not initialized, call DBDataSource constructor first");

		return (SQLTableDef) metaData.getTable(sTable.toLowerCase());
	} 

	// ----------------------------------------------------------

	/**
	 * <p>Get {@link SQLTableDef} or {@link SQLViewDef} object by name</p>
	 * @param sObjectName Object name
	 * @return ViewDef or TableDef object or <b>null</b> if no table nor view was found with given name.
	 * @throws NullPointerException if sObjectName is <b>null</b>
	 */
	public ViewDef getTableOrViewDef(String sObjectName) throws JDOException {

		if (null==sObjectName) throw new NullPointerException("JDBCDataSource.getTableOrViewDef() table name cannot be null");

		if (null==metaData)
			throw new JDOException("JDBCDataSource internal table map not initialized, call DBDataSource constructor first");

		if (metaData.containsTable(sObjectName))
			return metaData.getTable(sObjectName.toLowerCase());
		else if (metaData.containsView(sObjectName))
			return metaData.getView(sObjectName.toLowerCase());
		else 
			return null;
	} 
	
	// ----------------------------------------------------------

	/**
	 * <p>Get map of {@link JDBCTable} objects</p>
	 * @return Map<String,TableDef>
	 * @throws IllegalStateException DBTable objects are cached in a static HasMap,
	 * the HashMap is loaded upon first call to a DBBind constructor.
	 * If getJDCTablesMap() is called before creating any instance of DBBind
	 * then an IllegalStateException will be thrown.
	 */
	public Map<String,TableDef> getJDCTablesMap() throws IllegalStateException {

		if (null==metaData)
			throw new IllegalStateException("JDBCDataSource internal table map not initialized, call JDBCDataSource initialize first");

		return metaData.tableMap();
	} // getDBTablesMap

	// ----------------------------------------------------------

	/**
	 * <p>Get a {@link JDCConnection} instance from connection pool</p>
	 * @param sCaller Symbolic name identifying the caller program or subroutine,
	 * this field is used for statistical control of database accesses,
	 * performance tunning and debugging open/close mismatch.
	 * @return An open connection to the database.
	 * @throws SQLException
	 */
	@Override
	public synchronized JDCConnection getConnection(String sCaller) throws SQLException {
		JDCConnection oConn;

		if (DebugFile.trace) {
			DebugFile.writeln("Begin DBBind.getConnection(" + sCaller + ")");
			DebugFile.incIdent();
		}

		if (null!=connectXcpt) {

			if (DebugFile.trace) {
				DebugFile.writeln(connectXcpt.getClass().getName()+" "+connectXcpt.getMessage());
				if (connectXcpt.getCause()!=null) {
					DebugFile.writeln(connectXcpt.getCause().getClass().getName()+" "+connectXcpt.getCause().getMessage());
				} // fi
				DebugFile.decIdent();
			} // fi

			if (connectXcpt instanceof SQLException) {
				throw (SQLException) connectXcpt;

			} else {
				throw new SQLException(connectXcpt.getClass().getName()+" "+connectXcpt.getMessage(), connectXcpt.getCause());
			}
		}

		oConn = super.getConnection(sCaller);
		oConn.setAutoCommit(autocommit);

		if (DebugFile.trace) {
			if (oConn!=null) DebugFile.writeln("Connection process id. is " + oConn.pid());
			DebugFile.decIdent();
			DebugFile.writeln("End DBBind.getConnection(" + sCaller + ") : " + (null==oConn ? "null" : "[Connection]") );
		}

		return oConn;
	} // getConnection()

	// ----------------------------------------------------------

	/**
	 * <p>Get a Connection instance from connection pool</p>
	 * @param sCaller Symbolic name identifying the caller program or subroutine,
	 * this field is used for statistical control of database accesses,
	 * performance tunning and debugging open/close mismatch.
	 * @return An open connection to the database.
	 * @throws SQLException
	 */
	@Override
	public JDOConnection getJdoConnection() {
		try {
			return getConnection(null);
		} catch (SQLException sqle) {
			throw new JDOException(sqle.getMessage(), sqle);
		}
	}

	// ----------------------------------------------------------

	/**
	 * <p>Get a Connection instance directly from the database bypassing the pool</p>
	 * @param sUser User name
	 * @param sPasswd Password
	 * @return An open connection to the database.
	 * Returned type is actually an unpooled com.knowgate.jdc.JDCConnection instance.
	 * @throws SQLException
	 */
	public synchronized Connection getConnection(String sUser, String sPasswd) throws SQLException {
		Connection oConn;

		if (DebugFile.trace) {
			DebugFile.writeln("Begin DBBind.getConnection(" + sUser + ", ...)");
			DebugFile.incIdent();

			if (null!=connectXcpt) {
				DebugFile.writeln("Previous exception " + connectXcpt.getMessage());
				DebugFile.decIdent();
			}
		}

		if (null!=connectXcpt) {
			if (connectXcpt instanceof SQLException)
				throw (SQLException) connectXcpt;
			else
				throw new SQLException(connectXcpt.getClass().getName()+" "+connectXcpt.getMessage());
		}

		if (sUser==null && sPasswd==null)
			oConn = DriverManager.getConnection(getProperty(URI));
		else
			oConn = DriverManager.getConnection(getProperty(URI), sUser, sPasswd);   

		oConn.setAutoCommit(autocommit);

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End DBBind.getConnection() : " + (null==oConn ? "null" : "[Connection]") );
		}

		return (Connection) new JDCConnection(oConn, null);
	} // getConnection()

	// ---------------------------------------------------------

	/**
	 * Get LogWriter from java.sql.DriverManager
	 */
	@Override
	public PrintWriter getLogWriter() throws SQLException {
		return DriverManager.getLogWriter();
	}

	// ---------------------------------------------------------

	/**
	 * Set LogWriter for java.sql.DriverManager
	 */
	@Override
	public void setLogWriter(PrintWriter printwrt) throws SQLException {
		DriverManager.setLogWriter(printwrt);
	}

	// ---------------------------------------------------------

	/**
	 * Get login timeout from java.sql.DriverManager
	 */
	@Override
	public int getLoginTimeout() throws SQLException {
		return DriverManager.getLoginTimeout();
	}

	// ---------------------------------------------------------

	/**
	 * Set login timeout for java.sql.DriverManager
	 */
	@Override
	public void setLoginTimeout(int seconds) throws SQLException {
		DriverManager.setLoginTimeout(seconds);
	}

	// ----------------------------------------------------------

	/**
	 * This method is added for compatibility with Java 7 and it is not implemented
	 * @return null
	 */
	@Override
	public Logger getParentLogger() {
		return null;
	}

	// ----------------------------------------------------------

	/**
	 * Get a sequence generator provided by the underlying RDBMS
	 * @return Sequence
	 */
	@Override
	public Sequence getSequence(String name) throws JDOException {
		if (databaseProductId.equals(RDBMS.ORACLE))
			return new OrclSequenceGenerator(this, name);
		else if (databaseProductId.equals(RDBMS.POSTGRESQL))
			return new PgSequenceGenerator(this, name);
		else if (databaseProductId.equals(RDBMS.HSQLDB))
			return new HsqlSequenceGenerator(this, name);
		else
			throw new JDOUnsupportedOptionException(databaseProductId+" does not support sequence generation");
	}

	// ----------------------------------------------------------

	@Override
	public boolean isWrapperFor(Class<?> wrapped) throws SQLException {
		return getClass().isInstance(wrapped);
	}

	// ----------------------------------------------------------

	@Override
	public <T> T unwrap(Class<T> arg0) throws SQLException {
		return null;
	}

	// ----------------------------------------------------------
	
	@Override
	public Connection getConnection() throws SQLException {
		return getConnection(null);
	}
	
}
