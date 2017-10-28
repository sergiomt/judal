package org.judal.jdbc.metadata;

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

import java.io.IOException;
import java.io.StringBufferInputStream;
import java.sql.SQLException;
import java.sql.DatabaseMetaData;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.jdo.JDOException;
import javax.jdo.JDOUnsupportedOptionException;
import javax.jdo.JDOUserException;
import javax.jdo.metadata.ColumnMetadata;
import javax.jdo.metadata.ForeignKeyMetadata;
import javax.jdo.metadata.JoinMetadata;

import com.knowgate.debug.*;

import org.judal.jdbc.RDBMS;
import org.judal.jdbc.jdc.JDCConnection;
import org.judal.metadata.ColumnDef;
import org.judal.metadata.IndexDef.Type;
import org.judal.metadata.TableDef;
import org.judal.storage.Param;
import org.judal.storage.table.Record;

/**
 * <p>Represent database table structure as a Java object</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 */

public class SQLTableDef extends TableDef implements SQLSelectableDef {

	private static final long serialVersionUID = 1L;

	public static String DEFAULT_CREATION_TIMESTAMP_COLUMN_NAME = "dt_created";

	private RDBMS dbms;
	
	private SQLHelper helper;

	/**
	 * <p>Constructor</p>
	 * @param sTableName
	 * @throws SQLException 
	 */
	public SQLTableDef(RDBMS eDbms, String sTableName) throws SQLException {
		super(sTableName);
		dbms = eDbms;
		setCatalog(null);
		setSchema(null);
		setCreationTimestampColumnName(DEFAULT_CREATION_TIMESTAMP_COLUMN_NAME);
		helper = null;
	}

	// ---------------------------------------------------------------------------
	
	/**
	 * Constructor
	 * @param sCatalogName Database catalog name
	 * @param sSchemaName Database schema name
	 * @param sTableName Database table name (not qualified)
	 * @throws SQLException 
	 */
	public SQLTableDef(RDBMS eDbms, String sCatalogName, String sSchemaName, String sTableName) throws SQLException {
		super(sTableName);
		dbms = eDbms;
		setCatalog(null);
		setSchema(null);
		setCreationTimestampColumnName(DEFAULT_CREATION_TIMESTAMP_COLUMN_NAME);
		setCatalog(sCatalogName);
		setSchema(sSchemaName);
		helper = null;
	}

	// ---------------------------------------------------------------------------

	public SQLTableDef(RDBMS eDbms, String sTableName, ColumnDef[] oCols) throws SQLException {
		super(sTableName, oCols);
		dbms = eDbms;
		setCatalog(null);
		setSchema(null);
		setCreationTimestampColumnName(DEFAULT_CREATION_TIMESTAMP_COLUMN_NAME);

		int iPos = 0;
		for (ColumnDef oCol : oCols) {
			SQLColumn oDbc = new SQLColumn(sTableName, oCol.getName(), oCol.getType().shortValue(), ColumnDef.typeName(oCol.getType()), oCol.getLength(), oCol.getScale(), oCol.getAllowsNull() ? DatabaseMetaData.columnNullable : DatabaseMetaData.columnNoNulls, ++iPos);
			oDbc.setTarget(oCol.getTarget());
			addColumnMetadata(oDbc);
			if (oCol.isIndexed())
				addIndexMetadata(new SQLIndex(sTableName, "i"+String.valueOf(oCol.getPosition())+"_"+sTableName, oCol.getName(), oCol.getIndexType()==Type.ONE_TO_ONE));
		}    
		helper = new SQLHelper(eDbms, this, DEFAULT_CREATION_TIMESTAMP_COLUMN_NAME);
	}

	// ---------------------------------------------------------------------------
	/**
	 * <p>Constructor</p>
	 * @param sTableName
	 */
	public SQLTableDef(SQLTableDef source) {
		super(source);
		dbms = source.dbms;
	}
	
	// ---------------------------------------------------------------------------

	@Override
	public SQLTableDef clone() {
		return new SQLTableDef(this);
	}
	
	// ---------------------------------------------------------------------------

	public SQLViewDef asView() throws JDOException {
		SQLViewDef retval;
		try {
			retval = new SQLViewDef(dbms, getName(), null);
			retval.setColumns(columns);
			retval.setCatalog(getCatalog());
			retval.setSchema(getSchema());
		} catch (SQLException sqle) {
			throw new JDOException(sqle.getMessage(), sqle);
		}
		return retval;
	}

	// ---------------------------------------------------------------------------

	/** Create a new column without adding it to the table definition.
	 * @return JDCColumn
	 */
	@Override
	protected SQLColumn createColumn() {
		return new SQLColumn();
	}

	// ---------------------------------------------------------------------------

	/**
	 * Get table names including the joined tables if present
	 * @return String of the form "this_table_name [INNER|OUTER] JOIN joined_table_name ON this_column_name=joined_column_name"
	 */
	@Override
	public String getTables() throws JDOUserException,JDOUnsupportedOptionException {
		if (getNumberOfJoins()==0) {
			return getName();
		} else if (getNumberOfJoins()==1) {
			JoinMetadata join = getJoins()[0];
			for (ForeignKeyMetadata fk : getForeignKeys()) {
				if (fk.getTable().equals(join.getTable())) {
					if (fk.getNumberOfColumns()!=join.getNumberOfColumns())
						throw new JDOUserException("Foreign key "+(fk.getName()==null ? "" : fk.getName()) +" number of columns "+fk.getNumberOfColumns()+" does not match join number of columns "+join.getNumberOfColumns());
					StringBuilder joinSql = new StringBuilder();
					joinSql.append(getName()).append(" ").append(join.getOuter() ? "OUTER" : "INNER").append(" JOIN ").append(join.getTable()).append(" ON ");
					joinSql.append(getName()).append(".").append(fk.getColumns()[0].getName()).append("=").append(join.getTable()).append(".").append(join.getColumn());
					if (fk.getNumberOfColumns()>1) {
						for (int c=1; c<fk.getNumberOfColumns(); c++) {
							joinSql.append(" AND ").append(getName()).append(".").append(fk.getColumns()[c].getName()).append("=").append(join.getTable()).append(".").append(join.getColumns()[c]);
						}							
					}
					return joinSql.toString();
				}
			}
			throw new JDOUserException("Cannot find a foreign key corresponding to join definition");
		} else {
			throw new JDOUnsupportedOptionException("Only one join at a time is supported");
		}
	}

	// ---------------------------------------------------------------------------

	/**
	 * <p>Load a single table register into a Record</p>
	 * @param oConn Database Connection
	 * @param PKValues Primary key values of register to be read, in the same order as they appear in table source.
	 * @param AllValues Record Output parameter. Read values.
	 * @return <b>true</b> if register was found <b>false</b> otherwise.
	 * @throws NullPointerException If all objects in PKValues array are null (only debug version)
	 * @throws ArrayIndexOutOfBoundsException if the length of PKValues array does not match the number of primary key columns of the table
	 * @throws IllegalStateException if columns list has not been set for this TableDef
	 * @throws SQLException
	 */
	public boolean loadRegister(JDCConnection oConn, Object[] PKValues, Record AllValues)
			throws SQLException, NullPointerException, IllegalStateException, ArrayIndexOutOfBoundsException {
		if (null==helper)
			throw new IllegalStateException("Column list is not set");
		return helper.loadRegister(oConn, PKValues, AllValues);
	} // loadRegister

	// ---------------------------------------------------------------------------

	/**
	 * <p>Store a single register at the database representing a Java Object</p>
	 * for register containing LONGVARBINARY, IMAGE, BYTEA or BLOB fields use
	 * storeRegisterLong() method.
	 * Columns with auto increment serial are not written by this storeRegister().
	 * Columns named "dt_created" are also invisible for storeRegister() method so that
	 * register creation timestamp is not altered by afterwards updates.
	 * @param oConn Database Connection
	 * @param Record Values to assign to fields.
	 * @return <b>true</b> if register was inserted for first time, <false> if it was updated.
	 * @throws IllegalStateException if columns list has not been set for this TableDef
	 * @throws SQLException
	 */

	public boolean storeRegister(JDCConnection oConn, Record AllValues) throws SQLException, IllegalStateException {
		if (null==helper)
			throw new IllegalStateException("Column list is not set");
		return helper.storeRegister(oConn, AllValues);
	} // storeRegister

	// ---------------------------------------------------------------------------

	/**
	 * <p>Store a single register at the database representing a Java Object</p>
	 * for register NOT containing LONGVARBINARY, IMAGE, BYTEA or BLOB fields use
	 * storeRegister() method witch is faster than storeRegisterLong().
	 * Columns named "dt_created" are invisible for storeRegisterLong() method so that
	 * register creation timestamp is not altered by afterwards updates.
	 * @param oConn Database Connection
	 * @param Record Values to assign to fields.
	 * @param BinaryLengths map of lengths for long fields.
	 * @return <b>true</b> if register was inserted for first time, <false> if it was updated.
	 * @throws IllegalStateException if columns list has not been set for this TableDef
	 * @throws SQLException
	 */

	public boolean storeRegisterLong(JDCConnection oConn, Record AllValues, Map<String,Long> BinaryLengths) throws IOException, SQLException, IllegalStateException {
		if (null==helper)
			throw new IllegalStateException("Column list is not set");
		return helper.storeRegisterLong(oConn, AllValues, BinaryLengths);
	} // storeRegisterLong

	// ---------------------------------------------------------------------------

	/**
	 * <p>Delete a single register from this table at the database</p>
	 * @param oConn Database connection
	 * @param AllValues Record with, at least, the primary key values for the register. Other Record values are ignored.
	 * @return <b>true</b> if register was delete, <b>false</b> if register to be deleted was not found.
	 * @throws IllegalStateException if columns list has not been set for this TableDef
	 * @throws SQLException
	 */
	public boolean deleteRegister(JDCConnection oConn, Map<String,Object> AllValues) throws SQLException,IllegalStateException {
		if (null==helper)
			throw new IllegalStateException("Column list is not set");
		return helper.deleteRegister(oConn, AllValues);
	} // deleteRegister

	// ---------------------------------------------------------------------------

	/**
	 * <p>Checks if register exists at this table</p>
	 * @param oConn Database Connection
	 * @param sQueryString Register Query String, as a SQL WHERE clause syntax
	 * @return <b>true</b> if register exists, <b>false</b> otherwise.
	 * @throws IllegalStateException if columns list has not been set for this TableDef
	 * @throws SQLException
	 */

	public boolean existsRegister(JDCConnection oConn, String sQueryString) throws SQLException,IllegalStateException {
		if (null==helper)
			throw new IllegalStateException("Column list is not set");
		return helper.existsRegister(oConn, sQueryString);
	}

	// ---------------------------------------------------------------------------

	/**
	 * <p>Checks if register exists at this table</p>
	 * @param oConn Database Connection
	 * @param sQueryString Register Query String, as a SQL WHERE clause syntax
	 * @return <b>true</b> if register exists, <b>false</b> otherwise.
	 * @throws IllegalStateException if columns list has not been set for this TableDef
	 * @throws SQLException
	 */

	public boolean existsRegister(JDCConnection oConn, String sQueryString, Object[] oQueryParams) throws SQLException,IllegalStateException {
		if (null==helper)
			throw new IllegalStateException("Column list is not set");
		return helper.existsRegister(oConn, sQueryString, oQueryParams);
	}

	// ---------------------------------------------------------------------------

	/**
	 * <p>Checks if register exists at this table</p>
	 * @param oConn Database Connection
	 * @param AllValues Map<String,Object>
	 * @return <b>true</b> if register exists, <b>false</b> otherwise.
	 * @throws IllegalStateException if columns list has not been set for this TableDef
	 * @throws SQLException
	 */

	public boolean existsRegister(JDCConnection oConn, Map<String,Object> AllValues) throws SQLException,IllegalStateException {
		if (null==helper)
			throw new IllegalStateException("Column list is not set");
		return helper.existsRegister(oConn, AllValues);
	} // existsRegister


	public int getRDBMSId(Connection conn) throws SQLException {
		final String productName = conn.getMetaData().getDatabaseProductName();
		if (productName.equals(RDBMS.POSTGRESQL.toString()))
			return RDBMS.POSTGRESQL.intValue();
		else if (productName.equals(RDBMS.ORACLE.toString()))
			return RDBMS.ORACLE.intValue();
		else if (productName.equals(RDBMS.MYSQL.toString()))
			return RDBMS.MYSQL.intValue();
		else if (productName.equals(RDBMS.ACCESS.toString()))
			return RDBMS.ACCESS.intValue();
		else if (productName.equals(RDBMS.MSSQL.toString()))
			return RDBMS.MSSQL.intValue();
		else if (productName.equals(RDBMS.SQLITE.toString()))
			return RDBMS.SQLITE.intValue();
		else if (productName.equals(RDBMS.HSQLDB.toString()))
			return RDBMS.HSQLDB.intValue();
		else
			return 0;
	}

	/**
	 * <p>Read JDCColumn List from DatabaseMetaData</p>
	 * @param conn Database Connection
	 * @param mdata DatabaseMetaData
	 * @throws SQLException
	 */
	public List<SQLColumn> readCols(Connection conn, DatabaseMetaData mdata) throws SQLException {
		ArrayList<SQLColumn> columns = null;
		int errCode;
		Statement stmt;
		ResultSet rset;
		ResultSetMetaData rdata;
		SQLColumn col;
		int ncols;
		int nreadonly;

		String columnName;
		short sqlType;
		String typeName;
		int precision;
		int digits;
		int nullabla;
		boolean autoInc;
		int colPos;

		if (DebugFile.trace) {
			DebugFile.writeln("Begin SQLTableDef.readCols(Connection, DatabaseMetaData)" );
			DebugFile.incIdent();
			DebugFile.writeln("DatabaseMetaData.getColumns(" + getCatalog() + "," + getSchema() + "," + getName() + ",%)");
		}

		final int iDBMS = getRDBMSId(conn);

		if (DebugFile.trace) DebugFile.writeln("RDBMS code is "+String.valueOf(iDBMS));

		stmt = conn.createStatement();

		try {
			String ssql;

			if (iDBMS==RDBMS.POSTGRESQL.intValue()) {
				if (getSchema()==null) {
					ssql = "SELECT * FROM " + getName() + " WHERE 1=0";
				} else if (getSchema().length()==0) {
					 ssql= "SELECT * FROM " + getName() + " WHERE 1=0";
				} else {
					ssql = "SELECT * FROM \"" + getSchema() + "\"." + getName() + " WHERE 1=0";
				}
			} else {
				ssql = "SELECT * FROM " + getName() + " WHERE 1=0";
			}

			if (DebugFile.trace) DebugFile.writeln("Statement.executeQuery("+ssql+")");
			rset = stmt.executeQuery(ssql);
			
			errCode = 0;
		}
		catch (SQLException sqle) {
			// Patch for Oracle. DatabaseMetadata.getTables() returns table names
			// that later cannot be SELECTed, so this catch ignore these system tables

			stmt.close();
			rset = null;

			if (DebugFile.trace) DebugFile.writeln("SQLException " + getSchema() + "." + getName() + " " + sqle.getMessage());

			errCode = sqle.getErrorCode();
			if (errCode==0) errCode=-1;
			if (!sqle.getSQLState().equals("42000"))
				throw new SQLException(getSchema() + "." + getName() + " " + sqle.getMessage(), sqle.getSQLState(), sqle.getErrorCode());
		}

		if (0==errCode) {
			if (DebugFile.trace) DebugFile.writeln("ResultSet.getMetaData()");

			rdata = rset.getMetaData();

			ncols = rdata.getColumnCount();

			nreadonly = 0;

			columns = new ArrayList<SQLColumn>(ncols);

			if (DebugFile.trace) DebugFile.writeln("table has " + String.valueOf(ncols) + " columns");

			for (int c=1; c<=ncols; c++) {
				columnName = rdata.getColumnName(c).toLowerCase();
				typeName = rdata.getColumnTypeName(c);
				sqlType = (short) rdata.getColumnType(c);

				if (iDBMS==RDBMS.POSTGRESQL.intValue())
					switch (sqlType) {
					case Types.CHAR:
					case Types.VARCHAR:
						precision = rdata.getColumnDisplaySize(c);
						break;
					default:
						precision = rdata.getPrecision(c);
					}
				else {
					if (sqlType==Types.BLOB || sqlType==Types.CLOB)
						precision = 2147483647;
					else
						precision = rdata.getPrecision(c);
				}

				digits = rdata.getScale(c);
				nullabla = rdata.isNullable(c);
				autoInc = rdata.isAutoIncrement(c);
				colPos = c;

				if (RDBMS.ORACLE.intValue()==iDBMS && sqlType==Types.NUMERIC && precision<=6 && digits==0) {
					// Workaround for an Oracle 9i bug witch is unable to convert from Short to NUMERIC but does understand SMALLINT
					col = new SQLColumn (getName(), columnName, (short) Types.SMALLINT, ColumnDef.typeName(Types.SMALLINT), precision, digits, nullabla, autoInc, colPos-nreadonly);
				}
				else {
					col = new SQLColumn (getName(), columnName, sqlType, ColumnDef.typeName(sqlType), precision, digits, nullabla, autoInc, colPos-nreadonly);
				}

				if (columnName.equals(getCreationTimestampColumnName()))
					nreadonly++;
				else
					columns.add(col);
				
			} // next

			if (DebugFile.trace) DebugFile.writeln("ResultSet.close()");

			rset.close();
			rset = null;
			stmt.close();
			stmt = null;

			if (RDBMS.ORACLE.intValue()==iDBMS) /* Oracle */ {

				stmt = conn.createStatement();

				if (DebugFile.trace) {
					if (null==getSchema())
						DebugFile.writeln("Statement.executeQuery(SELECT NULL AS TABLE_CAT, COLS.OWNER AS TABLE_SCHEM, COLS.TABLE_NAME, COLS.COLUMN_NAME, COLS.POSITION AS KEY_SEQ, COLS.CONSTRAINT_NAME AS PK_NAME FROM USER_CONS_COLUMNS COLS, USER_CONSTRAINTS CONS WHERE CONS.OWNER=COLS.OWNER AND CONS.CONSTRAINT_NAME=COLS.CONSTRAINT_NAME AND CONS.CONSTRAINT_TYPE='P' AND CONS.TABLE_NAME='" + getName().toUpperCase()+ "')");
					else
						DebugFile.writeln("Statement.executeQuery(SELECT NULL AS TABLE_CAT, COLS.OWNER AS TABLE_SCHEM, COLS.TABLE_NAME, COLS.COLUMN_NAME, COLS.POSITION AS KEY_SEQ, COLS.CONSTRAINT_NAME AS PK_NAME FROM USER_CONS_COLUMNS COLS, USER_CONSTRAINTS CONS WHERE CONS.OWNER=COLS.OWNER AND CONS.CONSTRAINT_NAME=COLS.CONSTRAINT_NAME AND CONS.CONSTRAINT_TYPE='P' AND CONS.OWNER='" + getSchema().toUpperCase() + "' AND CONS.TABLE_NAME='" + getName().toUpperCase()+ "')");
				}

				if (null==getSchema())
					rset = stmt.executeQuery("SELECT NULL AS TABLE_CAT, COLS.OWNER AS TABLE_SCHEM, COLS.TABLE_NAME, COLS.COLUMN_NAME, COLS.POSITION AS KEY_SEQ, COLS.CONSTRAINT_NAME AS PK_NAME FROM USER_CONS_COLUMNS COLS, USER_CONSTRAINTS CONS WHERE CONS.OWNER=COLS.OWNER AND CONS.CONSTRAINT_NAME=COLS.CONSTRAINT_NAME AND CONS.CONSTRAINT_TYPE='P' AND CONS.TABLE_NAME='" + getName().toUpperCase()+ "'");
				else
					rset = stmt.executeQuery("SELECT NULL AS TABLE_CAT, COLS.OWNER AS TABLE_SCHEM, COLS.TABLE_NAME, COLS.COLUMN_NAME, COLS.POSITION AS KEY_SEQ, COLS.CONSTRAINT_NAME AS PK_NAME FROM USER_CONS_COLUMNS COLS, USER_CONSTRAINTS CONS WHERE CONS.OWNER=COLS.OWNER AND CONS.CONSTRAINT_NAME=COLS.CONSTRAINT_NAME AND CONS.CONSTRAINT_TYPE='P' AND CONS.OWNER='" + getSchema().toUpperCase() + "' AND CONS.TABLE_NAME='" + getName().toUpperCase()+ "'");
			}
			else if (RDBMS.ACCESS.intValue()==iDBMS) { // Microsoft Access
				rset=null;
			}
			else if (RDBMS.HSQLDB.intValue()==iDBMS) {
				if (DebugFile.trace)
					DebugFile.writeln("DatabaseMetaData.getPrimaryKeys("+getCatalog()+","+getSchema()+","+getName().toUpperCase()+")");
				rset = mdata.getPrimaryKeys(getCatalog(), getSchema(), getName().toUpperCase());
			} else {				
				if (DebugFile.trace)
					DebugFile.writeln("DatabaseMetaData.getPrimaryKeys("+getCatalog()+","+getSchema()+","+getName()+")");
				rset = mdata.getPrimaryKeys(getCatalog(), getSchema(), getName());
			} // fi (iDBMS)

			if (rset!=null) {
				while (rset.next()) {
					String sPkColName = rset.getString(4);
					if (DebugFile.trace)
						DebugFile.writeln("found primary key column "+sPkColName);
					for (ColumnDef colDef : columns) {
						if (colDef.getName().equalsIgnoreCase(sPkColName)) {
							colDef.setPrimaryKey(true);
							break;
						}
					}
				} // wend
				autoSetPrimaryKey();

				if (DebugFile.trace) DebugFile.writeln("ResultSet.close()");

				rset.close();
				rset = null;
			} else {
				if (DebugFile.trace) DebugFile.writeln("no primary key found");
			}// fi (oRSet)

			if (null!=stmt) { stmt.close(); stmt = null; }

		} // fi (0==iErrCode)

		if (getColumns().length>0)
			clearColumnsMeta();
		for (SQLColumn jcol : columns)
			addColumnMetadata(jcol);

		helper = new SQLHelper(dbms, this, getCreationTimestampColumnName());

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End SQLTableDef.readCols()");
		}

		return columns;

	} // readCols

	// ----------------------------------------------------------

	/**
	 * <p>Read Indexes List from DatabaseMetaData</p>
	 * @param conn Database Connection
	 * @param mdata DatabaseMetaData
	 * @throws SQLException
	 */
	public List<SQLIndex> readIndexes(Connection conn, DatabaseMetaData mdata) throws SQLException {
		ArrayList<SQLIndex> indexes = new ArrayList<SQLIndex>(4);
		Statement oStmt = null;
		ResultSet oRSet = null;

		if (DebugFile.trace) {
			DebugFile.writeln("Begin JDCTableDef.readIndexes(Connection, DatabaseMetaData)" );
			DebugFile.incIdent();
		}

		final int dbms = getRDBMSId(conn);

		try {
			switch (dbms) {
			case 1: // MySQL
				oStmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				if (DebugFile.trace)
					DebugFile.writeln("Statement.executeQuery(SELECT COLUMN_NAME,COLUMN_KEY FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME ='"+getName()+"' AND COLUMN_KEY!='')");
				oRSet = oStmt.executeQuery("SELECT COLUMN_NAME,COLUMN_KEY FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME ='"+getName()+"' AND COLUMN_KEY!=''");
				while (oRSet.next()) {
					String sIndexName = oRSet.getString(1);
					String sIndexType = oRSet.getString(2);
					if (DebugFile.trace)
						DebugFile.writeln("index name "+sIndexName+", index type "+sIndexType);
					indexes.add(new SQLIndex(getName(), sIndexName, sIndexName, sIndexType.equalsIgnoreCase("PRI") || sIndexType.equalsIgnoreCase("UNI")));
				} //wend
				oRSet.close();
				oRSet=null;
				oStmt.close();
				oStmt=null;
				break;
			case 2: // PostgreSQL
				oStmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				if (DebugFile.trace)
					DebugFile.writeln("Statement.executeQuery(SELECT indexname,indexdef FROM pg_indexes WHERE tablename='"+getName()+"')");
				oRSet = oStmt.executeQuery("SELECT indexname,indexdef FROM pg_indexes WHERE tablename='"+getName()+"'");
				while (oRSet.next()) {
					String sIndexName = oRSet.getString(1);
					String sIndexDef = oRSet.getString(2);
					if (DebugFile.trace)
						DebugFile.writeln("index name "+sIndexName+", index definition "+sIndexDef);
					int lPar = sIndexDef.indexOf('(');
					int rPar = sIndexDef.indexOf(')');
					if (lPar>0 && rPar>0)
						indexes.add(new SQLIndex(getName(), sIndexName, sIndexDef.substring(++lPar,rPar).split(","), sIndexDef.toUpperCase().indexOf("UNIQUE")>0));
				} //wend
				oRSet.close();
				oRSet=null;
				oStmt.close();
				oStmt=null;
				break;
			}
		} catch (SQLException sqle) {
			if (DebugFile.trace) DebugFile.writeln("Cannot get indexes for " + getName() );
			if (oRSet!=null) oRSet.close();
			if (oStmt!=null) oStmt.close();
		} 

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End JDCTableDef.readIndexes()");
		}
		
		return indexes;

	} // readIndexes

	// ----------------------------------------------------------

	/**
	 * Get SQL DDL creation script for this table
	 * @param eRDBMS
	 * @return String like "CREATE TABLE table_name ( ... ) "
	 */
	public String getSource()  {
		StringBuilder builder = new StringBuilder(2000);
		builder.append("CREATE TABLE ").append(getName()).append(" (\n");
		for (ColumnDef c : getColumns())
			builder.append(((SQLColumn)c).sqlScriptDef(dbms)).append(",\n");
		if (getPrimaryKeyMetadata()!=null && getPrimaryKeyMetadata().getNumberOfColumns()>0) {
			String[] pkcols = new String[getPrimaryKeyMetadata().getNumberOfColumns()];
			int c = 0;
			for (ColumnMetadata col : getPrimaryKeyMetadata().getColumns())
				pkcols[c++] = col.getName();
			builder.append("CONSTRAINT ").append((getPrimaryKeyMetadata().getName()==null ? "pk_"+getName() : getPrimaryKeyMetadata().getName()));
			builder.append(" PRIMARY KEY (").append(String.join(",", pkcols)).append("),\n");
		}
		int n = 0;
		if (!RDBMS.HSQLDB.equals(dbms)) {
			for (ColumnDef c : getColumns()) {
				if (c.getConstraint()!=null)
					builder.append("CONSTRAINT ck_").append(String.valueOf(++n)+"_"+getName()).append(" CHECK ("+c.getConstraint()+"),\n");
			}
		}
		n = 0;
		if (getForeignKeys()!=null) {
			for (ForeignKeyMetadata fk : getForeignKeys()) {
				builder.append("CONSTRAINT ").append(fk.getName()==null ? "fk_"+String.valueOf(++n)+getName() : fk.getName());
				if (RDBMS.HSQLDB.equals(dbms))
					builder.append(" FOREIGN KEY ");
				ArrayList<String> fkcols = new ArrayList<String>(fk.getColumns().length);
				for (ColumnMetadata c : fk.getColumns())
					fkcols.add(c.getName());
				builder.append(" (").append(String.join(",", fkcols)).append(") REFERENCES ").append(fk.getTable());
				fkcols.clear();
				for (ColumnMetadata c : fk.getColumns())
					fkcols.add(getColumnByName(c.getName()).getTargetField());
				builder.append(" (").append(String.join(",", fkcols)).append("),\n");
			}
		}
		if (builder.length()>1) builder.setLength(builder.length()-2); // remove trailing comma
		builder.append("\n)");
		return builder.toString();
	} // getSource

	// ----------------------------------------------------------

	@Override
	public String getDrop() {
		return "DROP TABLE "+getName();
	}

	// ----------------------------------------------------------
	
	@Override
	public Param[] getParams() {
		return new Param[0];
	}


} // SQLTableDef
