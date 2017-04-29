package org.judal.jdbc.metadata;

/**
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
import java.io.File;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.StringBufferInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.FileInputStream;
import java.sql.SQLException;
import java.sql.DatabaseMetaData;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import javax.jdo.JDOUnsupportedOptionException;
import javax.jdo.JDOUserException;
import javax.jdo.metadata.ColumnMetadata;
import javax.jdo.metadata.ForeignKeyMetadata;
import javax.jdo.metadata.JoinMetadata;

import com.knowgate.debug.*;

import org.judal.jdbc.RDBMS;
import org.judal.jdbc.jdc.JDCConnection;
import org.judal.metadata.ColumnDef;
import org.judal.metadata.Scriptable;
import org.judal.metadata.IndexDef.Type;
import org.judal.metadata.TableDef;
import org.judal.storage.Param;
import org.judal.storage.table.Record;


/**
 * <p>Represent database table structure as a Java object</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 */

public class SQLTableDef extends TableDef implements Scriptable {

	public static String DEFAULT_CREATION_TIMESTAMP_COLUMN_NAME = "dt_created";

	private RDBMS dbms;

	/**
	 * <p>Constructor</p>
	 * @param sTableName
	 */
	public SQLTableDef(RDBMS eDbms, String sTableName) {
		super(sTableName);
		dbms = eDbms;
		setCatalog(null);
		setSchema(null);
		setCreationTimestampColumnName(DEFAULT_CREATION_TIMESTAMP_COLUMN_NAME);
	}

	// ---------------------------------------------------------------------------
	
	/**
	 * Constructor
	 * @param sCatalogName Database catalog name
	 * @param sSchemaName Database schema name
	 * @param sTableName Database table name (not qualified)
	 */

	public SQLTableDef(RDBMS eDbms, String sCatalogName, String sSchemaName, String sTableName) {
		this(eDbms, sTableName);
		setCatalog(sCatalogName);
		setSchema(sSchemaName);
	}

	// ---------------------------------------------------------------------------

	public SQLTableDef(RDBMS eDbms, String sTableName, ColumnDef[] oCols) {
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

	/** Create a new column without adding it to the table definition.
	 * @return JDCColumn
	 */
	@Override
	protected SQLColumn createColumn() {
		return new SQLColumn();
	}

	// ---------------------------------------------------------------------------

	/**
	 * Get table name including the joined table if present
	 * @return String of the form "this_table_name [INNER|OUTER] JOIN joined_table_name ON this_column_name=joined_column_name"
	 */
	@Override
	public String getTable() throws JDOUserException,JDOUnsupportedOptionException {
		if (super.getTable()!=null) {
			return super.getTable();
		} else if (getNumberOfJoins()==0) {
			return getName();
		} else if (getNumberOfJoins()==1) {
			JoinMetadata join = getJoins()[0];
			for (ForeignKeyMetadata fk : getForeignKeys()) {
				if (fk.getTable().equals(join.getTable())) {
					if (fk.getNumberOfColumns()!=join.getNumberOfColumns())
						throw new JDOUserException("Foreign key "+fk.getName()+" columns "+fk.getColumns()+" do not match join columns "+join.getColumns());
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
	 * <p>Load a single table register into a Java HashMap</p>
	 * @param oConn Database Connection
	 * @param PKValues Primary key values of register to be read, in the same order as they appear in table source.
	 * @param AllValues Record Output parameter. Read values.
	 * @return <b>true</b> if register was found <b>false</b> otherwise.
	 * @throws NullPointerException If all objects in PKValues array are null (only debug version)
	 * @throws ArrayIndexOutOfBoundsException if the length of PKValues array does not match the number of primary key columns of the table
	 * @throws SQLException
	 */
	public boolean loadRegister(JDCConnection oConn, Object[] PKValues, Record AllValues)
			throws SQLException, NullPointerException, IllegalStateException, ArrayIndexOutOfBoundsException {
		int c;
		boolean bFound;
		Object oVal;
		Iterator<ColumnDef> oColIterator;
		PreparedStatement oStmt = null;
		ResultSet oRSet = null;
		Chronometer oChn = null;

		if (null==oConn)
			throw new NullPointerException("DBTable.loadRegister() Connection is null");

		if (DebugFile.trace) {
			oChn = new Chronometer();

			DebugFile.writeln("Begin DBTable.loadRegister([Connection:"+oConn.pid()+"], Object[], [HashMap])" );
			DebugFile.incIdent();

			boolean bAllNull = true;
			for (int n=0; n<PKValues.length; n++)
				bAllNull &= (PKValues[n]==null);

			if (bAllNull)
				throw new NullPointerException(getName() + " cannot retrieve register, value supplied for primary key is NULL.");
		}

		if (sqlSelect==null) {
			throw new SQLException("Primary key not found", "42S12");
		}

		AllValues.clear();

		bFound = false;

		try {

			if (DebugFile.trace) DebugFile.writeln("  Connection.prepareStatement(" + sqlSelect + ")");

			// Prepare SELECT sentence for reading
			oStmt = oConn.prepareStatement(sqlSelect);

			// Bind primary key values
			for (int p=0; p<getPrimaryKeyMetadata().getNumberOfColumns(); p++) {
				if (DebugFile.trace) DebugFile.writeln("  binding primary key " + PKValues[p] + ".");
				oConn.bindParameter(oStmt, p+1, PKValues[p]);
			} // next

			if (DebugFile.trace) DebugFile.writeln("  Connection.executeQuery()");

			oRSet = oStmt.executeQuery();

			if (oRSet.next()) {
				if (DebugFile.trace) DebugFile.writeln("  ResultSet.next()");

				bFound = true;

				// Iterate through read columns
				// and store read values at AllValues
				c = 1;
				for (ColumnDef oDBCol : getColumns()) {
					oVal = oRSet.getObject(c++);
					if (oRSet.wasNull()) {
						if (DebugFile.trace) DebugFile.writeln("Value of column "+oDBCol.getName()+" is NULL");
					} else {
						AllValues.put(oDBCol.getName(), oVal);
					}// fi
				}
			}

			if (DebugFile.trace) DebugFile.writeln("ResultSet.close()");

			oRSet.close();
			oRSet = null;

			if (DebugFile.trace) DebugFile.writeln("PreparedStatement.close()");

			oStmt.close();
			oStmt = null;
		}
		catch (SQLException sqle) {
			if (DebugFile.trace) {
				DebugFile.writeln("SQLException "+sqle.getMessage());
				DebugFile.decIdent();
			}
			try {
				if (null!=oRSet) oRSet.close();
				if (null!=oStmt) oStmt.close();
			}
			catch (Exception ignore) { }
			throw new SQLException(sqle.getMessage(), sqle.getSQLState(), sqle.getErrorCode());
		}

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("loading register took "+oChn.stop()+" ms");
			DebugFile.writeln("End DBTable.loadRegister() : " + (bFound ? "true" : "false"));
		}

		return bFound;

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
	 * @throws SQLException
	 */

	public boolean storeRegister(JDCConnection oConn, Record AllValues) throws SQLException {
		int c;
		boolean bNewRow = false;
		String sCol;
		String sSQL = "";
		ListIterator<String> oKeyIterator;
		int iAffected = -1;
		PreparedStatement oStmt = null;

		if (null==oConn)
			throw new NullPointerException("DBTable.storeRegister() Connection is null");

		if (DebugFile.trace)
		{
			DebugFile.writeln("Begin DBTable.storeRegister([Connection:"+oConn.pid()+"], {" + AllValues.toString() + "})" );
			DebugFile.incIdent();
		}

		try {
			if (null!=sqlUpdate) {
				if (DebugFile.trace) DebugFile.writeln("Connection.prepareStatement(" + sqlUpdate + ")");

				sSQL = sqlUpdate;

				oStmt = oConn.prepareStatement(sSQL);

				c = 1;

				for (ColumnDef oCol : getColumns()) {
					sCol = oCol.getName().toLowerCase();

					boolean isPkCol = false;
					for (ColumnMetadata col : getPrimaryKeyMetadata().getColumns())
						if (col.getName().equalsIgnoreCase(sCol)) {
							isPkCol = true;
							break;
						}

					if (!isPkCol &&
						(sCol.compareTo(getCreationTimestampColumnName())!=0) &&
						!oCol.getAutoIncrement()) {

						if (DebugFile.trace) {
							if (oCol.getType()==java.sql.Types.CHAR  || oCol.getType()==java.sql.Types.VARCHAR ||
									oCol.getType()==java.sql.Types.NCHAR || oCol.getType()==java.sql.Types.NVARCHAR ) {

								if (AllValues.apply(sCol)!=null) {
									DebugFile.writeln("Binding " + sCol + "=" + AllValues.apply(sCol).toString());

									if (AllValues.apply(sCol).toString().length() > oCol.getLength())
										DebugFile.writeln("ERROR: value for " + oCol.getName() + " exceeds columns precision of " + String.valueOf(oCol.getLength()));
								} // fi (AllValues.get(sCol)!=null)
								else
									DebugFile.writeln("Binding " + sCol + "=NULL");
							}
						} // fi (DebugFile.trace)

						try {
							c += oConn.bindParameter (oStmt, c, AllValues.apply(sCol), oCol.getType().shortValue());
						} catch (ClassCastException e) {
							if (AllValues.apply(sCol)!=null)
								throw new SQLException("ClassCastException at column " + sCol + " Cannot cast Java " + AllValues.apply(sCol).getClass().getName() + " to SQL type " + oCol.getType(), "07006");
							else
								throw new SQLException("ClassCastException at column " + sCol + " Cannot cast NULL to SQL type " + oCol.getType(), "07006");
						}

					} // endif (!oPrimaryKeys.contains(sCol))
				} // wend
				
				for (ColumnMetadata cmd : getPrimaryKeyMetadata().getColumns()) {
					ColumnDef oCol = getColumnByName(cmd.getName());
					c += oConn.bindParameter (oStmt, c, AllValues.apply(oCol.getName()), oCol.getType());
				} // wend

				if (DebugFile.trace) DebugFile.writeln("PreparedStatement.executeUpdate()");

				try {
					iAffected = oStmt.executeUpdate();
				} catch (SQLException sqle) {
					if (DebugFile.trace) {
						DebugFile.writeln("SQLException "+sqle.getMessage());
						DebugFile.decIdent();
					}
					oStmt.close();
					throw new SQLException(sqle.getMessage(), sqle.getSQLState(), sqle.getErrorCode());
				}

				if (DebugFile.trace) DebugFile.writeln(String.valueOf(iAffected) +  " affected rows");

				oStmt.close();
				oStmt = null;
			} // fi (sUpdate!=null)

			if (iAffected<=0) {
				bNewRow = true;

				if (DebugFile.trace) DebugFile.writeln("Connection.prepareStatement(" + sqlInsert + ")");

				sSQL = sqlInsert;

				oStmt = oConn.prepareStatement(sqlInsert);

				c = 1;

				for (ColumnDef oCol : getColumns()) {
					sCol = oCol.getName();

					if (!oCol.getAutoIncrement()) {
						if (DebugFile.trace) {
							if (null!=AllValues.apply(sCol))
								DebugFile.writeln("Binding " + sCol + "=" + AllValues.apply(sCol).toString());
							else
								DebugFile.writeln("Binding " + sCol + "=NULL");
						} // fi

						c += oConn.bindParameter (oStmt, c, AllValues.apply(sCol), oCol.getType());            	
					} // fi autoincrement
				} // wend

				if (DebugFile.trace) DebugFile.writeln("PreparedStatement.executeUpdate()");

				try {
					iAffected = oStmt.executeUpdate();
				} catch (SQLException sqle) {
					if (DebugFile.trace) {
						DebugFile.writeln("SQLException "+sqle.getMessage());
						DebugFile.decIdent();
					}
					oStmt.close();
					throw new SQLException(sqle.getMessage(), sqle.getSQLState(), sqle.getErrorCode());
				}

				if (DebugFile.trace) DebugFile.writeln(String.valueOf(iAffected) +  " affected rows");

				oStmt.close();
				oStmt =null;
			}
			else
				bNewRow = false;
		}
		catch (SQLException sqle) {
			try { if (null!=oStmt) oStmt.close(); } catch (Exception ignore) { }

			throw new SQLException (sqle.getMessage() + " " + sSQL, sqle.getSQLState(), sqle.getErrorCode());
		}

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End DBTable.storeRegister() : " + String.valueOf(bNewRow && (iAffected>0)));
		}

		return bNewRow && (iAffected>0);
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
	 * @throws SQLException
	 */

	public boolean storeRegisterLong(JDCConnection oConn, Record AllValues, Map<String,Long> BinaryLengths) throws IOException, SQLException {
		int c;
		boolean bNewRow = false;
		String sCol;
		PreparedStatement oStmt;
		int iAffected;
		LinkedList<InputStream> oStreams;
		InputStream oStream;
		String sClassName;

		if (null==oConn)
			throw new NullPointerException("DBTable.storeRegisterLong() Connection is null");

		if (DebugFile.trace)
		{
			DebugFile.writeln("Begin DBTable.storeRegisterLong([Connection:"+oConn.pid()+"], {" + AllValues.toString() + "})" );
			DebugFile.incIdent();
		}

		oStreams  = new LinkedList<InputStream>();

		if (null!=sqlUpdate) {

			if (DebugFile.trace) DebugFile.writeln("Connection.prepareStatement(" + sqlUpdate + ")");

			oStmt = oConn.prepareStatement(sqlUpdate);

			try { if (oConn.getDataBaseProduct()!=RDBMS.POSTGRESQL.intValue()) oStmt.setQueryTimeout(20); } catch (SQLException sqle) { if (DebugFile.trace) DebugFile.writeln("Error at PreparedStatement.setQueryTimeout(20)" + sqle.getMessage()); }

			c = 1;
			for (ColumnDef oCol : getColumns()) {
				sCol = oCol.getName().toLowerCase();

				boolean isPkCol = false;
				for (ColumnMetadata col : getPrimaryKeyMetadata().getColumns())
					if (col.getName().equalsIgnoreCase(sCol)) {
						isPkCol = true;
						break;
					}
				
				if (!isPkCol &&
					(!sCol.equalsIgnoreCase(getCreationTimestampColumnName())) &&
					!oCol.getAutoIncrement()) {

					if (DebugFile.trace) {
						if (oCol.getType()==java.sql.Types.CHAR || oCol.getType()==java.sql.Types.VARCHAR) {
							if (AllValues.apply(sCol) != null) {
								DebugFile.writeln("Binding " + sCol + "=" + AllValues.apply(sCol).toString());
								if (AllValues.apply(sCol).toString().length() > oCol.getLength())
									DebugFile.writeln("ERROR: value for " + oCol.getName() + " exceeds columns precision of " + String.valueOf(oCol.getLength()));
							} // fi (AllValues.get(sCol)!=null)
							else
								DebugFile.writeln("Binding " + sCol + "=NULL");
						}
					} // fi (DebugFile.trace)

					short cType = oCol.getType().shortValue();
					if (cType==java.sql.Types.LONGVARCHAR ||
							cType==java.sql.Types.CLOB ||
							cType==java.sql.Types.BINARY ||
							cType==java.sql.Types.VARBINARY ||
							cType==java.sql.Types.LONGVARBINARY ||
							cType==java.sql.Types.BLOB) {
						if (BinaryLengths.containsKey(sCol)) {
							if (((Long)BinaryLengths.get(sCol)).intValue()>0) {
								sClassName = AllValues.apply(sCol).getClass().getName();
								if (sClassName.equals("java.io.File"))
									oStream = new FileInputStream((File) AllValues.apply(sCol));
								else if (sClassName.equals("[B"))
									oStream = new ByteArrayInputStream((byte[]) AllValues.apply(sCol));
								else if (sClassName.equals("[C"))
									oStream = new StringBufferInputStream(new String((char[]) AllValues.apply(sCol)));
								else {
									Class[] aInts = AllValues.apply(sCol).getClass().getInterfaces();
									if (aInts==null) {
										throw new SQLException ("Invalid object binding for column " + sCol);
									} else {
										boolean bSerializable = false;
										for (int i=0; i<aInts.length &!bSerializable; i++)
											bSerializable |= aInts[i].getName().equals("java.io.Serializable");
										if (bSerializable) {
											ByteArrayOutputStream oBOut = new ByteArrayOutputStream();
											ObjectOutputStream oOOut = new ObjectOutputStream(oBOut);
											oOOut.writeObject(AllValues.apply(sCol));
											oOOut.close();
											ByteArrayInputStream oBin = new ByteArrayInputStream(oBOut.toByteArray());
											oStream = new ObjectInputStream(oBin);	                  
										} else {
											throw new SQLException ("Invalid object binding for column " + sCol);                      
										}
									} // fi
								}
								oStreams.addLast(oStream);
								int iStrmLen = ((Long)BinaryLengths.get(sCol)).intValue();
								if (DebugFile.trace) DebugFile.writeln("PreparedStatement.setBinaryStream("+String.valueOf(c+1)+","+oStream.getClass().getName()+","+String.valueOf(iStrmLen)+")");
								oStmt.setBinaryStream(c++, oStream, iStrmLen);
							}
							else {
								oStmt.setObject (c++, null, oCol.getType());
							}
						}
						else {
							oStmt.setObject (c++, null, oCol.getType());
						}
					}
					else {
						c += oConn.bindParameter (oStmt, c, AllValues.apply(sCol), oCol.getType());          	
					}
				} // fi (!oPrimaryKeys.contains(sCol))
			} // wend

			for (ColumnMetadata cmd : getPrimaryKeyMetadata().getColumns()) {
				ColumnDef oCol = getColumnByName(cmd.getName());
				c += oConn.bindParameter (oStmt, c, AllValues.apply(oCol.getName()), oCol.getType());
			} // wend

			if (DebugFile.trace) DebugFile.writeln("PreparedStatement.executeUpdate()");

			iAffected = oStmt.executeUpdate();

			if (DebugFile.trace) DebugFile.writeln(String.valueOf(iAffected) +  " affected rows");

			oStmt.close();

			ListIterator<InputStream> oStrmIterator = oStreams.listIterator();

			while (oStrmIterator.hasNext())
				oStrmIterator.next().close();

			oStreams.clear();

		}
		else
			iAffected = 0;

		if (0==iAffected)
		{
			bNewRow = true;

			if (DebugFile.trace) DebugFile.writeln("Connection.prepareStatement(" + sqlInsert + ")");

			oStmt = oConn.prepareStatement(sqlInsert);

			c = 1;
			for (ColumnDef oCol : getColumns()) {

				sCol = oCol.getName();

				if (!oCol.getAutoIncrement()) {
					if (DebugFile.trace) {
						if (null!=AllValues.apply(sCol))
							DebugFile.writeln("Binding " + sCol + "=" + AllValues.apply(sCol).toString());
						else
							DebugFile.writeln("Binding " + sCol + "=NULL");
					}
					short cType = ((SQLColumn)oCol).getSqlType();
					if (cType==java.sql.Types.LONGVARCHAR ||
							cType==java.sql.Types.CLOB ||
							cType==java.sql.Types.BINARY ||
							cType==java.sql.Types.VARBINARY ||
							cType==java.sql.Types.LONGVARBINARY ||
							cType==java.sql.Types.BLOB) {
						if (BinaryLengths.containsKey(sCol)) {
							if ( ( (Long) BinaryLengths.get(sCol)).intValue() > 0) {
								sClassName = AllValues.apply(sCol).getClass().getName();
								if (sClassName.equals("java.io.File"))
									oStream = new FileInputStream((File) AllValues.apply(sCol));
								else if (sClassName.equals("[B"))
									oStream = new ByteArrayInputStream((byte[]) AllValues.apply(sCol));
								else if (sClassName.equals("[C"))
									oStream = new StringBufferInputStream(new String((char[]) AllValues.apply(sCol)));
								else {
									Class[] aInts = AllValues.apply(sCol).getClass().getInterfaces();
									if (aInts==null) {
										throw new SQLException ("Invalid object binding for column " + sCol);
									} else {
										boolean bSerializable = false;
										for (int i=0; i<aInts.length &!bSerializable; i++)
											bSerializable |= aInts[i].getName().equals("java.io.Serializable");
										if (bSerializable) {
											ByteArrayOutputStream oBOut = new ByteArrayOutputStream();
											ObjectOutputStream oOOut = new ObjectOutputStream(oBOut);
											oOOut.writeObject(AllValues.apply(sCol));
											oOOut.close();
											ByteArrayInputStream oBin = new ByteArrayInputStream(oBOut.toByteArray());
											oStream = new ObjectInputStream(oBin);	                  
										} else {
											throw new SQLException ("Invalid object binding for column " + sCol);                      
										}
									} // fi
								}
								oStreams.addLast(oStream);
								int iStrmLen = ((Long)BinaryLengths.get(sCol)).intValue();
								if (DebugFile.trace) DebugFile.writeln("PreparedStatement.setBinaryStream("+String.valueOf(c+1)+","+oStream.getClass().getName()+","+String.valueOf(iStrmLen)+")");
								oStmt.setBinaryStream(c++, oStream, iStrmLen);
							}
							else
								oStmt.setObject(c++, null, oCol.getType());
						}
						else
							oStmt.setObject(c++, null, oCol.getType());
					}
					else
						c += oConn.bindParameter (oStmt, c, AllValues.apply(sCol), oCol.getType());          	
				} // fi autoincrement
			} // wend

			if (DebugFile.trace) DebugFile.writeln("PreparedStatement.executeUpdate()");

			iAffected = oStmt.executeUpdate();

			if (DebugFile.trace) DebugFile.writeln(String.valueOf(iAffected) +  " affected rows");

			oStmt.close();

			ListIterator<InputStream> oStrmIterator = oStreams.listIterator();

			while (oStrmIterator.hasNext())
				oStrmIterator.next().close();

			oStreams.clear();
		}

		else
			bNewRow = false;

		// End SQLException

		if (DebugFile.trace)
		{
			DebugFile.decIdent();
			DebugFile.writeln("End DBTable.storeRegisterLong() : " + String.valueOf(bNewRow));
		}

		return bNewRow;
	} // storeRegisterLong

	// ---------------------------------------------------------------------------

	/**
	 * <p>Delete a single register from this table at the database</p>
	 * @param oConn Database connection
	 * @param AllValues Record with, at least, the primary key values for the register. Other Record values are ignored.
	 * @return <b>true</b> if register was delete, <b>false</b> if register to be deleted was not found.
	 * @throws SQLException
	 */
	public boolean deleteRegister(JDCConnection oConn, Map<String,Object> AllValues) throws SQLException {
		int c;
		boolean bDeleted;
		PreparedStatement oStmt;
		ColumnMetadata oPK;
		ColumnDef oCol;

		if (DebugFile.trace)
		{
			DebugFile.writeln("Begin DBTable.deleteRegister([Connection], {" + AllValues.toString() + "})" );
			DebugFile.incIdent();
		}

		if (sqlDelete==null) {
			throw new SQLException("Primary key not found", "42S12");
		}

		// Begin SQLException

		if (DebugFile.trace) DebugFile.writeln("Connection.prepareStatement(" + sqlDelete + ")");

		oStmt = oConn.prepareStatement(sqlDelete);

		c = 1;

		while (c<=getPrimaryKeyMetadata().getNumberOfColumns()) {
			oPK = getPrimaryKeyMetadata().getColumns()[c-1];
			oCol = getColumnByName(oPK.getName());
			if (DebugFile.trace) DebugFile.writeln("PreparedStatement.setObject(" + String.valueOf(c) + "," + AllValues.get(oPK.getName()) + ")");
			oStmt.setObject (c++, AllValues.get(oPK.getName()), oCol.getType());
		} // wend

		if (DebugFile.trace) DebugFile.writeln("PreparedStatement.executeUpdate()");

		bDeleted = (oStmt.executeUpdate()>0);

		// End SQLException

		if (DebugFile.trace)
		{
			DebugFile.decIdent();
			DebugFile.writeln("End DBTable.deleteRegister() : " + (bDeleted ? "true" : "false"));
		}

		return bDeleted;
	} // deleteRegister

	// ---------------------------------------------------------------------------

	/**
	 * <p>Checks if register exists at this table</p>
	 * @param oConn Database Connection
	 * @param sQueryString Register Query String, as a SQL WHERE clause syntax
	 * @return <b>true</b> if register exists, <b>false</b> otherwise.
	 * @throws SQLException
	 */

	public boolean existsRegister(JDCConnection oConn, String sQueryString) throws SQLException {
		Statement oStmt = oConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

		ResultSet oRSet = oStmt.executeQuery("SELECT NULL FROM " + getName() + " WHERE " + sQueryString);
		boolean bExists = oRSet.next();
		oRSet.close();
		oStmt.close();

		return bExists;
	}

	// ---------------------------------------------------------------------------

	/**
	 * <p>Checks if register exists at this table</p>
	 * @param oConn Database Connection
	 * @param sQueryString Register Query String, as a SQL WHERE clause syntax
	 * @return <b>true</b> if register exists, <b>false</b> otherwise.
	 * @throws SQLException
	 */

	public boolean existsRegister(JDCConnection oConn, String sQueryString, Object[] oQueryParams) throws SQLException {
		PreparedStatement oStmt = oConn.prepareStatement("SELECT NULL FROM " + getName() + " WHERE " + sQueryString, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

		if (oQueryParams!=null) {
			for (int p=0; p<oQueryParams.length; p++)
				oStmt.setObject(p+1, oQueryParams[p]);
		}

		ResultSet oRSet = oStmt.executeQuery();
		boolean bExists = oRSet.next();
		oRSet.close();
		oStmt.close();

		return bExists;
	}

	// ---------------------------------------------------------------------------

	/**
	 * <p>Checks if register exists at this table</p>
	 * @param oConn Database Connection
	 * @param AllValues Map<String,Object>
	 * @return <b>true</b> if register exists, <b>false</b> otherwise.
	 * @throws SQLException
	 */

	public boolean existsRegister(JDCConnection oConn, Map<String,Object> AllValues) throws SQLException {
		int c;
		boolean bExists;
		PreparedStatement oStmt;
		ResultSet oRSet;
		ColumnMetadata oPK;
		ColumnDef oCol;

		if (DebugFile.trace)
		{
			DebugFile.writeln("Begin DBTable.existsRegister([Connection], {" + AllValues.toString() + "})" );
			DebugFile.incIdent();
		}

		oStmt = oConn.prepareStatement(sqlExists, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

		c = 1;
		while (c<=getPrimaryKeyMetadata().getNumberOfColumns()) {
			oPK = getPrimaryKeyMetadata().getColumns()[c-1];
			oCol = getColumnByName(oPK.getName());
			oStmt.setObject (c++, AllValues.get(oPK.getName()), oCol.getType());
		} // wend

		oRSet = oStmt.executeQuery();
		bExists = oRSet.next();

		oRSet.close();
		oStmt.close();

		if (DebugFile.trace)
		{
			DebugFile.decIdent();
			DebugFile.writeln("End DBTable.existsRegister() : " + String.valueOf(bExists));
		}

		return bExists;
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

	public void precomputeSqlStatements(int dbms) throws SQLException {

		String insertAllCols = "";
		String getAllCols = "";
		String setAllCols = "";
		String setPkCols = "";
		String setNoPkCols = "";
		
		if (DebugFile.trace)
		{
			DebugFile.writeln("Begin JDCTableDef.readColumns([DatabaseMetaData])" );
			DebugFile.incIdent();
			DebugFile.writeln("DatabaseMetaData.getColumns(" + getCatalog() + "," + getSchema() + "," + getName() + ",%)");
		}


			for (ColumnDef column : getColumns()) {
				String columnName = column.getName();

				if (column.isPrimaryKey())
					setPkCols += column.getName() + "=? AND ";
				
				if (dbms==RDBMS.POSTGRESQL.intValue()) {
					if (((SQLColumn) column).getSqlTypeName().equalsIgnoreCase("geography")) {
						insertAllCols  += columnName+",";
						getAllCols  += "ST_X("+columnName+"::geometry)||' '||ST_Y("+columnName+"::geometry) AS "+columnName+",";        	  
						setAllCols  += "ST_SetSRID(ST_MakePoint(?,?),4326),";
						setNoPkCols += columnName + "=ST_SetSRID(ST_MakePoint(?,?),4326),";
					} else if (((SQLColumn) column).getSqlTypeName().equalsIgnoreCase("serial")) {
						getAllCols += columnName + ",";
					} else {
						insertAllCols += columnName + ",";        	 
						getAllCols += columnName + ",";
						setAllCols += "?,";
						if (!column.isPrimaryKey() && !columnName.equalsIgnoreCase(getCreationTimestampColumnName()))
							setNoPkCols += columnName + "=?,";
					}  
				} else {
					if (column.getAutoIncrement()) {
						getAllCols += columnName + ",";        		
					} else {
						insertAllCols += columnName + ",";        	 
						getAllCols += columnName + ",";
						setAllCols += "?,";
						if (!column.isPrimaryKey() && !columnName.equalsIgnoreCase(getCreationTimestampColumnName()))
							setNoPkCols += columnName + "=?,";        		
					}
				}
			} // wend

			if (setPkCols.length()>0)
				setPkCols = setPkCols.substring(0, setPkCols.length()-5);
			
			if (DebugFile.trace) DebugFile.writeln("get all cols " + getAllCols );

			if (getAllCols.length()>0)
				getAllCols = getAllCols.substring(0, getAllCols.length()-1);
			else
				getAllCols = "*";

			if (insertAllCols.length()>0)
				insertAllCols = insertAllCols.substring(0, insertAllCols.length()-1);

			if (DebugFile.trace) DebugFile.writeln("set all cols " + setAllCols );

			if (setAllCols.length()>0)
				setAllCols = setAllCols.substring(0, setAllCols.length()-1);

			if (DebugFile.trace) DebugFile.writeln("set no pk cols " + setNoPkCols );

			if (setNoPkCols.length()>0)
				setNoPkCols = setNoPkCols.substring(0, setNoPkCols.length()-1);

			if (DebugFile.trace) DebugFile.writeln("set pk cols " + setPkCols );

			if (setPkCols.length()>0) {
				sqlSelect = "SELECT " + getAllCols + " FROM " + getName() + " WHERE " + setPkCols;
				sqlInsert = "INSERT INTO " + getName() + "(" + insertAllCols + ") VALUES (" + setAllCols + ")";
				if (setNoPkCols.length()>0)
					sqlUpdate = "UPDATE " + getName() + " SET " + setNoPkCols + " WHERE " + setPkCols;
				else
					sqlUpdate = null;
				sqlDelete = "DELETE FROM " + getName() + " WHERE " + setPkCols;
				sqlExists = "SELECT NULL FROM " + getName() + " WHERE " + setPkCols;
			}
			else {
				sqlSelect = null;
				sqlInsert = "INSERT INTO " + getName() + "(" + insertAllCols + ") VALUES (" + setAllCols + ")";
				sqlUpdate = null;
				sqlDelete = null;
				sqlExists = null;
			}


		if (DebugFile.trace)
		{
			DebugFile.writeln(sqlSelect!=null ? sqlSelect : "NO SELECT STATEMENT");
			DebugFile.writeln(sqlInsert!=null ? sqlInsert : "NO INSERT STATEMENT");
			DebugFile.writeln(sqlUpdate!=null ? sqlUpdate : "NO UPDATE STATEMENT");
			DebugFile.writeln(sqlDelete!=null ? sqlDelete : "NO DELETE STATEMENT");
			DebugFile.writeln(sqlExists!=null ? sqlExists : "NO EXISTS STATEMENT");

			DebugFile.decIdent();
			DebugFile.writeln("End JDCTableDef.readColumns()");
		}

	} // precomputeSqlStatements

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
			builder.append("CONSTRAINT ").append((getPrimaryKeyMetadata().getName()==null ? "pk_"+getName() : getPrimaryKeyMetadata().getName())).append(" PRIMARY KEY (").append(String.join(",", pkcols)).append("),\n");
		}
		int n = 0;
		for (ColumnDef c : getColumns()) {
			if (c.getConstraint()!=null)
				builder.append("CONSTRAINT ck_").append(String.valueOf(++n)+"_"+getName()).append(" CHECK ("+c.getConstraint()+"),\n");
		}
		n = 0;
		if (getForeignKeys()!=null) {
			for (ForeignKeyMetadata fk : getForeignKeys()) {
				builder.append("CONSTRAINT ").append(fk.getName()==null ? "fk_"+String.valueOf(++n)+getName() : fk.getName());
				ArrayList<String> fkcols = new ArrayList<String>(fk.getColumns().length);
				for (ColumnMetadata c : fk.getColumns())
					fkcols.add(c.getName());
				builder.append(" (").append(String.join(",", fkcols)).append(") REFERENCES ").append(fk.getTable());
				fkcols.clear();
				for (ColumnMetadata c : fk.getColumns())
					fkcols.add(getColumnByName(c.getName()).getTarget());
				builder.append(" (").append(String.join(",", fkcols)).append("),\n").append(fk.getTable());
			}			
		}
		builder.setLength(builder.length()-2); // remove trailing comma
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

	// ----------------------------------------------------------
	
	private String sqlSelect;
	private String sqlInsert;
	private String sqlUpdate;
	private String sqlDelete;
	private String sqlExists;

} // DBTable
