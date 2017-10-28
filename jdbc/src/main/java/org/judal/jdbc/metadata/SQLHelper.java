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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.StringBufferInputStream;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Map;

import javax.jdo.metadata.ColumnMetadata;

import org.judal.jdbc.RDBMS;
import org.judal.jdbc.jdc.JDCConnection;
import org.judal.metadata.ColumnDef;
import org.judal.metadata.TypeDef;
import org.judal.storage.table.Record;

import com.knowgate.debug.Chronometer;
import com.knowgate.debug.DebugFile;

/**
 * <p>Helper functions for loading, storing and deleting rows from a SQL table.</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class SQLHelper {

	private String sqlSelect;
	private String sqlInsert;
	private String sqlUpdate;
	private String sqlDelete;
	private String sqlExists;
	private TypeDef tdef;
	private String timestampColumn;
	
	// ----------------------------------------------------------

	/**
	 * @param dbms int Integer code from RDMBS enum
	 * @param tdef TypeDef
	 * @param timestampColumn String Name of creation timestamp column. If provided, the value of this column will not be updated after it has been initially set.
	 * @throws SQLException
	 */
	public SQLHelper(int dbms, TypeDef tdef, String timestampColumn) throws SQLException {
		this.tdef = tdef;
		this.timestampColumn= timestampColumn;
		precomputeSqlStatements(dbms);
	}

	// ----------------------------------------------------------

	/**
	 * @param dbms RDBMS
	 * @param tdef TypeDef
	 * @param timestampColumn String Name of creation timestamp column. If provided, the value of this column will not be updated after it has been initially set.
	 * @throws SQLException
	 */
	public SQLHelper(RDBMS dbms, TypeDef tdef, String timestampColumn) throws SQLException {
		this.tdef = tdef;
		this.timestampColumn= timestampColumn;
		precomputeSqlStatements(dbms.intValue());
	}
	
	// ----------------------------------------------------------

	private void precomputeSqlStatements(int dbms) throws SQLException {

		String insertAllCols = "";
		String getAllCols = "";
		String setAllCols = "";
		String setPkCols = "";
		String setNoPkCols = "";

		if (DebugFile.trace)
		{
			DebugFile.writeln("Begin SQLHelper.precomputeSqlStatements([DatabaseMetaData])" );
			DebugFile.incIdent();
			DebugFile.writeln("DatabaseMetaData.getColumns(" + tdef.getCatalog() + "," + tdef.getSchema() + "," + tdef.getName() + ",%)");
		}


		for (ColumnDef column : tdef.getColumns()) {
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
					if (!column.isPrimaryKey() && !columnName.equalsIgnoreCase(timestampColumn))
						setNoPkCols += columnName + "=?,";
				}  
			} else {
				if (column.getAutoIncrement()) {
					getAllCols += columnName + ",";        		
				} else {
					insertAllCols += columnName + ",";        	 
					getAllCols += columnName + ",";
					setAllCols += "?,";
					if (!column.isPrimaryKey() && !columnName.equalsIgnoreCase(timestampColumn))
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
			sqlSelect = "SELECT " + getAllCols + " FROM " + tdef.getName() + " WHERE " + setPkCols;
			sqlInsert = "INSERT INTO " + tdef.getName() + "(" + insertAllCols + ") VALUES (" + setAllCols + ")";
			if (setNoPkCols.length()>0)
				sqlUpdate = "UPDATE " + tdef.getName() + " SET " + setNoPkCols + " WHERE " + setPkCols;
			else
				sqlUpdate = null;
			sqlDelete = "DELETE FROM " + tdef.getName() + " WHERE " + setPkCols;
			sqlExists = "SELECT NULL FROM " + tdef.getName() + " WHERE " + setPkCols;
		}
		else {
			sqlSelect = null;
			sqlInsert = "INSERT INTO " + tdef.getName() + "(" + insertAllCols + ") VALUES (" + setAllCols + ")";
			sqlUpdate = null;
			sqlDelete = null;
			sqlExists = null;
		}


		if (null==sqlSelect) {
			throw new SQLException("NO PK FOUND!");
		}

		if (DebugFile.trace)
		{
			DebugFile.writeln(sqlSelect!=null ? sqlSelect : "NO SELECT STATEMENT");
			DebugFile.writeln(sqlInsert!=null ? sqlInsert : "NO INSERT STATEMENT");
			DebugFile.writeln(sqlUpdate!=null ? sqlUpdate : "NO UPDATE STATEMENT");
			DebugFile.writeln(sqlDelete!=null ? sqlDelete : "NO DELETE STATEMENT");
			DebugFile.writeln(sqlExists!=null ? sqlExists : "NO EXISTS STATEMENT");

			DebugFile.decIdent();
			DebugFile.writeln("End SQLHelper.precomputeSqlStatements()");
		}

	} // precomputeSqlStatements
	
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
			throw new NullPointerException("SQLHelper.loadRegister() Connection is null");

		if (DebugFile.trace) {
			oChn = new Chronometer();

			DebugFile.writeln("Begin SQLHelper.loadRegister([Connection:"+oConn.pid()+"], Object[], [HashMap])" );
			DebugFile.incIdent();

			boolean bAllNull = true;
			for (int n=0; n<PKValues.length; n++)
				bAllNull &= (PKValues[n]==null);

			if (bAllNull)
				throw new NullPointerException(tdef.getName() + " cannot retrieve register, value supplied for primary key is NULL.");
		}

		if (sqlSelect==null) {
			throw new SQLException("SQLHelper.loadRegister() Primary key not found", "42S12");
		}

		AllValues.clear();

		bFound = false;

		try {

			if (DebugFile.trace) DebugFile.writeln("  Connection.prepareStatement(" + sqlSelect + ")");

			// Prepare SELECT sentence for reading
			oStmt = oConn.prepareStatement(sqlSelect);

			// Bind primary key values
			for (int p=0; p<tdef.getPrimaryKeyMetadata().getNumberOfColumns(); p++) {
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
				for (ColumnDef oDBCol : tdef.getColumns()) {
					oVal = oRSet.getObject(c++);
					if (oRSet.wasNull()) {
						if (DebugFile.trace) DebugFile.writeln("Value of column " + oDBCol.getName() + " is NULL");
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
			DebugFile.writeln("End SQLHelper.loadRegister() : " + (bFound ? "true" : "false"));
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
			throw new NullPointerException("SQLHelper.storeRegister() Connection is null");

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

				for (ColumnDef oCol : tdef.getColumns()) {
					sCol = oCol.getName().toLowerCase();

					boolean isPkCol = false;
					for (ColumnMetadata col : tdef.getPrimaryKeyMetadata().getColumns())
						if (col.getName().equalsIgnoreCase(sCol)) {
							isPkCol = true;
							break;
						}

					if (!isPkCol &&
						(sCol.compareTo(timestampColumn)!=0) &&
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
				
				for (ColumnMetadata cmd : tdef.getPrimaryKeyMetadata().getColumns()) {
					ColumnDef oCol = tdef.getColumnByName(cmd.getName());
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

				for (ColumnDef oCol : tdef.getColumns()) {
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
			DebugFile.writeln("End SQLHelper.storeRegister() : " + String.valueOf(bNewRow && (iAffected>0)));
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
			throw new NullPointerException("SQLHelper.storeRegisterLong() Connection is null");

		if (DebugFile.trace)
		{
			DebugFile.writeln("Begin SQLHelper.storeRegisterLong([Connection:"+oConn.pid()+"], {" + AllValues.toString() + "})" );
			DebugFile.incIdent();
		}

		oStreams  = new LinkedList<InputStream>();

		if (null!=sqlUpdate) {

			if (DebugFile.trace) DebugFile.writeln("Connection.prepareStatement(" + sqlUpdate + ")");

			oStmt = oConn.prepareStatement(sqlUpdate);

			try { if (oConn.getDataBaseProduct()!=RDBMS.POSTGRESQL.intValue()) oStmt.setQueryTimeout(20); } catch (SQLException sqle) { if (DebugFile.trace) DebugFile.writeln("Error at PreparedStatement.setQueryTimeout(20)" + sqle.getMessage()); }

			c = 1;
			for (ColumnDef oCol : tdef.getColumns()) {
				sCol = oCol.getName().toLowerCase();

				boolean isPkCol = false;
				for (ColumnMetadata col : tdef.getPrimaryKeyMetadata().getColumns())
					if (col.getName().equalsIgnoreCase(sCol)) {
						isPkCol = true;
						break;
					}
				
				if (!isPkCol &&
					(!sCol.equalsIgnoreCase(timestampColumn)) &&
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

			for (ColumnMetadata cmd : tdef.getPrimaryKeyMetadata().getColumns()) {
				ColumnDef oCol = tdef.getColumnByName(cmd.getName());
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
			for (ColumnDef oCol : tdef.getColumns()) {

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
			throw new SQLException("SQLHelper.deleteRegister() Primary key not found", "42S12");
		}

		// Begin SQLException

		if (DebugFile.trace) DebugFile.writeln("Connection.prepareStatement(" + sqlDelete + ")");

		oStmt = oConn.prepareStatement(sqlDelete);

		c = 1;

		while (c<=tdef.getPrimaryKeyMetadata().getNumberOfColumns()) {
			oPK = tdef.getPrimaryKeyMetadata().getColumns()[c-1];
			oCol = tdef.getColumnByName(oPK.getName());
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
		
		boolean bExists = false;
		try (Statement oStmt = oConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			 ResultSet oRSet = oStmt.executeQuery("SELECT NULL FROM " + tdef.getName() + " WHERE " + sQueryString)) {
			bExists = oRSet.next();
		}
		
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
		boolean bExists = false;
		try (PreparedStatement oStmt = oConn.prepareStatement("SELECT NULL FROM " + tdef.getName() + " WHERE " + sQueryString, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
			if (oQueryParams!=null) {
				for (int p=0; p<oQueryParams.length; p++) {
					int sqlType;
					Object value = oQueryParams[p];
					if (value instanceof Boolean)
						sqlType = Types.BOOLEAN;
					else if (value instanceof Short)
						sqlType = Types.SMALLINT;
					else if (value instanceof Integer)
						sqlType = Types.INTEGER;
					else if (value instanceof Long)
						sqlType = Types.BIGINT;
					else if (value instanceof Float)
						sqlType = Types.FLOAT;
					else if (value instanceof Double)
						sqlType = Types.DOUBLE;
					else if (value instanceof BigDecimal)
						sqlType = Types.DECIMAL;
					else if (value instanceof String)
						sqlType = Types.VARCHAR;
					else
						sqlType = Types.OTHER;
					if (Types.OTHER==sqlType)
						oStmt.setObject(p+1, oQueryParams[p], sqlType);
					else
						oStmt.setObject(p+1, oQueryParams[p]);						
				}
			}

			ResultSet oRSet = oStmt.executeQuery();
			bExists = oRSet.next();
			oRSet.close();
		}
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
		while (c<=tdef.getPrimaryKeyMetadata().getNumberOfColumns()) {
			oPK = tdef.getPrimaryKeyMetadata().getColumns()[c-1];
			oCol = tdef.getColumnByName(oPK.getName());
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
	
}