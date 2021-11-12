package org.judal.jdbc.jdc;

/*
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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Map;

import javax.jdo.metadata.ColumnMetadata;

import org.judal.jdbc.HStore;
import org.judal.jdbc.RDBMS;
import org.judal.jdbc.jdc.JDCConnection;
import org.judal.jdbc.metadata.SQLColumn;
import org.judal.jdbc.metadata.SQLStatements;
import org.judal.metadata.ColumnDef;
import org.judal.metadata.TypeDef;
import org.judal.storage.table.Record;

import com.knowgate.debug.Chronometer;
import com.knowgate.debug.DebugFile;
import com.knowgate.gis.LatLong;

/**
 * <p>Helper functions for loading, storing and deleting rows from a SQL table.</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 */
@SuppressWarnings("deprecation")
public class JDCDAO {

	private SQLStatements sqlStatements;
	private TypeDef tdef;
	
	// ----------------------------------------------------------

	public JDCDAO(TypeDef tdef, SQLStatements sqlStatements) throws SQLException {
		this.tdef = tdef;
		this.sqlStatements = sqlStatements;
	}

	// ---------------------------------------------------------------------------

	public static Object toJavaObject(Object oObj, String sColName, int iColType) {
		Object oRetVal = oObj;
		if (iColType==Types.OTHER) {
			final String pgObjectClass = oObj.getClass().getName();
			if (pgObjectClass.equals("org.postgresql.util.PGobject") ||
				pgObjectClass.equals("org.openstreetmap.osmosis.hstore.PGHStore")) {
				try {
					Method getType = oObj.getClass().getMethod("getType");
					Method getValue = oObj.getClass().getMethod("getValue");
					String pgObjectValue = (String) getValue.invoke(oObj);
					String pgObjectType = (String) getType.invoke(oObj);
					if (DebugFile.trace)
						DebugFile.writeln("Column " + sColName + " is PGObject of type " + pgObjectType + " with value " + pgObjectValue);
					if (pgObjectType.toLowerCase().indexOf("hstore")>=0) {
						oRetVal = new HStore(pgObjectValue).asMap();
					} else if (pgObjectValue.matches("-?\\d+(\\x2E\\d+)? -?\\d+(\\x2E\\d+)?")) {
						String[] aLatLng = pgObjectValue.split(" ");
						oRetVal = new LatLong(Float.parseFloat(aLatLng[0]), Float.parseFloat(aLatLng[1]));
					}
				} catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
					if (DebugFile.trace)
						DebugFile.writeln(e.getClass().getName() + " " + e.getMessage());
				}
			}
		}
		return oRetVal;
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
	 * @throws NullPointerException If oConn is null.
	 */
	public boolean loadRegister(JDCConnection oConn, Object[] PKValues, Record AllValues)
			throws SQLException, NullPointerException, IllegalStateException, ArrayIndexOutOfBoundsException {
		int c;
		boolean bFound;
		Object oVal;
		PreparedStatement oStmt = null;
		ResultSet oRSet = null;
		Chronometer oChn = null;

		if (null==oConn)
			throw new NullPointerException("SQLHelper.loadRegister() Connection is null");

		if (DebugFile.trace) {
			oChn = new Chronometer();

			DebugFile.writeln("Begin JDCDAO.loadRegister(" + oConn + ", Object[], [HashMap])" );
			DebugFile.incIdent();
			DebugFile.writeln("XId=" + (oConn.getId()==null ? "null" : oConn.getId().toString()));

			boolean bAllNull = true;
			for (int n=0; n<PKValues.length; n++)
				bAllNull &= (PKValues[n]==null);

			if (bAllNull)
				throw new NullPointerException(tdef.getName() + " cannot retrieve register, value supplied for primary key is NULL.");
		}

		if (sqlStatements.getSelect()==null) {
			throw new SQLException("JDCDAO.loadRegister() Primary key not found", "42S12");
		}

		AllValues.clear();

		bFound = false;

		try {

			if (DebugFile.trace) DebugFile.writeln("  Connection.prepareStatement(" + sqlStatements.getSelect() + ")");

			// Prepare SELECT sentence for reading
			oStmt = oConn.prepareStatement(sqlStatements.getSelect());

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
						AllValues.put(oDBCol.getName(), toJavaObject(oVal, oDBCol.getName(), oDBCol.getType()));
					} // fi
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
				DebugFile.writeln(sqle.getClass().getName() + " "+sqle.getMessage() + ", SQLState=" + sqle.getSQLState() + ", ErrorCode=" + sqle.getErrorCode());
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
			DebugFile.writeln("loading register took " + oChn.stop() + " ms");
			DebugFile.writeln("End JDCDAO.loadRegister() : " + (bFound ? "true" : "false"));
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
	 * @throws NullPointerException If oConn is null.
	 */
	public boolean storeRegister(JDCConnection oConn, Record AllValues) throws SQLException {
		int c;
		boolean bNewRow = false;
		String sCol;
		String sSQL = "";
		int iAffected = -1;
		PreparedStatement oStmt = null;

		if (null==oConn)
			throw new NullPointerException("JDCDAO.storeRegister() Connection is null");

		if (DebugFile.trace)
		{
			DebugFile.writeln("Begin JDCDAO.storeRegister(" + oConn +", {" + AllValues.toString() + "})" );
			DebugFile.incIdent();
			DebugFile.writeln("XId=" + (oConn.getId()==null ? "null" : oConn.getId().toString()));
		}

		try {
			if (null!=sqlStatements.getUpdate()) {
				if (DebugFile.trace) DebugFile.writeln("Connection.prepareStatement(" + sqlStatements.getUpdate() + ")");

				sSQL = sqlStatements.getUpdate();

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
						(!sCol.equals(sqlStatements.getTimestampColumn())) &&
						!oCol.getAutoIncrement()) {

						if (DebugFile.trace) {
							if (oCol.getType()==java.sql.Types.CHAR  || oCol.getType()==java.sql.Types.VARCHAR ||
									oCol.getType()==java.sql.Types.NCHAR || oCol.getType()==java.sql.Types.NVARCHAR ) {

								if (AllValues.apply(sCol)!=null) {
									if (AllValues.apply(sCol).toString().length() > oCol.getLength())
										DebugFile.writeln("ERROR: value for " + oCol.getName() + " exceeds columns precision of " + String.valueOf(oCol.getLength()));
								} // fi (AllValues.get(sCol)!=null)
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
					oStmt = null;
					throw new SQLException(sqle.getMessage(), sqle.getSQLState(), sqle.getErrorCode());
				}

				if (DebugFile.trace) DebugFile.writeln(String.valueOf(iAffected) +  " affected rows");

				oStmt.close();
				oStmt = null;
			} // fi (sUpdate!=null)

			if (iAffected<=0) {
				bNewRow = true;

				sSQL = sqlStatements.getInsert();

				if (DebugFile.trace) DebugFile.writeln("Connection.prepareStatement(" + sqlStatements.getInsert() + ")");

				oStmt = oConn.prepareStatement(sqlStatements.getInsert());

				c = 1;

				for (ColumnDef oCol : tdef.getColumns()) {
					sCol = oCol.getName();

					if (!oCol.getAutoIncrement()) {

						// if (DebugFile.trace) {
						//	if (null!=AllValues.apply(sCol))
						//		DebugFile.writeln("Binding " + sCol + "=" + AllValues.apply(sCol).toString());
						//	else
						//		DebugFile.writeln("Binding " + sCol + "=NULL");
						// }

						c += oConn.bindParameter (oStmt, c, AllValues.apply(sCol), oCol.getType());
					} // fi autoincrement
				} // wend

				if (DebugFile.trace) DebugFile.writeln("PreparedStatement.executeUpdate()");

				iAffected = oStmt.executeUpdate();

				if (DebugFile.trace) DebugFile.writeln(String.valueOf(iAffected) +  " affected rows");

				oStmt.close();
				oStmt = null;
			}
			else
				bNewRow = false;
		}
		catch (SQLException sqle) {
			if (DebugFile.trace) {
				DebugFile.writeln(sqle.getClass().getName() + " " + sqle.getMessage() + ", SQLState=" + sqle.getSQLState() +", ErrorCode=" + sqle.getErrorCode() + ", Connection Name=" + oConn.getName() + ", XId=" + (oConn==null ? "null" : oConn.getId().toString()) + ", SQL=" + sSQL);
				DebugFile.decIdent();
			}

			try { if (null!=oStmt) oStmt.close(); } catch (Exception ignore) { }

			throw new SQLException (sqle.getMessage() + " " + sSQL, sqle.getSQLState(), sqle.getErrorCode());
		}

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End JDCDAO.storeRegister() : " + String.valueOf(bNewRow && (iAffected>0)));
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
	 * @throws NullPointerException If oConn is null.
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
			throw new NullPointerException("JDCDAO.storeRegisterLong() Connection is null");

		if (DebugFile.trace)
		{
			DebugFile.writeln("Begin JDCDAO.storeRegisterLong(" + oConn +", {" + AllValues.toString() + "})" );
			DebugFile.incIdent();
			DebugFile.writeln("XId=" + (oConn.getId()==null ? "null" : oConn.getId().toString()));
		}

		oStreams  = new LinkedList<InputStream>();

		if (null!=sqlStatements.getUpdate()) {

			if (DebugFile.trace) DebugFile.writeln("Connection.prepareStatement(" + sqlStatements.getUpdate() + ")");

			oStmt = oConn.prepareStatement(sqlStatements.getUpdate());

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
					(!sCol.equalsIgnoreCase(sqlStatements.getTimestampColumn())) &&
					!oCol.getAutoIncrement()) {

					if (DebugFile.trace) {
						if (oCol.getType()==java.sql.Types.CHAR || oCol.getType()==java.sql.Types.VARCHAR) {
							if (AllValues.apply(sCol) != null) {
								// DebugFile.writeln("Binding " + sCol + "=" + AllValues.apply(sCol).toString());
								if (AllValues.apply(sCol).toString().length() > oCol.getLength())
									DebugFile.writeln("ERROR: value for " + oCol.getName() + " exceeds columns precision of " + String.valueOf(oCol.getLength()));
							} // fi (AllValues.get(sCol)!=null)
							// else
							//	DebugFile.writeln("Binding " + sCol + "=NULL");
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

			if (DebugFile.trace) DebugFile.writeln("Connection.prepareStatement(" + sqlStatements.getInsert() + ")");

			oStmt = oConn.prepareStatement(sqlStatements.getInsert());

			c = 1;
			for (ColumnDef oCol : tdef.getColumns()) {

				sCol = oCol.getName();

				if (!oCol.getAutoIncrement()) {
					if (DebugFile.trace) {
						
						// if (null!=AllValues.apply(sCol))
						//	 DebugFile.writeln("Binding " + sCol + "=" + AllValues.apply(sCol).toString());
						// else
						//	 DebugFile.writeln("Binding " + sCol + "=NULL");
					}
					short cType = ((SQLColumn) oCol).getSqlType();
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
			DebugFile.writeln("End JDCDAO.storeRegisterLong() : " + String.valueOf(bNewRow));
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
	 * @throws NullPointerException If oConn is null.
	 */
	public boolean deleteRegister(JDCConnection oConn, Map<String,Object> AllValues) throws SQLException {
		int c;
		boolean bDeleted;
		PreparedStatement oStmt;
		ColumnMetadata oPK;
		ColumnDef oCol;

		if (null==oConn)
			throw new NullPointerException("JDCDAO.deleteRegister() Connection is null");

		if (DebugFile.trace)
		{
			DebugFile.writeln("Begin JDCDAO.deleteRegister(" + oConn + ", {" + AllValues.toString() + "})" );
			DebugFile.incIdent();
			DebugFile.writeln("XId=" + (oConn.getId()==null ? "null" : oConn.getId().toString()));
		}

		if (sqlStatements.getDelete()==null) {
			throw new SQLException("JDCDAO.deleteRegister() Primary key not found", "42S12");
		}

		if (DebugFile.trace) DebugFile.writeln("Connection.prepareStatement(" + sqlStatements.getDelete() + ")");

		oStmt = oConn.prepareStatement(sqlStatements.getDelete());

		c = 1;

		while (c<=tdef.getPrimaryKeyMetadata().getNumberOfColumns()) {
			oPK = tdef.getPrimaryKeyMetadata().getColumns()[c-1];
			oCol = tdef.getColumnByName(oPK.getName());
			if (DebugFile.trace) DebugFile.writeln("PreparedStatement.setObject(" + String.valueOf(c) + "," + AllValues.get(oPK.getName()) + ")");
			oStmt.setObject (c++, AllValues.get(oPK.getName()), oCol.getType());
		} // wend

		if (DebugFile.trace) DebugFile.writeln("PreparedStatement.executeUpdate()");

		bDeleted = (oStmt.executeUpdate()>0);

		if (DebugFile.trace)
		{
			DebugFile.decIdent();
			DebugFile.writeln("End JDCDAO.deleteRegister() : " + (bDeleted ? "true" : "false"));
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
	 * @throws NullPointerException If oConn is null.
	 */
	public boolean existsRegister(JDCConnection oConn, String sQueryString, Object[] oQueryParams) throws SQLException {
		if (null==oConn)
			throw new NullPointerException("JDCDAO.existsRegister() Connection is null");

		boolean bExists = false;
		ResultSet oRSet = null;
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

			oRSet = oStmt.executeQuery();
			bExists = oRSet.next();
		} finally {
			if (oRSet!=null) oRSet.close();
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
	 * @throws NullPointerException If oConn is null.
	 */
	public boolean existsRegister(JDCConnection oConn, Map<String,Object> AllValues) throws SQLException {
		int c;
		boolean bExists;
		PreparedStatement oStmt = null;
		ResultSet oRSet = null;
		ColumnMetadata oPK;
		ColumnDef oCol;

		if (null==oConn)
			throw new NullPointerException("JDCDAO.existsRegister() Connection is null");

		if (DebugFile.trace)
		{
			DebugFile.writeln("Begin JDCDAO.existsRegister(" + oConn + ", {" + AllValues.toString() + "})" );
			DebugFile.incIdent();
			DebugFile.writeln("XId=" + (oConn.getId()==null ? "null" : oConn.getId().toString()));
		}

		try {
			oStmt = oConn.prepareStatement(sqlStatements.getExists(), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

			c = 1;
			while (c<=tdef.getPrimaryKeyMetadata().getNumberOfColumns()) {
				oPK = tdef.getPrimaryKeyMetadata().getColumns()[c-1];
				oCol = tdef.getColumnByName(oPK.getName());
				oStmt.setObject (c++, AllValues.get(oPK.getName()), oCol.getType());
			} // wend

			oRSet = oStmt.executeQuery();
			bExists = oRSet.next();

			oRSet.close();
			oRSet = null;
			oStmt.close();
			oStmt = null;

		} finally {
			if (oRSet!=null) oRSet.close();
			if (oStmt!=null) oStmt.close();
		}

		if (DebugFile.trace)
		{
			DebugFile.decIdent();
			DebugFile.writeln("End JDCDAO.existsRegister() : " + String.valueOf(bExists));
		}

		return bExists;
	} // existsRegister

}