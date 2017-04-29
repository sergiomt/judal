package org.judal.jdbc.jdc;


import java.math.BigDecimal;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import java.util.ListIterator;

import javax.jdo.metadata.ColumnMetadata;

import org.judal.jdbc.JDBCRelationalTable;
import org.judal.jdbc.RDBMS;
import org.judal.jdbc.metadata.SQLTableDef;
import org.judal.metadata.ColumnDef;
import org.judal.metadata.ColumnList;
import org.judal.storage.ImportLoader;
import org.judal.storage.table.Table;

import javax.jdo.JDOException;

import java.util.HashMap;
import java.util.Iterator;
import java.sql.Types;
import java.sql.SQLException;
import java.sql.PreparedStatement;

import com.knowgate.debug.DebugFile;

/**
 * Generic text to table loader
 * @author Sergio Montoro Ten
 * @version 7.0
 */
public class JDCTableLoader extends SQLTableDef implements ImportLoader {

	private JDCConnection jConn;
	private Object[] aValues;
	private short[] aColTypes;
	private SimpleDateFormat oDtFmt = new SimpleDateFormat("yyyy-MM-dd");
	private SimpleDateFormat oTsFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private HashMap<String,Integer> oInsrColPos;
	private HashMap<String,Integer> oUpdtColPos;
	private PreparedStatement oInsr;
	private PreparedStatement oUpdt;

	// ---------------------------------------------------------------------------

	public JDCTableLoader(RDBMS eDBMS, String sTableName) {
		super(eDBMS, sTableName);
		jConn = null;
	}

	// ---------------------------------------------------------------------------

	public String[] columnNames() throws IllegalStateException {
		return getColumnsStr().split(",");
	}

	// ---------------------------------------------------------------------------

	public short[] columnTypes() throws IllegalStateException {
		if (null==aColTypes)
			throw new IllegalStateException("TableLoader: must call prepare() before columnTypes()");
		return aColTypes;
	}

	// ---------------------------------------------------------------------------

	public void prepare(Table oTbl, ColumnList oColList)
			throws JDOException {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin TableLoader.prepare([Connection], [ColumnList])");
			DebugFile.incIdent();
		}
		JDCConnection oConn = ((JDBCRelationalTable) oTbl).getConnection();

		try {
			readCols(oConn, oConn.getMetaData());
			precomputeSqlStatements(getRDBMSId(oConn));
		} catch (SQLException  sqle) {
			throw new JDOException("SQLException "+sqle.getMessage(), sqle);
		}

		if (getNumberOfColumns()==0) {
			DebugFile.decIdent();
			throw new JDOException("No columns found for table "+getName());
		}
		aColTypes = new short[getColumns().length];
		int t=-1;
		for (ColumnDef oColDef : getColumns())
			aColTypes[++t] = oColDef.getType().shortValue();
		aValues = new Object[getNumberOfColumns()+1];
		String sSQL;
		String sCol;
		oInsrColPos = new HashMap<String,Integer>(1+oColList.size()*2);
		oUpdtColPos = new HashMap<String,Integer>(1+oColList.size()*2);

		// ************************************
		// Compose and prepare insert statement
		sSQL = "INSERT INTO "+getName()+" ("+oColList.toString(",")+") VALUES (";
		for (int c=0; c<oColList.size(); c++) {
			oInsrColPos.put(oColList.getColumnName(c).toLowerCase(),new Integer(c+1));
			sSQL += (c == 0 ? "" : ",") + "?";
		}
		sSQL += ")";

		if (DebugFile.trace) {
			DebugFile.writeln("Connection.prepareStatement("+sSQL+")");
		}

		try {
			oInsr = oConn.prepareStatement(sSQL);
		} catch (SQLException  sqle) {
			throw new JDOException("SQLException "+sqle.getMessage(), sqle);
		}

		// ************************************
		// Compose and prepare update statement

		if (getPrimaryKeyMetadata().getNumberOfColumns()==0) {

			oUpdt = null;

		} else {
			sSQL = oColList.toString("=?,");
			for (ColumnMetadata cmd : getPrimaryKeyMetadata().getColumns())
				sSQL = sSQL.replace(cmd.getName() + "=?,", "");
			String[] aUpdtCols = sSQL.split(",");
			for (int c=0; c<aUpdtCols.length; c++)
				oUpdtColPos.put(aUpdtCols[c].split("=")[0].toLowerCase(),new Integer(c+1));
			sSQL = "UPDATE "+getName()+" SET "+sSQL+ " WHERE ";
			int iPK=1;
			for (ColumnMetadata cmd : getPrimaryKeyMetadata().getColumns()) {
				sCol = cmd.getName();
				oUpdtColPos.put(sCol.toLowerCase(),new Integer(aUpdtCols.length+iPK));
				if (iPK>1) sSQL += " AND ";
				sSQL += sCol+"=?";
				iPK++;
			}
			if (DebugFile.trace) DebugFile.writeln("Connection.prepareStatement("+sSQL+")");

			try {
				oUpdt = oConn.prepareStatement(sSQL);
			} catch (SQLException  sqle) {
				try { oInsr.close(); } catch (Exception ignore) { }
				throw new JDOException("SQLException "+sqle.getMessage(), sqle);
			}
		}

		jConn = new JDCConnection(oConn, null);

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End TableLoader.prepare()");
		}
	} // prepare

	// ---------------------------------------------------------------------------

	public Object get(int iColumnIndex) throws ArrayIndexOutOfBoundsException {
		return aValues[iColumnIndex];
	}

	// ---------------------------------------------------------------------------

	public Object get(String sColumnName) throws ArrayIndexOutOfBoundsException {
		return aValues[getColumnIndex(sColumnName)-1];
	}

	// ---------------------------------------------------------------------------

	public void put(int iColumnIndex, String sValue)
			throws NumberFormatException,ArrayIndexOutOfBoundsException,ParseException {
		switch (aColTypes[iColumnIndex]) {
		case Types.TINYINT:
			aValues[iColumnIndex]=new Byte(sValue);
			break;
		case Types.SMALLINT:
			aValues[iColumnIndex]=new Short(sValue);
			break;
		case Types.INTEGER:
			aValues[iColumnIndex]=new Integer(sValue);
			break;
		case Types.BIGINT:
			aValues[iColumnIndex]=new Long(sValue);
			break;
		case Types.FLOAT:
			aValues[iColumnIndex]=new Float(sValue);
			break;
		case Types.DOUBLE:
		case Types.REAL:
			aValues[iColumnIndex]=new Double(sValue);
			break;
		case Types.DECIMAL:
		case Types.NUMERIC:
			aValues[iColumnIndex]=new BigDecimal(sValue);
			break;
		case Types.DATE:
			if (sValue.length()==10)
				aValues[iColumnIndex]=oDtFmt.parse(sValue);
			else    	
				aValues[iColumnIndex]=oTsFmt.parse(sValue);
			break;        
		case Types.TIMESTAMP:    	
			aValues[iColumnIndex]=oTsFmt.parse(sValue);
			break;        
		default:
			aValues[iColumnIndex]=sValue;      	
		}
	}

	// ---------------------------------------------------------------------------

	public void put(int iColumnIndex, Object oValue) throws ArrayIndexOutOfBoundsException {
		aValues[iColumnIndex]=oValue;
	}

	// ---------------------------------------------------------------------------

	public void put(String sColumnName, Object oValue) throws ArrayIndexOutOfBoundsException {
		aValues[getColumnIndex(sColumnName)-1]=oValue;
	}

	// ---------------------------------------------------------------------------

	public void setAllColumnsToNull() {
		for (int c=getNumberOfColumns()-1; c>=0; c--) aValues[c]=null;
	}

	// ---------------------------------------------------------------------------

	public void close() throws JDOException {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin TableLoader.close()");
			DebugFile.incIdent();
		}

		try { if (oUpdt!=null) oUpdt.close(); } catch (Exception ignore) {}
		try { if (oInsr!=null) oUpdt.close(); } catch (Exception ignore) {}

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End TableLoader.close()");
		}
	} // close

	// ---------------------------------------------------------------------------

	private static boolean test(int iInputValue, int iBitMask) {
		return (iInputValue&iBitMask)!=0;
	} // test

	// ---------------------------------------------------------------------------

	public void store(Table oTbl, String sWorkArea, int iFlags)
			throws JDOException,IllegalArgumentException,NullPointerException {

		int iAffected = 0;

		if (oInsr==null)
			throw new JDOException("Invalid command sequece. Must call ContactLoader.prepare() before TableLoader.store()");

		if (!test(iFlags,MODE_APPEND) && !test(iFlags,MODE_UPDATE))
			throw new IllegalArgumentException("TableLoader.store() Flags bitmask must contain either MODE_APPEND, MODE_UPDATE or both");

		if (test(iFlags,MODE_UPDATE) && oUpdt==null)
			throw new IllegalArgumentException("TableLoader.store() Flags bitmask cannot contain MODE_UPDATE because table has not primary key");

		if (DebugFile.trace) {
			DebugFile.writeln("Begin TableLoader.store([Connection], "+sWorkArea+","+String.valueOf(iFlags)+")");
			DebugFile.incIdent();
		}

		if (test(iFlags,MODE_UPDATE)) {
			if (DebugFile.trace) DebugFile.writeln("trying to update record...");
			for (ColumnDef oCol : getColumns()) {
				Integer iUpdtPos = (Integer) oUpdtColPos.get(oCol.getName());
				if (null!=iUpdtPos) {
					try {
						jConn.bindParameter(oUpdt, iUpdtPos.intValue(), get(oCol.getName()), oCol.getType().shortValue());
					} catch (SQLException sqle) {
						throw new JDOException("SQLException "+sqle.getMessage(), sqle);
					}
				} // fi
			} // wend
			try {
				iAffected = oUpdt.executeUpdate();
			} catch (SQLException sqle) {
				throw new JDOException("SQLException "+sqle.getMessage(), sqle);
			}
		} // fi (MODE_UPDATE)

		if (0==iAffected && test(iFlags,MODE_APPEND)) {
			if (DebugFile.trace) DebugFile.writeln("trying to insert record...");
			for (ColumnDef oCol : getColumns()) {
				Integer iInsrPos = (Integer) oInsrColPos.get(oCol.getName());
				if (null!=iInsrPos) {
					try {
						jConn.bindParameter(oInsr, iInsrPos.intValue(), get(oCol.getName()), oCol.getType().shortValue());
					} catch (SQLException sqle) {
						throw new JDOException("SQLException "+sqle.getMessage(), sqle);
					}
				} // fi
			} // wend
			try {
				iAffected = oInsr.executeUpdate();
			} catch (SQLException sqle) {
				throw new JDOException("SQLException "+sqle.getMessage(), sqle);
			}
		} // fi (MODE_APPEND)
		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End TableLoader.store()");
		}
	} // store

	// ---------------------------------------------------------------------------
}
