package org.judal.jdbc.metadata;

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

import java.sql.DatabaseMetaData;
import java.sql.Types;

import org.judal.jdbc.RDBMS;
import org.judal.metadata.ColumnDef;

/**
 * <p>Object representing metadata for a database table column.</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public final class SQLColumn extends ColumnDef {

	private static final long serialVersionUID = 10000l;

	public SQLColumn() {}

	//-----------------------------------------------------------

	/**
	 * <p>Constructor for non incremental autonumeric column</p>
	 * @param sTable String Table Name
	 * @param sColName String Column Name
	 * @param iColType short Column Type from java.sql.Types
	 * @param sColType String Column Type Name : ARRAY, BIGINT, BINARY, BIT, BLOB, BOOLEAN, CHAR, CLOB, DATE, DECIMAL, DOUBLE, FLOAT, INTEGER, JAVA_OBJECT, LONGVARBINARY, LONGVARCHAR, NCHAR, NVARCHAR, NULL, NUMERIC, REAL, SMALLINT, SQLXML, STRUCT, TIME, TIMESTAMP, TINYINT, VARBINARY, VARCHAR 
	 * @param iPrecision int
	 * @param iDecDigits int
	 * @param iNullable int DatabaseMetaData.columnNullable or DatabaseMetaData.columnNoNulls
	 * @param iColPos int Column Position 1&hellip;n
	 */
	public SQLColumn(String sTable, String sColName,
			short iColType, String sColType,
			int iPrecision, int iDecDigits,
			int iNullable, int iColPos) {
		super(sTable, sColName,iColType, iPrecision, iDecDigits, iNullable==DatabaseMetaData.columnNullable, null, null, null, false, iColPos);
		setJDBCType(sColType);
		setAutoIncrement(false);
	}

	//-----------------------------------------------------------

	/**
	 * <p>Constructor for incremental autonumeric column</p>
	 * @param sTable String Table Name
	 * @param sColName String Column Name
	 * @param iColType short Column Type from java.sql.Types
	 * @param sColType String Column Type Name : ARRAY, BIGINT, BINARY, BIT, BLOB, BOOLEAN, CHAR, CLOB, DATE, DECIMAL, DOUBLE, FLOAT, INTEGER, JAVA_OBJECT, LONGVARBINARY, LONGVARCHAR, NCHAR, NVARCHAR, NULL, NUMERIC, REAL, SMALLINT, SQLXML, STRUCT, TIME, TIMESTAMP, TINYINT, VARBINARY, VARCHAR 
	 * @param iPrecision int
	 * @param iDecDigits int
	 * @param iNullable int DatabaseMetaData.columnNullable or DatabaseMetaData.columnNoNulls
	 * @param bIsAutoInc boolean <b>true</b> if column is incremental autonumeric 
	 * @param iColPos int Column Position 1&hellip;n
	 */
	public SQLColumn(String sTable, String sColName,
			short iColType, String sColType,
			int iPrecision, int iDecDigits,
			int iNullable, boolean bIsAutoInc, int iColPos) {
		super(sTable, sColName,iColType, iPrecision, iDecDigits, iNullable==DatabaseMetaData.columnNullable, null, null, null, false, iColPos);
		setJDBCType(sColType);
		setAutoIncrement(bIsAutoInc);
	}

	/**
	 * <p>Constructor for nullable column with default precision</p>
	 * The precision is set by calling ColumnDef.getDefaultPrecision(iColType)
	 * @param sColName String Column Name
	 * @param iColPos int Column Position 1&hellip;n
	 * @param iColType short Column Type from java.sql.Types
	 */
	//-----------------------------------------------------------

	public SQLColumn(String sColName, int iColPos, short iColType) {
		super(null, sColName,iColType, ColumnDef.getDefaultPrecision(iColType), 0, true, null, null, null, false, iColPos);
		setJDBCType(SQLColumn.typeName(iColType));
		setAutoIncrement(false);
	}

	//-----------------------------------------------------------

	/**
	 * <p>Clone an existing column</p>
	 * @param source SQLColumn
	 */
	public SQLColumn(SQLColumn source) {
		super(source);
		setJDBCType(source.getJDBCType());
		setAutoIncrement(source.getAutoIncrement());
	}

	//-----------------------------------------------------------

	/**
	 *
	 * @return Column SQL Type
	 * @see java.sql.Types
	 */
	public short getSqlType() { return getType().shortValue(); }

	/**
	 * Set SQL type for this column
	 * @param iType short
	 */
	public void setSqlType(short iType) {
		setType(iType);
		setJDBCType(ColumnDef.typeName(getSqlType()));
	}

	/**
	 * @param iType int from java.sql.Types
	 * @see java.sql.Types
	 */
	public void setSqlType(int iType) {
		setType(iType);
		setJDBCType(ColumnDef.typeName(getSqlType()));
	}

	/**
	 * @return String SQL Type Name
	 */
	public String getSqlTypeName() { return getJDBCType(); }

	//-----------------------------------------------------------

	/**
	 * Get SQL script definition for this column.
	 * @param eRDBMS Target database management system
	 * @return String like "column_name VARCHAR2(30) NOT NULL DEFAULT '0'"
	 */
	public String sqlScriptDef(RDBMS eRDBMS) throws UnsupportedOperationException {
		String sTypedef = getName() + " ";
		switch (getSqlType()) {
		case Types.TIMESTAMP:
			if (null==Timestamp[eRDBMS.intValue()])
				throw new UnsupportedOperationException("Type "+typeName(getSqlType())+" is undefined for "+eRDBMS);
			sTypedef += Timestamp[eRDBMS.intValue()];
			break;
		case Types.DECIMAL:
		case Types.NUMERIC:
			if (null==Numeric[eRDBMS.intValue()])
				throw new UnsupportedOperationException("Type "+typeName(getSqlType())+" is undefined for "+eRDBMS);
			sTypedef += Numeric[eRDBMS.intValue()] + "("+String.valueOf(getLength())+","+String.valueOf(getScale())+")";
			break;
		case Types.CHAR:
			sTypedef += "CHAR("+String.valueOf(getLength())+")";
			break;
		case Types.NCHAR:
			sTypedef += "NCHAR("+String.valueOf(getLength())+")";
			break;
		case Types.VARCHAR:
			if (null==VarChar[eRDBMS.intValue()])
				throw new UnsupportedOperationException("Type "+typeName(getSqlType())+" is undefined for "+eRDBMS);
			sTypedef += VarChar[eRDBMS.intValue()]+"("+String.valueOf(getLength())+")";
			break;
		case Types.NVARCHAR:
			if (null==NVarChar[eRDBMS.intValue()])
				throw new UnsupportedOperationException("Type "+typeName(getSqlType())+" is undefined for "+eRDBMS);
			sTypedef += NVarChar[eRDBMS.intValue()]+"("+String.valueOf(getLength())+")";
			break;
		case Types.LONGVARCHAR:
			if (null==LongVarChar[eRDBMS.intValue()])
				throw new UnsupportedOperationException("Type "+typeName(getSqlType())+" is undefined for "+eRDBMS);
			sTypedef += LongVarChar[eRDBMS.intValue()];
			break;
		case Types.LONGVARBINARY:
			if (null==LongVarBinary[eRDBMS.intValue()])
				throw new UnsupportedOperationException("Type "+typeName(getSqlType())+" is undefined for "+eRDBMS);
			sTypedef += LongVarBinary[eRDBMS.intValue()];
			break;
		case Types.BLOB:
			if (null==Blob[eRDBMS.intValue()])
				throw new UnsupportedOperationException("Type "+typeName(getSqlType())+" is undefined for "+eRDBMS);
			sTypedef += Blob[eRDBMS.intValue()];
			break;
		case Types.CLOB:
			if (null==Clob[eRDBMS.intValue()])
				throw new UnsupportedOperationException("Type "+typeName(getSqlType())+" is undefined for "+eRDBMS);
			sTypedef += Clob[eRDBMS.intValue()];
			break;
		default:
			sTypedef += getSqlTypeName()==null ? ColumnDef.typeName(getSqlType()) : getSqlTypeName();
		}
		
		if (RDBMS.HSQLDB.equals(eRDBMS))
			sTypedef += defaultClause(eRDBMS) + (getAllowsNull() ? " NULL" : " NOT NULL");
		else
			sTypedef += (getAllowsNull() ? " NULL" : " NOT NULL") + defaultClause(eRDBMS);
		
		return sTypedef;
	} // sqlScriptDef

	protected String defaultClause(RDBMS eRDBMS) {
		String sClause;
		if (getDefaultValue()==null) {
			sClause = "";
		} else {
			String sDefVal = getDefaultValue().toString();
			sClause = " DEFAULT ";
			switch (getSqlType()) {
			case Types.BIGINT:
			case Types.INTEGER:
				if (sDefVal.equalsIgnoreCase("SERIAL"))
					sClause += Serial[eRDBMS.intValue()];
				else
					sClause += getDefaultValue();
				break;
			case Types.TIMESTAMP:
				if (sDefVal.equalsIgnoreCase("NOW") || sDefVal.equalsIgnoreCase("CURRENT_TIMESTAMP"))
					sClause += CurrentTimeStamp[eRDBMS.intValue()];
				else
					sClause += getDefaultValue();
				break;
			case Types.CHAR:
			case Types.VARCHAR:
			case Types.NCHAR:
			case Types.NVARCHAR:
				sClause += "'" + getDefaultValue() + "'";
				break;
			default:
				sClause += getDefaultValue();
				break;
			}	
		} // fi	
		return sClause;
		
	}

	//-----------------------------------------------------------

	// Type names for different databases

	// 1 = MYSQL
	// 2 = POSTGRESQL
	// 3 = MSSQL
	// 4 = SYBASE
	// 5 = ORACLE
	// 6 = DB2
	// 7 = INFORMIX
	// 8 = DERBY
	// 9 = XBASE
	// 10= ACCESS
	// 11= SQLLITE
	// 12= HSQLDB
	
	public static final String CurrentTimeStamp[] = { null, "CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP", "GETDATE()", "GETDATE()", "SYSDATE", null, null, null, null, null, null, "CURRENT_TIMESTAMP" };
	public static final String Timestamp[] = { null, "TIMESTAMP", "TIMESTAMP", "DATETIME", "DATETIME", "DATE", null, null, null, null, null, null, "TIMESTAMP" };
	public static final String LongVarChar[] = { null, "MEDIUMTEXT", "TEXT", "NTEXT", null, "LONG", null, null, null, null, null, null, "LONGVARCHAR" };
	public static final String LongVarBinary[] = { null, "MEDIUMBLOB", "BYTEA", "IMAGE", null, "LONG RAW", null, null, null, null, null, null, "LONGVARBINARY" };
	public static final String Serial[] = { null, "AUTO_INCREMENT", "", "IDENTITY", null, "", null, null, null, null, null, null, "LONGVARBINARY" };
	public static final String VarChar[] = { null, "VARCHAR", "VARCHAR", "NVARCHAR", null, "VARCHAR2", null, null, null, null, null, null, "VARCHAR" };
	public static final String NVarChar[] = { null, "VARCHAR", "VARCHAR", "NVARCHAR", null, "VARCHAR2", null, null, null, null, null, null, "VARCHAR" };
	public static final String Blob[] = { null, "MEDIUMBLOB", "BYTEA", "IMAGE", null, "BLOB", null, null, null, null, null, null, "BLOB" };
	public static final String Clob[] = { null, "MEDIUMTEXT", "TEXT", "NTEXT", null, "CLOB", null, null, null, null, null, null, "CLOB" };
	public static final String Numeric[] = { null, "DECIMAL", "DECIMAL", "DECIMAL", null, "NUMBER", null, null, null, null, null, null, "NUMERIC" };

} // JDCColumn
