package org.judal.metadata;

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

import java.sql.Types;
import java.sql.Timestamp;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.regex.Pattern;

import javax.jdo.metadata.ColumnMetadata;
import javax.jdo.metadata.ExtensionMetadata;

import java.util.regex.Matcher;

import java.io.Serializable;

/**
 * Column Definition
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class ColumnDef extends ExtendableDef implements Serializable, ColumnMetadata {

	private static final long serialVersionUID = 10000l;

	private int iPosition;
	private int nLength;
	private int iDecimalDigits;
	private int iType;
	private String sSQLTypeName;
	private String sName;
	private String sAlias;
	private String sTableName;
	private boolean bNullable;
	private NonUniqueIndexDef.Type eIndex;
	private boolean bPk;
	private boolean bAutoIncrement;
	private String sInsertValue;
	private String sForeignKey;
	private String sForeignKeyField;
	private String sFamily;
	private Object oDefault;
	private SimpleDateFormat oDtFmt;

	private Pattern oPattern;
	private static HashMap<String,Pattern> oRegExps = new HashMap<String,Pattern>();

	public ColumnDef() { }

	/**
	 * Constructor
	 * @param sColName String column name
	 * @param iColType int Column type as in java.sql.Types
	 * @param iColPos int Position [1..n]
	 */
	public ColumnDef(String sColName, int iColType, int iColPos) {
		iPosition = iColPos;
		sName = sColName;
		sTableName = null;
		iType = iColType;
		nLength = -1;
		iDecimalDigits = -1;
		bNullable = true;
		eIndex = null;
		bPk = false;
		oDefault = null;
		sForeignKey = null;
		sFamily = null;
		oPattern = null;
		oDtFmt = null;
	}

	//-----------------------------------------------------------

	/**
	 * Constructor
	 * @param sTable String table name
	 * @param sColName String column name
	 * @param iColType int Column type as in java.sql.Types
	 * @param nColLen int Column maximum length
	 * @param iDecDigits int Decimal value maximum digits
	 * @param bIsNullable boolean true if the column allows null values
	 * @param eIndexType Index.Type
	 * @param sForeignKeyTableName String name of table containing the foreign key
	 * @param oDefaultValue Object Default value
	 * @param bIsPrimaryKey boolean true if the column is part of the primary key
	 * @param iColPos int Position [1..n]
	 */
	public ColumnDef(String sTable, String sColName, int iColType,
			int nColLen, int iDecDigits,
			boolean bIsNullable, NonUniqueIndexDef.Type eIndexType,
			String sForeignKeyTableName, Object oDefaultValue,
			boolean bIsPrimaryKey, int iColPos ) {
		sTableName = sTable;
		iPosition = iColPos;
		sName = sColName;
		iType = iColType;
		nLength = nColLen;
		iDecimalDigits = iDecDigits;
		bNullable = bIsNullable;
		eIndex = eIndexType;
		bPk = bIsPrimaryKey;
		oDefault = oDefaultValue;
		sForeignKey = sForeignKeyTableName;
		sFamily = null;
		oPattern = null;
		oDtFmt = null;
	}

	/**
	 * Constructor
	 * @param sTable String Table Name
	 * @param sColFamily String Column Family Name
	 * @param sColName String Column Name
	 * @param iColType java.sql.Type Column Type
	 * @param nColLen int Maximum Data Length
	 * @param iDecDigits int Decimal Digits
	 * @param bIsNullable boolean Whether or not the column admits NULL values
	 * @param eIndexType org.judal.storage.Index 
	 * @param sForeignKeyTableName String Foreign Key Table Name
	 * @param oDefaultValue Object Default Value
	 * @param bIsPrimaryKey boolean Is Primary Key
	 * @param iColPos int Column Position [1..n]
	 */
	public ColumnDef(String sTable, String sColFamily, String sColName, int iColType,
			int nColLen, int iDecDigits, boolean bIsNullable, NonUniqueIndexDef.Type eIndexType,
			String sForeignKeyTableName, Object oDefaultValue,
			boolean bIsPrimaryKey, int iColPos) {
		sTableName = sTable;
		iPosition = iColPos;
		sName = sColName;
		iType = iColType;
		nLength = nColLen;
		iDecimalDigits = iDecDigits;
		bNullable = bIsNullable;
		eIndex = eIndexType;
		bPk = bIsPrimaryKey;
		oDefault = oDefaultValue;
		sForeignKey = sForeignKeyTableName;
		sFamily = sColFamily;
		oPattern = null;
		oDtFmt = null;
	}

	public ColumnDef(int iColPos, String sColName, int iColType, int nColLen,
			boolean bIsNullable, NonUniqueIndexDef.Type eIndexType, String sCheckRegExp,
			String sForeignKeyTableName, Object oDefaultValue,
			boolean bIsPrimaryKey)  {
		iPosition = iColPos;
		sName = sColName;
		iType = iColType;
		nLength = nColLen;
		bNullable = bIsNullable;
		eIndex = eIndexType;
		bPk = bIsPrimaryKey;
		oDefault = oDefaultValue;
		sForeignKey = sForeignKeyTableName;
		sFamily = null;
		oDtFmt = null;
		if (null==sCheckRegExp) {
			oPattern = null;
		} else if (oRegExps.containsKey(sCheckRegExp)) {
			oPattern = oRegExps.get(sCheckRegExp);
		} else {
			oPattern = Pattern.compile(sCheckRegExp, Pattern.CASE_INSENSITIVE);
			oRegExps.put(sCheckRegExp, oPattern);
		}
	}

	/**
	 * Constructor
	 * @param iColPos int Column Position [1..n]
	 * @param sColName String Column Name
	 * @param iColType java.sql.Type Column Type
	 * @param nColLen int Maximum Data Length
	 * @param bIsNullable boolean Whether or not the column admits NULL values
	 * @param eIndexType org.judal.storage.Index 
	 * @param sCheckRegExp String Check Regular Expression
	 * @param sForeignKeyTableName String Foreign Key Table Name
	 * @param oDefaultValue Object Default Value
	 * @param bIsPrimaryKey boolean Is Primary Key
	 */
	public ColumnDef(int iColPos, String sColFamily, String sColName, int iColType, int nColLen,
			boolean bIsNullable, NonUniqueIndexDef.Type eIndexType, String sCheckRegExp,
			String sForeignKeyTableName, Object oDefaultValue,
			boolean bIsPrimaryKey) {
		iPosition = iColPos;
		sName = sColName;
		iType = iColType;
		nLength = nColLen;
		bNullable = bIsNullable;
		eIndex = eIndexType;
		bPk = bIsPrimaryKey;
		oDefault = oDefaultValue;
		sForeignKey = sForeignKeyTableName;
		sFamily = sColFamily;
		oDtFmt = null;
		if (null==sCheckRegExp) {
			oPattern = null;
		} else if (oRegExps.containsKey(sCheckRegExp)) {
			oPattern = oRegExps.get(sCheckRegExp);
		} else {
			oPattern = Pattern.compile(sCheckRegExp, Pattern.CASE_INSENSITIVE);
			oRegExps.put(sCheckRegExp, oPattern);
		}
	}

	/**
	 * Constructor
	 */
	public ColumnDef(ColumnMetadata oCol) {
		iPosition = oCol.getPosition();
		nLength = oCol.getLength();
		iDecimalDigits = oCol.getScale();
		iType = ColumnDef.getSQLType(oCol.getJDBCType());
		sSQLTypeName = oCol.getSQLType();
		sName = oCol.getName();
		bNullable = oCol.getAllowsNull();
		eIndex = null;
		bPk = false;
		bAutoIncrement = false;
		sForeignKey = oCol.getTarget();
		sFamily = null;
		oDefault = oCol.getDefaultValue();
		oPattern = null;
		oDtFmt = null;	
		if (oCol.getExtensions()!=null)
			for (ExtensionMetadata ext : oCol.getExtensions())
				newExtensionMetadata(ext.getKey(), ext.getValue(), ext.getVendorName());
	}
	
	protected ColumnDef(ColumnDef oCol) {
		iPosition = oCol.iPosition;
		nLength = oCol.nLength;
		iDecimalDigits = oCol.iDecimalDigits;
		iType = oCol.iType;
		sSQLTypeName = oCol.sSQLTypeName;
		sName = oCol.sName;
		sTableName = oCol.sTableName;
		bNullable = oCol.bNullable;
		eIndex = oCol.eIndex;
		bPk = oCol.bPk;
		bAutoIncrement = oCol.bAutoIncrement;
		sForeignKey = oCol.sForeignKey;
		sFamily = oCol.sFamily;
		oDefault = oCol.oDefault;
		oPattern = oCol.oPattern;
		oDtFmt = oCol.oDtFmt;	
		if (oCol.getExtensions()!=null)
			for (ExtensionMetadata ext : oCol.getExtensions())
				newExtensionMetadata(ext.getKey(), ext.getValue(), ext.getVendorName());
	}

	/**
	 * @return String Column Family Name or <b>null</b> if this column has no family defined.
	 */
	public String getFamily() {
		return sFamily;

	}

	/**
	 * Set column family name
	 * @param sFamilyName String
	 */
	public void setFamily(String sFamilyName) {
		sFamily = sFamilyName;

	}

	/**
	 * @return Column Name
	 */
	@Override
	public String getName() {
		return sName;
	}  

	/**
	 * Set column name
	 * @param sColName String
	 */
	@Override
	public ColumnMetadata setName(String sColName)  {
		sName=sColName;
		return this;
	}

	/**
	 * @return Integer Column Position (starting at column 1)
	 */
	@Override
	public Integer getPosition() {
		return iPosition;
	}

	/**
	 * Set column position (starting at column 1)
	 * @param iPos int
	 */
	@Override
	public ColumnMetadata setPosition(int iPos) {
		iPosition=iPos;
		return this;
	}

	/**
	 * @return Integer Decimal Digits
	 */

	@Override
	public Integer getScale() {
		return iDecimalDigits;
	}

	/**
	 * Set decimal digits
	 * @return ColumnMetadata <b>this</b> object
	 */
	@Override
	public ColumnMetadata setScale(int digits) {
		iDecimalDigits = digits;
		return this;
	}

	/**
	 * @return String Name of table containing this column
	 */
	public String getTableName() {
		return sTableName;
	}

	/**
	 * @return Integer
	 * @see java.sql.Types
	 */
	public Integer getType() {
		return iType;
	}  

	/**
	 * @param int
	 * @return ColumnMetadata <b>this</b> object
	 * @see java.sql.Types
	 */
	public ColumnMetadata setType(int iSqlType) {
		iType=iSqlType;
		return this;
	}  

	/**
	 * @param short
	 * @return ColumnMetadata <b>this</b> object
	 * @see java.sql.Types
	 */
	public ColumnMetadata setType(short iSqlType) {
		iType=(int) iSqlType;
		return this;
	}  

	/**
	 * Same as getSQLType()
	 * @return String
	 */
	@Override
	public String getJDBCType() {
		return sSQLTypeName;
	}  

	/**
	 * Same as setSQLType()
	 * @param String The String name representation of getType()
	 * @return ColumnMetadata <b>this</b> object
	 */
	@Override
	public ColumnMetadata setJDBCType(String sTypeName) {
		sSQLTypeName = sTypeName;
		return this;
	}  

	/**
	 * Same as getJDBCType()
	 * @return String
	 */
	@Override
	public String getSQLType() {
		return sSQLTypeName;
	}  

	/**
	 * Same as setJDBCType()
	 * @param String The String name representation of getType()
	 * @return ColumnMetadata <b>this</b> object
	 */
	@Override
	public ColumnMetadata setSQLType(String sTypeName) {
		sSQLTypeName = sTypeName;
		return this;
	}  
	
	/**
	 * @return Integer Column data length
	 */
	@Override
	public Integer getLength() {
		return nLength;
	}  

	/**
	 *
	 * Set column data maximum length
	 * @param p int
	 * @return ColumnMetadata <b>this</b> object
	 */
	@Override
	public ColumnMetadata setLength(int p) {
		nLength = p;
		return this;
	}
	
	/**
	 * @return String Column default value
	 */
	@Override
	public String getDefaultValue() {
		if (null==oDefault)
			return null;
		else if (oDefault instanceof String)
			return (String) oDefault;
		else
			return oDefault.toString();
	}  

	/**
	 * @param String Column default value
	 */
	@Override
	public ColumnMetadata setDefaultValue(String oDefValue) {
		oDefault = oDefValue;
		return this;
	}  

	@Override
	public String getInsertValue() {
		return sInsertValue;
	}

	@Override
	public ColumnMetadata setInsertValue(String sValue) {
		sInsertValue = sValue;
		return this;
	}
	
	/**
	 * @return String If getTarget() is not <b>null</b> return "foreign key " concatenated with the name of the table referenced. Else if a pattern constraint is set return the string representation of the pattern.
	 */
	public String getConstraint() {
		if (getTarget()==null)
			if (oPattern==null)
				return null;
			else
				return oPattern.toString();
		else
			return "foreign key "+getTarget();
	}  

	/**
	 * @return boolean <b>true</b> if getIndexType() is not <b>null</b>, <b>false</b> otherwise.
	 */
	public boolean isIndexed() {
		return eIndex!=null;
	}  

	/**
	 * @return NonUniqueIndexDef.Type or <b>null</b> if this column is not indexed
	 */
	public NonUniqueIndexDef.Type getIndexType() {
		return eIndex;
	}  

	/**
	 * @param indexType NonUniqueIndexDef.Type
	 */
	public void setIndexType(NonUniqueIndexDef.Type indexType) {
		eIndex = indexType;
	}  

	/**
	 * @return boolean <b>true</b> if this column is part of the primary key, <b>false</b> otherwise.
	 */
	public boolean isPrimaryKey() {
		return bPk;
	}  
	
	/**
	 * @return Boolean Allows NULLs?
	 */
	@Override
	public Boolean getAllowsNull() {
		return bNullable;
	}  

	/**
	 * @param boolean
	 * @return ColumnMetadata <b>this</b> object
	 */
	@Override
	public ColumnMetadata setAllowsNull(boolean b) {
		bNullable = b;
		return this;
	}  

	/**
	 * @return Boolean Is autoincrement value column?
	 */
	public Boolean getAutoIncrement() {
		return bAutoIncrement;
	}  

	/**
	 * @param boolean
	 * @return ColumnMetadata <b>this</b> object
	 */
	public ColumnMetadata setAutoIncrement(boolean a) {
		bAutoIncrement = a;
		return this;
	}  
	
	/**
	 * @return boolean <b>true</b> if getType() is BLOB, BINARY, VARBINARY or LONGVARBINARY
	 * @see java.sql.Types
	 */
	public boolean isOfBinaryType() {
		return getType()==java.sql.Types.BLOB ||
				getType()==java.sql.Types.BINARY ||
				getType()==java.sql.Types.VARBINARY ||
				getType()==java.sql.Types.LONGVARBINARY;
	}
	
	/**
	 * @return String Name of table referenced by the foreign key
	 */
	@Override
	public String getTarget() {
		return sForeignKey;
	}  

	/**
	 * @param String Name of table referenced by the foreign key
	 * @return ColumnMetadata <b>this</b> object
	 */
	@Override
	public ColumnMetadata setTarget(String sFk) {
		sForeignKey = sFk;
		return this;
	}  

	/**
	 * @return String Name of column referenced by the foreign key
	 */
	@Override
	public String getTargetField() {
		return sForeignKeyField;
	}  

	/**
	 * @param String Name of column referenced by the foreign key
	 * @return ColumnMetadata <b>this</b> object
	 */
	@Override
	public ColumnMetadata setTargetField(String sFk) {
		sForeignKeyField = sFk;
		return this;
	}  
	
	/**
	 * @param boolean Is this column part of the primary key?
	 */
	public ColumnMetadata setPrimaryKey(boolean bIsPk) {
		bPk = bIsPk;
		return this;
	}  

	/**
	 * Set format for string to date and date to string conversions
	 * @param sFmt String
	 * @throws IllegalArgumentException
	 */
	public void setDateFormat(String sFmt) throws IllegalArgumentException {
		oDtFmt = new SimpleDateFormat(sFmt);
	}

	/**
	 * @return SimpleDateFormat If none has been set then date format for "yyyy-MM-dd HH:mm:ss" is returned
	 */
	public SimpleDateFormat getDateFormat()  {
		if (oDtFmt==null) oDtFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return oDtFmt;
	}

	/**
	 * Try to convert an input String into the type of object that this column holds
	 * @param sIn String
	 * @return Object
	 * @throws NumberFormatException
	 * @throws ParseException
	 * @throws NullPointerException
	 */
	public Object convert(String sIn)
			throws NumberFormatException,ParseException,NullPointerException {
		if (sIn==null) return null;
		if (sIn.length()==0) return null;
		switch (getType()) {
		case Types.SMALLINT:
			return new Short(sIn);
		case Types.INTEGER:
			return new Integer(sIn);
		case Types.BIGINT:
			return new Long(sIn);
		case Types.FLOAT:
			return new Float(sIn);
		case Types.DOUBLE:
			return new Double(sIn);
		case Types.DECIMAL:
		case Types.NUMERIC:
			return new BigDecimal(sIn);
		case Types.DATE:
			return oDtFmt.parse(sIn);
		case Types.TIMESTAMP:
			return new Timestamp(oDtFmt.parse(sIn).getTime());
		default:
			return sIn;
		}
	} // convert

	public boolean check(Object sValue) {
		boolean bRetVal;
		if (null==sValue) {
			bRetVal = bNullable;
		} else {
			if (null==oPattern) {
				bRetVal = true;
			} else {
				Matcher oMatcher = oPattern.matcher(sValue.toString());
				boolean bMatches = oMatcher.matches();
				if (sValue instanceof String) {
					bRetVal = bMatches || (bNullable && ((String)sValue).length()==0);
				} else {
					switch (getType()) {
					case Types.SMALLINT:
						bRetVal = sValue instanceof Short;
						if (bRetVal) bRetVal = bMatches;
						break;
					case Types.INTEGER:
						bRetVal = sValue instanceof Integer;
						if (bRetVal) bRetVal = bMatches;
						break;
					case Types.BIGINT:
						bRetVal = sValue instanceof Long;
						if (bRetVal) bRetVal = bMatches;
						break;
					case Types.FLOAT:
						bRetVal = sValue instanceof Float;
						if (bRetVal) bRetVal = bMatches;
						break;
					case Types.DOUBLE:
						bRetVal = sValue instanceof Double;
						if (bRetVal) bRetVal = bMatches;
						break;
					case Types.DECIMAL:
					case Types.NUMERIC:
						bRetVal = sValue instanceof BigDecimal;
						if (bRetVal) bRetVal = bMatches;
						break;
					case Types.TIMESTAMP:
					case Types.DATE:
						bRetVal = sValue instanceof Date || sValue instanceof Timestamp || sValue instanceof Long;
						break;
					default:
						bRetVal = false;
					}
				}
			}
		}
		return bRetVal;
	} // check

	//-----------------------------------------------------------

	/**
	 * @return String like &lt;column name="<i>Column_Name</i>" jdbc-type="<i>TYPE_NAME</i>" length="<i>99</i>" scale="<i>9</i>" allows-null="<i>true</i>" target="Referenced_Table_Name" /&gt;
	 */
	public String toJdoXml() {
		StringBuilder builder = new StringBuilder();
		builder.append("      <column ");
		builder.append("name=\"");
		builder.append(getName());
		builder.append("\" jdbc-type=\"");
		builder.append(getJDBCType());
		builder.append("\" ");
		if (getType()==Types.CHAR || getType()==Types.NCHAR || getType()==Types.VARCHAR || getType()==Types.NVARCHAR ||
			getType()==Types.LONGVARCHAR || getType()==Types.LONGNVARCHAR ||
			getType()==Types.BINARY || getType()==Types.VARBINARY || getType()==Types.LONGVARBINARY) {
			builder.append("length=\"");
			builder.append(getLength());
			builder.append("\" ");
		}
		if (getType()==Types.DECIMAL || getType()==Types.NUMERIC)
			builder.append("scale=\""+String.valueOf(getScale())+"\"");
		builder.append("allows-null=\""+(getAllowsNull() ? "true" : "false")+"\" ");
		if (getTarget()!=null && getTarget().length()>0)
			builder.append("target=\""+getTarget()+"\"");
		builder.append(" />");
		return builder.toString();	
	}

	//-----------------------------------------------------------

	/**
	 * @return String like &lt;column name="<i>Column_Name</i>" primaryKey="<i>false</i>" required="<i>false</i>" type="<i>YPE_NAME</i>" size="<i>99,9</i>" autoIncrement="<i>false</i>" /&gt;
	 */
	public String toDdlXml() {
		StringBuilder builder = new StringBuilder();		
		builder.append("<column ");
		builder.append("name=\"");		
		builder.append(getName());
		builder.append("\" primaryKey=\"");
		builder.append(isPrimaryKey() ? "true" : "false");
		builder.append("\" required=\"");
		builder.append(getAllowsNull() ? "false" : "true");
		builder.append("\" type=\"");
		builder.append(getJDBCType());
		builder.append("\"");
		if (getType()==Types.CHAR || getType()==Types.NCHAR || getType()==Types.VARCHAR || getType()==Types.NVARCHAR ||
			getType()==Types.LONGVARCHAR || getType()==Types.LONGNVARCHAR ||
			getType()==Types.BINARY || getType()==Types.VARBINARY || getType()==Types.LONGVARBINARY) {
			builder.append(" size=\"");
			builder.append(getLength());
			builder.append("\" ");
		}
		if (getType()==Types.FLOAT || getType()==Types.DOUBLE ||getType()==Types.DECIMAL || getType()==Types.NUMERIC || getType()==Types.TIMESTAMP || getType()==Types.TIMESTAMP_WITH_TIMEZONE) {
			builder.append(" size=\"");
			builder.append(getLength());
			builder.append(",");
			builder.append(getScale());
			builder.append("\" ");			
		}
		builder.append(" autoIncrement=\""+(getAutoIncrement() ? "true" : "false")+"\" ");
		builder.append("></column>");
		return builder.toString();	
	}
	
	//-----------------------------------------------------------

	/**
	 * Get SQL type name from its integer identifier
	 * @param iSQLtype int
	 * @return String
	 */
	public static String typeName(int iSQLtype) {
		switch (iSQLtype) {
		case Types.BIGINT:
			return "BIGINT";
		case Types.BINARY:
			return "BINARY";
		case Types.BIT:
			return "BIT";
		case Types.BLOB:
			return "BLOB";
		case Types.BOOLEAN:
			return "BOOLEAN";
		case Types.CHAR:
			return "CHAR";
		case Types.CLOB:
			return "CLOB";
		case Types.DATE:
			return "DATE";
		case Types.DECIMAL:
			return "DECIMAL";
		case Types.DOUBLE:
			return "DOUBLE";
		case Types.FLOAT:
			return "FLOAT";
		case Types.INTEGER:
			return "INTEGER";
		case Types.LONGVARBINARY:
			return "LONGVARBINARY";
		case Types.LONGVARCHAR:
			return "LONGVARCHAR";
		case Types.NCHAR:
			return "NCHAR";
		case Types.NVARCHAR:
			return "NVARCHAR";
		case Types.NULL:
			return "NULL";
		case Types.NUMERIC:
			return "NUMERIC";
		case Types.REAL:
			return "REAL";
		case Types.SMALLINT:
			return "SMALLINT";
		case Types.TIME:
			return "TIME";
		case Types.TIMESTAMP:
			return "TIMESTAMP";
		case Types.TINYINT:
			return "TINYINT";
		case Types.VARBINARY:
			return "VARBINARY";
		case Types.VARCHAR:
			return "VARCHAR";
		case Types.ARRAY:
			return "ARRAY";
		case Types.JAVA_OBJECT:
			return "JAVA_OBJECT";
		default:
			return "OTHER";
		}
	}

	/**
	 * Get SQL type identifier from its name
	 * @param sToken String
	 * @return String
	 */
	public static int getSQLType (String sToken) {
		int iSQLType;
		if (sToken.equalsIgnoreCase("VARCHAR") || sToken.equalsIgnoreCase("VARCHAR2") || sToken.equalsIgnoreCase("AsciiType"))
			iSQLType = Types.VARCHAR;
		else if (sToken.equalsIgnoreCase("CHAR") || sToken.equalsIgnoreCase("UUIDType"))
			iSQLType = Types.CHAR;
		else if (sToken.equalsIgnoreCase("SMALLINT"))
			iSQLType = Types.SMALLINT;
		else if (sToken.equalsIgnoreCase("INT") || sToken.equalsIgnoreCase("INTEGER") || sToken.equalsIgnoreCase("IntegerType"))
			iSQLType = Types.INTEGER;
		else if (sToken.equalsIgnoreCase("BIGINT") || sToken.equalsIgnoreCase("SERIAL") || sToken.equalsIgnoreCase("LongType") || sToken.equalsIgnoreCase("CounterColumnType"))
			iSQLType = Types.BIGINT;
		else if (sToken.equalsIgnoreCase("FLOAT") || sToken.equalsIgnoreCase("FloatType"))
			iSQLType = Types.FLOAT;
		else if (sToken.equalsIgnoreCase("DOUBLE") || sToken.equalsIgnoreCase("DoubleType"))
			iSQLType = Types.DOUBLE;
		else if (sToken.equalsIgnoreCase("NUMERIC"))
			iSQLType = Types.NUMERIC;
		else if (sToken.equalsIgnoreCase("DECIMAL") || sToken.equalsIgnoreCase("DecimalType"))
			iSQLType = Types.DECIMAL;
		else if (sToken.equalsIgnoreCase("DATE"))
			iSQLType = Types.DATE;
		else if (sToken.equalsIgnoreCase("TIMESTAMP"))
			iSQLType = Types.TIMESTAMP;
		else if (sToken.equalsIgnoreCase("DATETIME") || sToken.equalsIgnoreCase("DateType"))
			iSQLType = Types.TIMESTAMP;
		else if (sToken.equalsIgnoreCase("NCHAR"))
			iSQLType = Types.NCHAR;
		else if (sToken.equalsIgnoreCase("NVARCHAR") || sToken.equalsIgnoreCase("UTF8Type"))
			iSQLType = Types.NVARCHAR;
		else if (sToken.equalsIgnoreCase("VARCHAR2"))
			iSQLType = Types.VARCHAR;
		else if (sToken.equalsIgnoreCase("LONGVARCHAR"))
			iSQLType = Types.LONGVARCHAR;
		else if (sToken.equalsIgnoreCase("LONG"))
			iSQLType = Types.LONGVARCHAR;
		else if (sToken.equalsIgnoreCase("TEXT"))
			iSQLType = Types.LONGVARCHAR;
		else if (sToken.equalsIgnoreCase("LONGVARBINARY") || sToken.equalsIgnoreCase("BytesType"))
			iSQLType = Types.LONGVARBINARY;
		else if (sToken.equalsIgnoreCase("LONG RAW"))
			iSQLType = Types.LONGVARBINARY;
		else if (sToken.equalsIgnoreCase("BLOB"))
			iSQLType = Types.BLOB;
		else if (sToken.equalsIgnoreCase("CLOB"))
			iSQLType = Types.CLOB;
		else if (sToken.equalsIgnoreCase("ARRAY"))
			iSQLType = Types.ARRAY;
		else if (sToken.equalsIgnoreCase("JAVA_OBJECT"))
			iSQLType = Types.JAVA_OBJECT;
		else
			iSQLType = Types.NULL;
		return iSQLType;
	}

	/**
	 * <p>Get column default precision in bytes</p>
	 * <table summary="Default Precisions">
	 * <tr><th>Type</th><th>Length</th></tr>
	 * <tr><td></td><td></td></tr>
	 * <tr><td>ARRAY</td><td>0</td></tr>
	 * <tr><td>BIT</td><td>1</td></tr>
	 * <tr><td>BOOLEAN</td><td>4</td></tr>
	 * <tr><td>SMALLINT</td><td>2</td></tr>
	 * <tr><td>INTEGER</td><td>4</td></tr>
	 * <tr><td>BIGINT</td><td>8</td></tr>
	 * <tr><td>FLOAT</td><td>17</td></tr>
	 * <tr><td>REAL</td><td>17</td></tr>
	 * <tr><td>DOUBLE</td><td>17</td></tr>
	 * <tr><td>NUMERIC</td><td>38</td></tr>
	 * <tr><td>DECIMAL</td><td>38</td></tr>
	 * <tr><td>CHAR</td><td>255</td></tr>
	 * <tr><td>VARCHAR</td><td>255</td></tr>
	 * <tr><td>VARBINARY</td><td>255</td></tr>
	 * <tr><td>NCHAR</td><td>127</td></tr>
	 * <tr><td>NVARCHAR</td><td>127</td></tr>
	 * <tr><td>LONGNVARCHAR</td><td>1073741822</td></tr>
	 * <tr><td>LONGVARCHAR</td><td>2147483647</td></tr>
	 * <tr><td>CLOB</td><td>2147483647</td></tr>
	 * <tr><td>BLOB</td><td>2147483647</td></tr>
	 * <tr><td>LONGVARBINARY</td><td>2147483647</td></tr>
	 * <tr><td>TIME</td><td>10</td></tr>
	 * <tr><td>DATE</td><td>10</td></tr>
	 * <tr><td>TIME_WITH_TIMEZONE</td><td>14</td></tr>
	 * <tr><td>TIMESTAMP</td><td>26</td></tr>
	 * <tr><td>TIMESTAMP_WITH_TIMEZONE</td><td>30</td></tr>
	 * </table>
	 * @param columnType int from java.sql.Types
	 * @return
	 */
	public static int getDefaultPrecision(int columnType) {
		int maxLength;
		switch (columnType) {
		case Types.ARRAY:
			maxLength = 0;
			break;			
		case Types.BIT:
			maxLength = 1;
			break;			
		case Types.BOOLEAN:
			maxLength = 4;
			break;			
		case Types.SMALLINT:
			maxLength = 2;
			break;
		case Types.INTEGER:
			maxLength = 4;
			break;
		case Types.FLOAT:
		case Types.REAL:
			maxLength = 17;
			break;
		case Types.BIGINT:
			maxLength = 8;
			break;
		case Types.DOUBLE:
			maxLength = 17;
			break;
		case Types.NUMERIC:
		case Types.DECIMAL:
			maxLength = 38;
			break;
		case Types.CHAR:
		case Types.VARCHAR:
		case Types.VARBINARY:
			maxLength = 255;
			break;
		case Types.NCHAR:
		case Types.NVARCHAR:
			maxLength = 127;
			break;
		case Types.CLOB:
		case Types.BLOB:
		case Types.LONGVARCHAR:
		case Types.LONGVARBINARY:
			maxLength = 2147483647;
			break;
		case Types.LONGNVARCHAR:
			maxLength = 1073741822;
			break;
		case Types.TIME_WITH_TIMEZONE:
			maxLength = 14;
			break;
		case Types.TIME:
		case Types.DATE:
			maxLength = 10;
			break;
		case Types.TIMESTAMP:
			maxLength = 26;
			break;
		case Types.TIMESTAMP_WITH_TIMEZONE:
			maxLength = 30;
			break;
		default:
			throw new IllegalArgumentException("Unrecognized SQL type "+String.valueOf(columnType));
		}
		return maxLength;
	}

	/**
	 * Infer SQL type for a Java object
	 * @param Object
	 * @return int
	 * @see java.sql.Types
	 */
	public static int typeForObject(Object obj) {
	if (obj==null)
		return Types.NULL;
	else if (obj instanceof String)
		return Types.VARCHAR;
	else if (obj instanceof Byte)
		return Types.TINYINT;
	else if (obj instanceof Short)
		return Types.SMALLINT;
	else if (obj instanceof Integer)
		return Types.INTEGER;
	else if (obj instanceof Long)
		return Types.BIGINT;
	else if (obj instanceof Float)
		return Types.FLOAT;
	else if (obj instanceof Double)
		return Types.DOUBLE;
	else if (obj instanceof BigDecimal)
		return Types.DECIMAL;
	else if (obj instanceof Date)
		return Types.TIMESTAMP;
	else if (obj instanceof byte[])
		return Types.BINARY;
	else if (obj.getClass().isArray())
		return Types.ARRAY;
	else
		return Types.JAVA_OBJECT;
	}
}
