package org.judal.storage.table.impl;

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

import java.util.Date;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import javax.jdo.FetchGroup;
import javax.jdo.JDOException;
import javax.jdo.JDOUserException;
import javax.jdo.metadata.ColumnMetadata;

import java.util.Iterator;

import org.judal.metadata.ViewDef;
import org.judal.metadata.ColumnDef;
import org.judal.metadata.PrimaryKeyDef;
import org.judal.serialization.BytesConverter;
import org.judal.serialization.JSONValue;
import org.judal.storage.ConstraintsChecker;
import org.judal.storage.DataSource;
import org.judal.storage.EngineFactory;
import org.judal.storage.FieldHelper;
import org.judal.storage.table.ColumnGroup;
import org.judal.storage.table.Record;
import org.judal.storage.table.Table;
import org.judal.storage.table.TableDataSource;

import com.knowgate.currency.Money;
import com.knowgate.dateutils.DateHelper;
import com.knowgate.debug.DebugFile;
import com.knowgate.gis.LatLong;
import com.knowgate.stringutils.Html;
import com.knowgate.stringutils.XML;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import java.lang.reflect.InvocationTargetException;

import java.math.BigDecimal;

import java.text.NumberFormat;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.Format;
import java.text.ParseException;
import java.text.SimpleDateFormat;


/**
 * <p>Partial implementation of Record interface.</p>
 * <p>The actual storage of rows is determined by a subclass.
 * It may be storing each row as Java member variables, an array or as a map
 * depending on whether PojoRecord, ArrayRecord, MapRecord is used.</p>
 * <p>Each AbstractRecord is associated with an underlying ViewDef or TableDef
 * which describes the list of columns held by the Record.
 * The ViewDef or TableDef may represent an actual table or view in the database
 * or may be generated on the fly to represent any set of columns.</p>
 * <p>If the actual implementation is of type MapRecord, then the Record may hold
 * more columns than just the ones described in the ViewDef/TableDef. This is in
 * order to accommodate varying columns list from schemaless storage systems or
 * growing column families from column-oriented databases.</p>
 * <p>It is possible to associate a ConstraintsChecker and a FieldHelper to the Record.
 * These classes help to perform client-side validations on data integrity and custom
 * type conversion for non-standard types of a database.</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 *
 */
public abstract class AbstractRecord implements Record {

	private static final long serialVersionUID = 10000l;

	protected transient ViewDef tableDef;
	protected transient ConstraintsChecker checker;
	protected transient FieldHelper fhelper;
	protected boolean hasLongVarBinaryData;
	protected HashMap<String,Long> longVarBinariesLengths;

	/**
	 * <p>Construct AbstractRecord reading the metadata from a ViewDef or TableDef and with no ConstraintsChecker nor FieldHelper.</p>
	 * @param tableDefinition ViewDef or TableDef
	 * @throws JDOUserException if tableDefinition is <b>null</b>
	 */
	public AbstractRecord(ViewDef tableDefinition) throws JDOUserException {
		this(tableDefinition, null, null);
	}

	/**
	 * <p>Construct AbstractRecord reading the metadata from a ViewDef or TableDef and with the given ConstraintsChecker and no FieldHelper.</p>
	 * @param tableDefinition ViewDef or TableDef
	 * @param constraintsChecker ConstraintsChecker
	 * @throws JDOUserException if tableDefinition is <b>null</b>
	 */
	public AbstractRecord(ViewDef tableDefinition, ConstraintsChecker constraintsChecker) throws JDOUserException {
		this(tableDefinition, null, constraintsChecker);
	}

	/**
	 * <p>Construct AbstractRecord reading the metadata from a ViewDef or TableDef and with the given FieldHelper and no ConstraintsChecker.</p>
	 * @param tableDefinition ViewDef or TableDef
	 * @param fieldHelper FieldHelper
	 * @throws JDOUserException if tableDefinition is <b>null</b>
	 */
	public AbstractRecord(ViewDef tableDefinition, FieldHelper fieldHelper) throws JDOUserException {
		this(tableDefinition, fieldHelper, null);
	}

	/**
	 * <p>Construct AbstractRecord reading the metadata from a ViewDef or TableDef and with the given FieldHelper and ConstraintsChecker.</p>
	 * @param tableDefinition ViewDef or TableDef
	 * @param fieldHelper FieldHelper
	 * @param constraintsChecker ConstraintsChecker
	 * @throws JDOUserException if tableDefinition is <b>null</b>
	 */
	public AbstractRecord(ViewDef tableDefinition, FieldHelper fieldHelper, ConstraintsChecker constraintsChecker) throws JDOUserException {
		if (null==tableDefinition)
			throw new JDOUserException("AbstractRecord constructor. TableDef cannot be null");
		tableDef = tableDefinition;
		setFieldHelper(fieldHelper);
		setConstraintsChecker(constraintsChecker);
		clearLongData();
	}
	
	/**
	 * <p>Construct AbstractRecord reading the metadata directly from a DataSource capable of providing it.</p>
	 * @param dataSource TableDataSource
	 * @param tableName Name of table or view
	 * @throws JDOException
	 * @throws JDOUserException if dataSource is <b>null</b>
	 */
	public AbstractRecord(TableDataSource dataSource, String tableName) throws JDOException {
		this(dataSource, tableName, null, null);
	}

	/**
	 * <p>Construct AbstractRecord reading the metadata directly from a DataSource capable of providing it.</p>
	 * @param dataSource TableDataSource
	 * @param tableName Name of table or view
	 * @param fieldHelper FieldHelper
	 * @throws JDOException
	 * @throws JDOUserException if dataSource is <b>null</b>
	 */
	public AbstractRecord(TableDataSource dataSource, String tableName, FieldHelper fieldHelper) throws JDOException {
		this(dataSource, tableName, fieldHelper, null);
	}
	
	/**
	 * <p>Construct AbstractRecord reading the metadata directly from a DataSource capable of providing it.</p>
	 * @param dataSource TableDataSource
	 * @param tableName Name of table or view
	 * @param constraintsChecker ConstraintsChecker
	 * @throws JDOException
	 * @throws JDOUserException if dataSource is <b>null</b>
	 */
	public AbstractRecord(TableDataSource dataSource, String tableName, ConstraintsChecker constraintsChecker) throws JDOException {
		this(dataSource, tableName, null, constraintsChecker);
	}

	/**
	 * <p>Construct AbstractRecord reading the metadata directly from a DataSource capable of providing it.</p>
	 * @param dataSource TableDataSource
	 * @param tableName Name of table or view
	 * @param fieldHelper FieldHelper
	 * @param constraintsChecker ConstraintsChecker
	 * @throws JDOException
	 * @throws JDOUserException if dataSource is <b>null</b>
	 */
	public AbstractRecord(TableDataSource dataSource, String tableName, FieldHelper fieldHelper, ConstraintsChecker constraintsChecker) throws JDOException {
		if (null==dataSource)
			throw new JDOUserException("No DataSource specified and no one provided by EngineFactory neither");
		tableDef = dataSource.getTableDef(tableName);
		if (null==tableDef)
			throw new JDOException("Table "+tableName+" does not exist or could not be read from the data source");
		setFieldHelper(fieldHelper);
		setConstraintsChecker(constraintsChecker);
		clearLongData();
	}
	
	/**
	 * <p>Get the value of a column.</p>
	 * @return Object Returned value may be <b>null</b> or an Option[AnyRef] if using implementation for Scala.
	 */
	@Override
	public abstract Object apply(String columnName);

	/**
	 * <p>Set value of a column.</p>
	 * @param colname String Column Name
	 * @param value Object Column value
	 */
	@Override
	public abstract Object put(String colname, Object value);

	/**
	 * <p>Remove value of a column.</p>
	 * This method may have different behaviors depending on the implementation.
	 * For MapRecord subclass, removing a column will cause that it won't be updated in the database.
	 * But for ArrayRecord and PojoRecord, removing a column will set it to <b>null</b> or cause an exception.
	 * @param colname String Column Name
	 */
	@Override
	public abstract Object remove(String colname);

	/**
	 * <p>Remove values for all columns.</p>
	 * This method may have different behaviors depending on the implementation.
	 */
	@Override
	public abstract void clear();

	/**
	 * @return ConstraintsChecker
	 */
	@Override
	public ConstraintsChecker getConstraintsChecker() {
		return checker;
	}

	/**
	 * @param checker ConstraintsChecker
	 */
	public void setConstraintsChecker(ConstraintsChecker checker) {
		this.checker = checker;
	}

	/**
	 * @return FieldHelper
	 */
	public FieldHelper getFieldHelper() {
		return fhelper;
	}

	/**
	 * @param helper FieldHelper
	 */
	public void setFieldHelper(FieldHelper helper) {
		this.fhelper = helper;
	}

	/**
	 * <p>Replace value of a column.</p>
	 * @param colname String Column Name
	 * @param newvalue Object New value for column
	 * @return Object Former value of column
	 */
	@Override
	public Object replace(String colname, Object newvalue) {
		Object retval = apply(colname);
		remove(colname);
		put(colname, newvalue);
		return retval;
	}
	
	/**
	 * <p>Load Record using given DataSource.</p>
	 * @param oDts TableDataSource
	 * @param oKey Object Value for primary key. May actually be Object[] if the key has multiple columns.
	 * @return <b>true</b> if a Record was found with the given primary key, <b>false</b>otherwise.
	 * @throws JDOException If the underlying table has no primary key
	 * @throws ClassCastException If oDts is not an instance of class TableDataSource or a subclass of it.
	 */
	@Override
	public boolean load(DataSource oDts, Object oKey) throws JDOException, ClassCastException {
		boolean bLoaded = false;
		try (Table oTbl = ((TableDataSource) oDts).openTable(this)) {
			bLoaded = oTbl.load(oKey, this);
		}
		return bLoaded;
	}

	/**
	 * <p>Load Record using EngineFactory.getDefaultTableDataSource().</p>
	 * @param oKey Object Value for primary key. May actually be Object[] if the key has multiple columns.
	 * @return <b>true</b> if a Record was found with the given primary key, <b>false</b>otherwise.
	 * @throws JDOException If the underlying table has no primary key
	 * @throws NullPointerException If EngineFactory.getDefaultTableDataSource() is not set
	 */
	@Override
	public final boolean load(Object oKey) throws JDOException,NullPointerException {
		return load(EngineFactory.getDefaultTableDataSource(), oKey);
	}

	/**
	 * <p>Store this Record using given DataSource.</p>
	 * A store operation will insert the Record if it does not exist or update it if already exists.
	 * If a ConstraintsChecker has been set, its check() method will be called before attempting to store the Record.
	 * This may result in a JDOException thrown if the Record does not comply with the constraints.
	 * @param oDts TableDataSource
	 * @throws JDOException
	 * @throws ClassCastException If oDts is not an instance of class TableDataSource or a subclass of it.
	 */
	@Override
	public void store(DataSource oDts) throws JDOException,ClassCastException {
		if (getConstraintsChecker()!=null)
			getConstraintsChecker().check(oDts, this);
		try (Table oTbl = ((TableDataSource) oDts).openTable(this)) {
			oTbl.store(this);
		}
	}

	/**
	 * <p>Store this Record using EngineFactory.getDefaultTableDataSource().</p>
	 * A store operation will insert the Record if it does not exist or update it if already exists.
	 * If a ConstraintsChecker has been set, its check() method will be called before attempting to store the Record.
	 * This may result in a JDOException thrown if the Record does not comply with the constraints.
	 * @throws JDOException
	 */
	@Override
	public final void store() throws JDOException {
		store(EngineFactory.getDefaultTableDataSource());
	}

	/**
	 * <p>Delete this Record using the given DataSource.</p>
	 * @param oDts TableDataSource
	 * @throws JDOException
	 * @throws ClassCastException If oDts is not an instance of class TableDataSource or a subclass of it.
	 */
	@Override
	public void delete(DataSource oDts) throws JDOException,ClassCastException {
		Table oTbl = ((TableDataSource) oDts).openTable(this);
		try {
			oTbl.delete(getKey());
		} finally {
			if (oTbl!=null) oTbl.close();
		}
	}

	/**
	 * <p>Delete this Record using EngineFactory.getDefaultTableDataSource().</p>
	 * @throws JDOException
	 */
	@Override
	public final void delete() throws JDOException {
		delete(EngineFactory.getDefaultTableDataSource());
	}
	
	/**
	 * @return ViewDef
	 */
	public ViewDef getTableDef() {
		return tableDef;
	}

	/**
	 * @param viewDef ViewDef
	 */
	public void setTableDef(ViewDef viewDef) {
		tableDef = viewDef;
	}
	
	public void clearLongData() {
		hasLongVarBinaryData = false;
		longVarBinariesLengths = null;
	}

	/**
	 * @return boolean <b>true</b> if this Record contains any LONGVARCHAR, LONGVARBINARY, CLOB or BLOB column
	 */
	public boolean hasLongData() {
		return hasLongVarBinaryData;
	}

	/**
	 * @return Map&lt;String,Long&gt;
	 */
	public Map<String,Long> longDataLengths() {
		return longVarBinariesLengths;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ColumnDef[] columns() {
		return tableDef.getColumns();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public FetchGroup fetchGroup() {
		FetchGroup group = new ColumnGroup();
		for (ColumnDef cdef : columns())
			group.addMember(cdef.getName());
		group.addCategory(FetchGroup.DEFAULT);
		return group;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ColumnDef getColumn(String colname) throws ArrayIndexOutOfBoundsException {
		return tableDef.getColumnByName(colname);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getBucketName() {
		return tableDef.getName();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getTableName() {
		return tableDef.getName();
	}

	/**
	 * <p>Get value of primary key.</p>
	 * @return Value of record primary key. Will be an array of Object if the primary key is composed of multiple columns.
	 * @throws JDOUserException if table or view has no primary key
	 */
	@Override
	public Object getKey() throws JDOException {
		Object retval;
		PrimaryKeyDef pk = tableDef.getPrimaryKeyMetadata();
		if (null==pk)
			throw new JDOUserException("Table or view "+getTableName()+"  has no primary key");
		if (DebugFile.trace) {
			DebugFile.writeln("Begin AbstractRecord.getKey()");
			DebugFile.incIdent();
			DebugFile.writeln("primary key has " +  String.valueOf(pk.getNumberOfColumns()) + " columns");
		}
		if (pk.getNumberOfColumns()==0) {
			retval = null;
		} else if (pk.getNumberOfColumns()==1) {
			retval = apply(pk.getColumn());
			if (retval instanceof String) {
				try {
					retval = pk.getColumns()[0].convert((String) retval);
				} catch (NumberFormatException | NullPointerException | ParseException e) {
					throw new JDOUserException("AbstractRecord.getKey() " + e.getClass().getName()+" " + e.getMessage());
				}
			}
		}  else {
			Object[] retvals = new Object[pk.getNumberOfColumns()];
			int c = 0;
			for (ColumnMetadata col : pk.getColumns()) {
				retvals[c] = apply(col.getName());
				if (retvals[c] instanceof String) {
					try {
						retvals[c] = pk.getColumns()[c].convert((String) retvals[c]);
					} catch (NumberFormatException | NullPointerException | ParseException e) {
						throw new JDOUserException("AbstractRecord.getKey() " + e.getClass().getName()+" " + e.getMessage());
					}
				}
				c++;
			}
			retval = (Object) retvals;
		}

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End AbstractRecord.getKey() : " + retval + (retval!=null ? " of class " + retval.getClass().getName() : ""));
		}
		return (Object) retval;
	}

	/**
	 * <p>Set value of primary key for this record.</p>
	 * @param value If primary key is composed of multiple columns then must be an array of Object
	 * @throws JDOException
	 */
	@Override
	public void setKey(Object value) throws JDOException {
		PrimaryKeyDef pk = tableDef.getPrimaryKeyMetadata();
		if (pk.getNumberOfColumns()==0) throw new JDOException("Table "+tableDef.getName()+" has no primary key");
		if (pk.getNumberOfColumns()==1) {
			if (DebugFile.trace) DebugFile.writeln("set "+pk.getColumn()+"="+value);
			if (pk.getColumn()==null)
				throw new JDOUserException("Primary key column name for table "+tableDef.getName()+" is null");
			else if (pk.getColumn().trim().length()==0)
				throw new JDOUserException("Primary key column name for table "+tableDef.getName()+" is empty");
			if (value instanceof String) {
				try {
					put(pk.getColumn(), pk.getColumns()[0].convert((String) value));
				} catch (NumberFormatException | NullPointerException | ParseException e) {
					throw new JDOUserException("AbstractRecord.setKey("+ value + ") "+e.getClass().getName()+" "+e.getMessage());
				}
			} else {
				put(pk.getColumn(), value);
			}
		} else {
			try {
				Object[] vals = (Object[]) value;
				int c = 0;
				for (ColumnMetadata col : pk.getColumns()) {
					if (value instanceof String) {
						try {
							put(col.getName(), pk.getColumns()[c].convert((String) vals[c]));
							c++;
						} catch (NumberFormatException | NullPointerException | ParseException e) {
							throw new JDOUserException("AbstractRecord.setKey("+ value + ") "+e.getClass().getName()+" "+e.getMessage());
						}
					}  else {
						put(col.getName(), vals[c++]);						
					}
				}
			} catch (ClassCastException cce) {
				String pkcols = "";
				for (ColumnMetadata col : pk.getColumns())
					pkcols += "," + col.getName();
				throw new JDOException("AbstractRecord.setKey() Key value bust be either a basic type or and Object[] but was "+value.getClass().getName()+" for ("+pkcols.substring(1)+")", cce);
			}
		}
	}

	/**
	 * <p>Set value of content, contentType and contentLength fields.</p>
	 * @param bytes byte[] Value of content field
	 * @param contentType String Value of contentType field
	 * @throws JDOException
	 */
	@Override
	public void setContent(byte[] bytes, String contentType) throws JDOException {
		put("content", bytes);
		put("contentType", contentType);
		if (isNullOrNone(bytes))
			put("contentLength", new Long(0l));
		else
			put("contentLength", new Long(bytes.length));			
	}

	/**
	 * <p>Get value for a DATETIME field<p>
	 * @param sKey Field Name
	 * @return Calendar value or <b>null</b>.
	 * @throws ClassCastException if sKey field is not of type DATETIME
	 */
	@Override
	public Calendar getCalendar(String sKey) throws ClassCastException {
		Date dDt = DateHelper.toDate(apply(sKey));
		if (dDt==null) {
			return null;
		} else {
			Calendar cDt = new GregorianCalendar();
			cDt.setTime(dDt);	
			return cDt;
		}
	} // getCalendar

	/**
	 * <p>Get value for a DATETIME field<p>
	 * @param sKey Field Name
	 * @return Date value or <b>null</b>.
	 * @throws ClassCastException if sKey field is not of type DATETIME
	 */
	@Override
	public Date getDate(String sKey) throws ClassCastException {
		return DateHelper.toDate(apply(sKey));
	} // getDate

	/**
	 * <p>Get value for a DATETIME field<p>
	 * @param sKey Field Name
	 * @param dtDefault Date default value
	 * @return Date value or default value.
	 * @throws ClassCastException if sKey field is not of type DATETIME
	 */
	@Override
	public Date getDate(String sKey, Date dtDefault) throws ClassCastException {
		Date dtRetVal;
		if (isNull(sKey)) {
			dtRetVal = dtDefault;
		} else {
			dtRetVal = getDate(sKey);
			if (null==dtRetVal) dtRetVal = dtDefault;
		}
		return dtRetVal;
	}

	/**
	 * <p>Get value for a DATE, DATETIME or TIMESTAMP field formated a String<p>
	 * @param sColName String Column Name
	 * @param sFormat Date Format (like "yyyy-MM-dd HH:mm:ss")
	 * @return Formated date or <b>null</b>.
	 * @throws ClassCastException if sKey field is not of type DATE, DATETIME or TIMESTAMP
	 * @see java.text.SimpleDateFormat
	 */
	@Override
	public String getDateFormated(String sColName, String sFormat)
			throws ClassCastException {
		Date oDt = getDate(sColName);
		SimpleDateFormat oSimpleDate;
		if (null!=oDt) {
			oSimpleDate = new SimpleDateFormat(sFormat);
			return oSimpleDate.format(oDt);
		}
		else
			return null;
	} // getDateFormated()

	/**
	 * <p>Get value for a DATE, DATETIME or TIMESTAMP field formated a yyyy-MM-dd hh:mm:ss<p>
	 * @param sColName String Column Name
	 * @return String Formated date or <b>null</b>.
	 * @throws ClassCastException if sKey field is not of type DATE
	 */
	@Override
	public String getDateTime(String sColName) throws ClassCastException {
		return getDateFormated(sColName, "yyyy-MM-dd hh:mm:ss");
	} // getDateTime

	/**
	 * <p>Get value for a DATE, DATETIME or TIMESTAMP field formated a yyyy-MM-dd HH:mm:ss<p>
	 * @param sColName String Column Name
	 * @return String Formated date or <b>null</b>.
	 * @throws ClassCastException if sKey field is not of type DATE, DATETIME or TIMESTAMP
	 */
	public String getDateTime24(String sColName) {
		return getDateFormated(sColName, "yyyy-MM-dd HH:mm:ss");
	} // getDateTime24()

	/**
	 * <p>Get DATE formated as ccyy-MM-dd<p>
	 * @param sColName Column Name
	 * @throws ClassCastException if sKey field is not of type DATE
	 * @return String value for Date or <b>null</b>.
	 */
	@Override
	@SuppressWarnings("deprecation")
	public String getDateShort(String sColName) throws ClassCastException {
		Date dDt = getDate(sColName);
		if (null!=dDt) {
			int y = dDt.getYear()+1900, m=dDt.getMonth()+1, d=dDt.getDate();
			return String.valueOf(y)+"-"+(m<10 ? "0" : "")+String.valueOf(m)+"-"+(d<10 ? "0" : "")+String.valueOf(d);
		}
		else
			return null;
	} // getDateShort

	/**
	 * <p>Get value of an interval part.</p>
	 * @param sColName String Column Name
	 * @param sPart String Part Identifier
	 * @return int
	 * @throws ClassNotFoundException If no FieldHelper has been set for this Record
	 * @throws ClassCastException
	 * @throws NullPointerException
	 * @throws NumberFormatException
	 * @throws IllegalArgumentException
	 */
	@Override
	public int getIntervalPart(String sColName, String sPart) throws ClassCastException, ClassNotFoundException, NullPointerException, NumberFormatException, IllegalArgumentException {
		if (null==fhelper)
			throw new ClassNotFoundException("No FieldHelper class has been specified for " + getTableName());
		return fhelper.getIntervalPart(this, sColName, sPart);
	}

	/**
	 * @param sColName String Column Name
	 * @return LatLong
	 * @throws ClassNotFoundException If no FieldHelper has been set for this Record
	 * @throws ClassCastException
	 * @throws NumberFormatException
	 * @throws ArrayIndexOutOfBoundsException
	 */
	@Override
	public LatLong getLatLong(String sColName) throws ClassCastException, ClassNotFoundException, NumberFormatException, ArrayIndexOutOfBoundsException {
		if (null==fhelper)
			throw new ClassNotFoundException("No FieldHelper class has been specified for " + getTableName());
		return fhelper.getLatLong(this, sColName);
	}
	
	/**
	 * <p>Get column as byte array.</p>
	 * If the column value is <b>null</b> then <b>null</b> will be returned
	 * Else If the column type is byte[] that same value will be returned
	 * Else this method will call org.judal.serialization.BytesConverter.toBytes(value, getColumn(sColName).getType())
	 * to convert the column Object into a byte array.
	 * @param sColName String Column Name
	 * @return byte[]
	 */
	@Override
	public byte[] getBytes(String sColName) {
		if (isNull(sColName))
			return null;
		else {
			Object value = apply(sColName);
			if (value instanceof byte[])
				return (byte[]) value;
			else
				return BytesConverter.toBytes(value, getColumn(sColName).getType());
		}
	}

	/**
	 * <p>Get column value as BigDecimal.</p>
	 * If the actual value of the column is not BigDecimal Then this function will try to convert it to BigDecimal.
	 * @param sColName String
	 * @return BigDecimal
	 * @throws NullPointerException
	 * @throws ClassCastException
	 * @throws NumberFormatException
	 */
	@Override
	public BigDecimal getDecimal(String sColName)
			throws NullPointerException,ClassCastException,NumberFormatException {
		if (!isNull(sColName)) {
		Object oDec = apply(sColName);
		if (oDec instanceof BigDecimal)
			return (BigDecimal) oDec;
		else if (oDec instanceof String)
			return new BigDecimal((String) oDec);
		else
			return new BigDecimal(oDec.toString());      
		} else {
			return null;
		}
	}

	/**
	 * <p>Get BigDecimal formated as a String using the given locale and fractional digits</p>
	 * @param sColName String Column Name
	 * @param oLoc Locale
	 * @param nFractionDigits Number of digits at the right of the decimal separator
	 * @return String decimal value formated according to Locale or <b>null</b>
	 * @throws ClassCastException
	 * @throws NumberFormatException
	 * @throws NullPointerException if sPattern is <b>null</b>
	 * @throws IllegalArgumentException if sPattern is invalid
	 */
	@Override
	public String getDecimalFormated(String sColName, Locale oLoc, int nFractionDigits)
			throws ClassCastException, NumberFormatException,NullPointerException,
			IllegalArgumentException {
		BigDecimal oDec = getDecimal(sColName);

		if (oDec==null) {
			return null;
		} else {
			DecimalFormat oNumFmt = (DecimalFormat) NumberFormat.getNumberInstance(oLoc);
			oNumFmt.setMaximumFractionDigits(nFractionDigits);
			return oNumFmt.format(oDec);
		}
	} // getDecimalFormated

	/**
	 * <p>Get BigDecimal formated as a String using the given pattern and the symbols for the default locale</p>
	 * @param sColName String Column Name
	 * @param sPattern String A non-localized pattern string, for example: "#0.00"
	 * @return String decimal value formated according to sPatern or <b>null</b>
	 * @throws ClassCastException
	 * @throws NumberFormatException
	 * @throws NullPointerException if sPattern is <b>null</b>
	 * @throws IllegalArgumentException if sPattern is invalid
	 */
	@Override
	public String getDecimalFormated(String sColName, String sPattern)
			throws ClassCastException, NumberFormatException,NullPointerException,
			IllegalArgumentException {
		BigDecimal oDec = getDecimal(sColName);

		if (oDec==null) {
			return null;
		} else {
			return new DecimalFormat(sPattern).format(oDec.doubleValue());
		}
	} // getDecimalFormated

	/**
	 * <p>Get double formated as a String using the given pattern and the symbols for the default locale</p>
	 * @param sColName Column Name
	 * @param sPattern A non-localized pattern string, for example: "#0.00"
	 * @return String decimal value formated according to sPatern or <b>null</b>
	 * @throws ClassCastException
	 * @throws NumberFormatException
	 * @throws NullPointerException
	 * @throws IllegalArgumentException
	 */
	public String getDoubleFormated(String sColName, String sPattern)
			throws ClassCastException,NumberFormatException,NullPointerException,IllegalArgumentException {
		if (isNull(sColName))
			return null;
		else
			return new DecimalFormat(sPattern).format(getDouble(sColName));
	}

	/**
	 * <p>Get float formated as a String using the given pattern and the symbols for the default locale</p>
	 * @param sColName Column Name
	 * @param sPattern A non-localized pattern string, for example: "#0.00"
	 * @return String decimal value formated according to sPatern or <b>null</b>
	 * @throws ClassCastException
	 * @throws NumberFormatException
	 * @throws NullPointerException
	 * @throws IllegalArgumentException
	 */
	public String getFloatFormated(String sColName, String sPattern)
			throws ClassCastException, NumberFormatException,NullPointerException, IllegalArgumentException {
		if (isNull(sColName))
			return null;
		else
			return new DecimalFormat(sPattern).format(getFloat(sColName));
	}

	/**
	 * <p>Get value of column as int.</p>
	 * This function must not be used to retrieve nullable values.
	 * If the actual value of the column is not int Then this function will try to convert it to int.
	 * @param sColName Column Name
	 * @return int
	 * @throws NullPointerException if column value is <b>null</b> or <b>None</b>
	 * @throws ClassCastException
	 * @throws NumberFormatException
	 */
	@Override
	public int getInt(String sColName)
			throws NullPointerException,ClassCastException,NumberFormatException {
		if (!isNull(sColName)) {
			Object oInt = apply(sColName);
			if (oInt instanceof Integer)
				return ((Integer) oInt).intValue();
			else if (oInt instanceof String) {
				String sInt = (String) oInt;
				if (sInt.endsWith(".0")) sInt = sInt.substring(0, sInt.length()-2);
				return Integer.parseInt(sInt);
			} else {
				String sInt = oInt.toString();
				if (sInt.endsWith(".0")) sInt = sInt.substring(0, sInt.length()-2);
				return Integer.parseInt(sInt);
			}
		} else {
			throw new NullPointerException("Column "+sColName+" is null");
		}
	}

	/**
	 * <p>Get value of column holding a map of key-value pairs.</p>
	 * @param sColName String Column Name
	 * @return Object A Map which class depends on the FieldHelper implementation
	 * @throws ClassNotFoundException if not FieldHelper has been set
	 * @throws ClassCastException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws IllegalArgumentException
	 * @throws InvocationTargetException
	 * @throws NoSuchMethodException
	 * @throws SecurityException
	 */
	@Override
	public Object getMap(String sColName) throws ClassCastException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, ClassNotFoundException {
		if (null==fhelper)
			throw new ClassNotFoundException("No FieldHelper class has been specified for " + getTableName());
		return fhelper.getMap(this, sColName);
	}
	
	/**
	 * <p>Get column value as Integer.</p>
	 * If the actual value of the column is not Integer Then this function will try to convert it to Integer.
	 * @param sColName String
	 * @return Integer
	 * @throws NumberFormatException
	 */
	@Override
	public Integer getInteger(String sColName) throws NumberFormatException {
		if (!isNull(sColName)) {
			Object oInt = apply(sColName);
			if (oInt instanceof Integer)
				return (Integer) oInt;
			else if (oInt instanceof String)
				return new Integer((String) oInt);
			else if (oInt instanceof BigDecimal)
				return new Integer(((BigDecimal) oInt).intValue());
			else
				return new Integer(oInt.toString());
		} else {
			return null;
		}
	}

	/**
	 * <p>Get Integer array value.</p>
	 * @param sColName String
	 * @return Integer[]
	 * @throws ClassNotFoundException If no FieldHelper has been set for this Record
	 * @throws ClassCastException
	 */
	@Override
	public Integer[] getIntegerArray(String sColName) throws ClassCastException, ClassNotFoundException {
		if (null==fhelper)
			throw new ClassNotFoundException("No FieldHelper class has been specified for " + getTableName());
		return fhelper.getIntegerArray(this, sColName);
	}

	/**
	 * <p>Get Long array value.</p>
	 * @param sColName String
	 * @return Long[]
	 * @throws ClassNotFoundException If no FieldHelper has been set for this Record
	 * @throws ClassCastException
	 */
	@Override
	public Long[] getLongArray(String sColName) throws ClassCastException, ClassNotFoundException {
		if (null==fhelper)
			throw new ClassNotFoundException("No FieldHelper class has been specified for " + getTableName());
		return fhelper.getLongArray(this, sColName);
	}

	/**
	 * <p>Get Float array value.</p>
	 * @param sColName String
	 * @return Float[]
	 * @throws ClassNotFoundException If no FieldHelper has been set for this Record
	 * @throws ClassCastException
	 */
	@Override
	public Float[] getFloatArray(String sColName) throws ClassCastException, ClassNotFoundException {
		if (null==fhelper)
			throw new ClassNotFoundException("No FieldHelper class has been specified for " + getTableName());
		return fhelper.getFloatArray(this, sColName);
	}

	/**
	 * <p>Get Double array value.</p>
	 * @param sColName String
	 * @return Double[]
	 * @throws ClassNotFoundException If no FieldHelper has been set for this Record
	 * @throws ClassCastException
	 */
	@Override
	public Double[] getDoubleArray(String sColName) throws ClassCastException, ClassNotFoundException {
		if (null==fhelper)
			throw new ClassNotFoundException("No FieldHelper class has been specified for " + getTableName());
		return fhelper.getDoubleArray(this, sColName);
	}

	/**
	 * <p>Get Date array value.</p>
	 * @param sColName String
	 * @return Date[]
	 * @throws ClassNotFoundException If no FieldHelper has been set for this Record
	 * @throws ClassCastException
	 */
	@Override
	public Date[] getDateArray(String sColName) throws ClassCastException, ClassNotFoundException {
		if (null==fhelper)
			throw new ClassNotFoundException("No FieldHelper class has been specified for " + getTableName());
		return fhelper.getDateArray(this, sColName);
	}
	
	/**
	 * <p>Get String array value.</p>
	 * @param sColName String
	 * @return String[]
	 * @throws ClassNotFoundException If no FieldHelper has been set for this Record
	 * @throws ClassCastException
	 */
	@Override
	public String[] getStringArray(String sColName) throws ClassCastException, ClassNotFoundException {
		if (null==fhelper)
			throw new ClassNotFoundException("No FieldHelper class has been specified for " + getTableName());
		return fhelper.getStringArray(this, sColName);
	}
	
	/**
	 * <p>Get value of a VARCHAR field that holds a money+currency amount<p>
	 * Money values are stored with its currency sign embedded inside,
	 * like "26.32 USD" or "$48.3" or "35.44 €"
	 * @param sColName Column Name
	 * @return com.knowgate.math.Money
	 * @throws NumberFormatException
	 */
	@Override
	public Money getMoney(String sColName) throws NumberFormatException {
		Object oVal = apply(sColName);
		if (null!=oVal)
			if (oVal.toString().length()>0)
				return Money.parse(oVal.toString());
			else
				return null;
		else
			return null;
	} // getMoney

	/**
	 * <p>Get value of column as short.</p>
	 * This function must not be used to retrieve nullable values.
	 * If the actual value of the column is not short Then this function will try to convert it to short.
	 * @param sColName String Column Name
	 * @return short
	 * @throws NullPointerException if column value is <b>null</b> or <b>None</b>
	 * @throws NumberFormatException
	 */
	@Override
	public short getShort(String sColName) throws NullPointerException,NumberFormatException {
		if (!isNull(sColName)) {
			Object oShrt = apply(sColName);
			if (oShrt instanceof Short)
				return ((Short) oShrt).shortValue();
			else if (oShrt instanceof String) {
				String sShrt = (String) oShrt;
				if (sShrt.endsWith(".0")) sShrt = sShrt.substring(0, sShrt.length()-2);
				return Short.parseShort(sShrt);
			} else {
				String sShrt = oShrt.toString();
				if (sShrt.endsWith(".0")) sShrt = sShrt.substring(0, sShrt.length()-2);
				return Short.parseShort(sShrt);
			}
		} else {
			throw new NullPointerException("Column "+sColName+" is null");
		}
	}

	/**
	 * <p>Get value of column as long.</p>
	 * This function must not be used to retrieve nullable values.
	 * If the actual value of the column is not long Then this function will try to convert it to long.
	 * @param sColName String Column Name
	 * @return long
	 * @throws NullPointerException if column value is <b>null</b> or <b>None</b>
	 * @throws NumberFormatException
	 */
	@Override
	public long getLong(String sColName) throws NullPointerException,NumberFormatException {
		if (!isNull(sColName)) {
			Object oLng = apply(sColName);
			if (oLng instanceof Long)
				return ((Long) oLng).longValue();
			else if (oLng instanceof String) {
				String sLng = (String) oLng;
				if (sLng.endsWith(".0")) sLng = sLng.substring(0, sLng.length()-2);
				return Long.parseLong(sLng);
			} else {
				String sLng = oLng.toString();
				if (sLng.endsWith(".0")) sLng = sLng.substring(0, sLng.length()-2);
				return Long.parseLong(sLng);
			}
		} else {
			throw new NullPointerException("Column "+sColName+" is null");
		}
	}

	/**
	 * <p>Get value of column as float.</p>
	 * This function must not be used to retrieve nullable values.
	 * If the actual value of the column is not float Then this function will try to convert it to float.
	 * @param sColName String Column Name
	 * @return float
	 * @throws NullPointerException if column value is <b>null</b> or <b>None</b>
	 * @throws NumberFormatException
	 */
	@Override
	public float getFloat(String sColName)
			throws NullPointerException, ClassCastException, NumberFormatException {
		Object oVal = apply(sColName);
		Class oCls;
		float fRetVal;

		if (isNullOrNone(oVal))
			throw new NullPointerException(sColName + " is null");
		
		oCls = oVal.getClass();
		try {
			if (oCls.equals(Short.TYPE))
				fRetVal = (float) ((Short) oVal).shortValue();
			else if (oCls.equals(Integer.TYPE))
				fRetVal = (float) ((Integer) oVal).intValue();
			else if (oCls.equals(Class.forName("java.math.BigDecimal")))
				fRetVal = ((java.math.BigDecimal) oVal).floatValue();
			else if (oCls.equals(Float.TYPE))
				fRetVal = ((Float) oVal).floatValue();
			else if (oCls.equals(Double.TYPE))
				fRetVal = ((Double) oVal).floatValue();
			else
				fRetVal = new Float(oVal.toString()).floatValue();
		} catch (ClassNotFoundException cnfe) { /* never thrown */ fRetVal = 0f; }
		return fRetVal;
	} // getFloat

	/**
	 * <p>Get value for a DOUBLE or NUMBER([1..28],m) column<p>
	 * This function must not be used to retrieve nullable values.
	 * If the actual value of the column is not double Then this function will try to convert it to double.
	 * @param sColName Column Name
	 * @return Column value.
	 * @throws NullPointerException if column is <b>null</b> or no column with such name was found at internal value collection.
	 * @throws NumberFormatException
	 */  
	@Override
	public double getDouble(String sColName)
			throws NullPointerException, ClassCastException, NumberFormatException {
		Object oVal = apply(sColName);
		Class oCls;
		double dRetVal;
		
		if (isNullOrNone(oVal))
			throw new NullPointerException(sColName + " is null");
		
		oCls = oVal.getClass();
		try {
			if (oCls.equals(Short.TYPE))
				dRetVal = (double) ((Short) oVal).shortValue();
			else if (oCls.equals(Integer.TYPE))
				dRetVal = (double) ((Integer) oVal).intValue();
			else if (oCls.equals(Class.forName("java.math.BigDecimal")))
				dRetVal = ((java.math.BigDecimal) oVal).doubleValue();
			else if (oCls.equals(Float.TYPE))
				dRetVal = ((Float) oVal).floatValue();
			else if (oCls.equals(Double.TYPE))
				dRetVal = ((Double) oVal).doubleValue();
			else
				dRetVal = new Double(oVal.toString()).floatValue();
		} catch (ClassNotFoundException cnfe) { /* never thrown */ dRetVal = 0d; }

		return dRetVal;
	} // getDouble

	/**
	 * <p>Get value of String column.</p>
	 * @param sColName String Column Name
	 * @return String
	 * @throws ClassCastException
	 */
	@Override
	public String getString(String sColName) throws ClassCastException {
		return (String) apply(sColName);
	}

	/**
	 * <p>Get value of String column replacing <b>null</b> with a default value.<p>
	 * @param sColName String Column Name
	 * @param sDefault String Default Value
	 * @return String
	 * @throws ClassCastException
	 */
	@Override
	public String getString(String sColName, String sDefault) throws ClassCastException {
		if (isNull(sColName))
			return sDefault;
		else
			return (String) apply(sColName);
	}

	/**
	 * <p>Get value of String column replacing <b>null</b>
	 * with a default value and replacing non-ASCII and quote values with &#<i>code</i>;<p>
	 * @param sColName Column Name
	 * @param sDefault Default value
	 * @return Field value or default value encoded as HTML numeric entities.
	 */
	@Override
	public String getStringHtml(String sColName, String sDefault)
			throws ArrayIndexOutOfBoundsException {
		String sStr = getString(sColName, sDefault);
		return Html.encode(sStr);
	} // getStringHtml

	/**
	 * @param sColName String Column Name
	 * @param bDefault boolean Default Value
	 * @return boolean
	 * @throws ClassCastException
	 */
	@Override
	public boolean getBoolean(String sColName, boolean bDefault) throws ClassCastException {
		if (isNull(sColName))
			return bDefault;
		else
			return ((Boolean) apply(sColName)).booleanValue();
	}

	/**
	 * @return <b>true</b> if Record does not contain the given key or its value is <b>null</b> or its value is an empty string ""
	 */
	@Override
	public boolean isEmpty(String sColName) {
		Object obj = apply(sColName);
		if (isNullOrNone(obj))
			return true;
		else if (obj instanceof String)
			return ((String) obj).length()==0;
		else
			return false;
	}

	/**
	 * <p>Test is a field is <b>null</b> or <b>None</b>.</p>
	 * If the sColName is a String and its value is "null" Then it will also be considered <b>null</b>
	 * @param sColName Column Name
	 * @return <b>true</b> if field is null or None or if it is a String value "null" or if no column with given name was found 
	 */
	@Override  
	public boolean isNull(String sColName) {
		Object obj = apply(sColName);
		if (isNullOrNone(obj))
			return true;
		else if (obj instanceof String)
			return ((String) obj).equalsIgnoreCase("null");
		else
			return false;
	}

	/**
	 * <p>Put boolean value at internal collection</p>
	 * @param sColName String Column Name
	 * @param bVal Field Value
	 * @return Boolean Previous column value
	 */
	@Override
	public Boolean put(String sColName, boolean bVal) {
		Object prev = put(sColName, new Boolean(bVal));
		return isNullOrNone(prev) ? null : (Boolean) prev;
	}

	/** <p>Set byte value at internal collection</p>
	 * @param sColName String Column Name
	 * @param byVal byte Field Value
	 * @return Byte Previous column value
	 */
	@Override
	public Byte put(String sColName, byte byVal) {
		Object prev = put(sColName,new Byte(byVal));
		return isNullOrNone(prev) ? null : (Byte) prev;
	}

	/** <p>Set short value at internal collection</p>
	 * @param sColName String Column Name
	 * @param iVal short Field Value
	 * @return Short Previous column value
	 */
	@Override
	public Short put(String sColName, short iVal) {
		Object prev = put(sColName, new Short(iVal));
		return isNullOrNone(prev) ? null : (Short) prev;
	}

	/** <p>Set int value at internal collection</p>
	 * @param sColName String Column Name
	 * @param iVal int Field Value
	 * @return Integer Previous column value
	 */
	@Override
	public Integer put(String sColName, int iVal) {
		Object prev = put(sColName, new Integer(iVal));
		return isNullOrNone(prev) ? null : (Integer) prev;
	}

	/** <p>Set long value at internal collection</p>
	 * @param sColName String Column Name
	 * @param lVal long Field Value
	 * @return Long Previous column value
	 */
	@Override
	public Long put(String sColName, long lVal) {
		Object prev = put(sColName, new Long(lVal));
		return isNullOrNone(prev) ? null : (Long) prev;
	}

	/** <p>Set float value at internal collection</p>
	 * @param sColName String Column Name
	 * @param fVal float Field Value
	 * @return Float Previous column value
	 */
	@Override
	public Float put(String sColName, float fVal) {
		Object prev = put(sColName, new Float(fVal));
		return isNullOrNone(prev) ? null : (Float) prev;
	}

	/** <p>Set double value at internal collection</p>
	 * @param sColName String Column Name
	 * @param dVal double Field Value
	 * @return Double Previous column value
	 */
	@Override
	public Double put(String sColName, double dVal) {
		Object prev = put(sColName, new Double(dVal));
		return isNullOrNone(prev) ? null : (Double) prev;
	}

	/**
	 * Put Date value using specified format
	 * @param sColName String Column Name
	 * @param sDate String Field Value as String
	 * @param oPattern SimpleDateFormat Date format to be used   
	 * @return Date Previous column value
	 * @throws ParseException
	 */
	@Override
	public Date put(String sKey, String sDate, SimpleDateFormat oPattern) throws ParseException {
		Object prev = put(sKey, oPattern.parse(sDate));
		return isNullOrNone(prev) ? null : (Date) prev;
	}

	/**
	 * Parse BigDecimal value and put it at internal collection
	 * @param sColName String Column Name
	 * @param sDecVal String Field Value
	 * @param oPattern DecimalFormat
	 * @return BigDecimal Previous column value
	 * @throws ParseException
	 */
	@Override
	public BigDecimal put(String sColName, String sDecVal, DecimalFormat oPattern) throws ParseException {
		Object prev = put(sColName, oPattern.parse(sDecVal));
		return isNullOrNone(prev) ? null : (BigDecimal) prev;
	}

	/**
	 * <p>Get representation of this Record as a JSON object.</p>
	 * @return String
	 */
	@Override
	public String toJSON() throws IOException {
		final StringBuilder oBF = new StringBuilder();
		JSONValue.writeJSONObject(asMap(), oBF);
		return oBF.toString();
	}
	
	/**
	 * Write Record as XML.
	 * @param identSpaces String Number of indentation spaces at the beginning of each line.
	 * @param attribs Map&lt;String,String&gt; Attributes to add to top node.
	 * @param dateFormat DateFormat Date format. If <b>null</b> then yyyy-MM-DD HH:mm:ss pattern will be used.
	 * @param decimalFormat NumberFormat Decimal format. If <b>null</b> then DecimalFormat.getNumberInstance() will be used.
	 * @param textFormat Format. Custom formatter for text fields. May be used to encode text as HTML or similar transformations.
	 * @return String
	 */
	public String toXML(String identSpaces, Map<String,String> attribs, DateFormat dateFormat, NumberFormat decimalFormat, Format textFormat) {

		String ident = identSpaces==null ? "" : identSpaces;
		final String LF = identSpaces==null ? "" : "\n";
		StringBuilder oBF = new StringBuilder(4000);
		Object oColValue;
		String sColName;
		String sStartElement = identSpaces + identSpaces + "<";
		String sEndElement = ">" + LF;

		DateFormat oXMLDate = dateFormat==null ? new SimpleDateFormat("yyyy-MM-DD HH:mm:ss") : dateFormat;
		NumberFormat oXMLDecimal = decimalFormat==null ? DecimalFormat.getNumberInstance() : decimalFormat;
		
		String nodeName = getClass().getName();
		int dot = nodeName.lastIndexOf('.');
		if (dot>0)
			nodeName = nodeName.substring(dot+1);

		if (null==attribs) {
			oBF.append(ident + "<" + nodeName + ">" + LF);
		} else {
			oBF.append(ident + "<" + nodeName);
			Iterator<String> oNames = attribs.keySet().iterator();
			while (oNames.hasNext()) {
				String sName = oNames.next();
				oBF.append(" "+sName+"=\""+attribs.get(sName)+"\"");
			} // wend
			oBF.append(">" + LF);
		} // fi

		for (ColumnDef oCol : columns()) {
			sColName = oCol.getName();
			oColValue = apply(sColName);

			oBF.append(sStartElement);
			oBF.append(sColName);
			if (null!=oColValue) {
				oBF.append(" isnull=\"false\">");
				if (oColValue instanceof String) {
					if (textFormat==null)
						oBF.append(XML.toCData((String) oColValue));
					else
						oBF.append(XML.toCData(textFormat.format((String) oColValue)));
				} else if (oColValue instanceof java.util.Date) {
					oBF.append(oXMLDate.format((java.util.Date) oColValue));
				} else if (oColValue instanceof Calendar) {
					oBF.append(oXMLDate.format((java.util.Calendar) oColValue));
				} else if (oColValue instanceof BigDecimal) {
					oBF.append(oXMLDecimal.format((BigDecimal) oColValue));
				} else {
					oBF.append(oColValue);
				}
			} else {
				oBF.append(" isnull=\"true\">");
			}
			oBF.append("</").append(sColName).append(sEndElement);
		} // wend

		oBF.append(ident).append("</").append(nodeName).append(">");

		return oBF.toString();
	} // toXML

	/**
	 * Write Record as XML.
	 * @param identSpaces String Number of indentation spaces at the beginning of each line.
	 * @param dateFormat DateFormat Date format. If <b>null</b> then yyyy-MM-DD HH:mm:ss pattern will be used.
	 * @param decimalFormat NumberFormat Decimal format. If <b>null</b> then DecimalFormat.getNumberInstance() will be used.
	 * @param textFormat Format. Custom formatter for text fields. May be used to encode text as HTML or similar transformations.
	 * @return String
	 */
	@Override
	public String toXML(String identSpaces, DateFormat dateFormat, NumberFormat decimalFormat, Format textFormat) {
		return toXML(identSpaces, null, dateFormat, decimalFormat, textFormat);
	}
	
	/**
	 * Write Record as XML.
	 * @return String
	 */
	@Override
	public String toXML() {
		return toXML("", null, null, null, null);
	}

	protected Object getBinaryData(String sKey, Object oObj) {
		Object retObj = null;
		if (oObj instanceof byte[]) {
			if (!hasLongVarBinaryData) longVarBinariesLengths = new HashMap<String,Long>();	      		   	      		    
			longVarBinariesLengths.put(sKey, new Long(((byte[])oObj).length));
			hasLongVarBinaryData = true;	      			
			retObj = oObj;
		} else {
			Class[] aInts = oObj.getClass().getInterfaces();
			if (null==aInts) {
				retObj = oObj;
			} else {
				boolean bIsSerializable = false;
				for (int i=0; i<aInts.length && !bIsSerializable; i++)
					bIsSerializable |= aInts[i].getName().equals("java.io.Serializable");
				if (bIsSerializable) {
					try {
						ByteArrayOutputStream oBOut = new ByteArrayOutputStream();
						ObjectOutputStream oOOut = new ObjectOutputStream(oBOut);
						oOOut.writeObject(oObj);
						byte[] aBytes = oBOut.toByteArray();
						if (aBytes!=null) {
							if (!hasLongVarBinaryData) longVarBinariesLengths = new HashMap<String,Long>();	      		   	      		    
							longVarBinariesLengths.put(sKey, new Long(aBytes.length));
							hasLongVarBinaryData = true;  			        	
							retObj = aBytes;
						}
						oOOut.close();
						oBOut.close();              
					} catch (IOException neverthrown) { }
				} else {
					retObj = oObj;
				}
			}
		}
		return retObj;
	}

	protected static String getColumnAlias(String columnName) {
		final int len = columnName.length()-5;
		for (int n=0; n<len; n++)
			if (columnName.charAt(n)==' ' &&
			   (columnName.charAt(n+1)=='A' || columnName.charAt(n+1)=='a') &&
			   (columnName.charAt(n+2)=='S' || columnName.charAt(n+2)=='s') &&
			    columnName.charAt(n+3)==' ')
			  return columnName.substring(n+4);
		return columnName;
	}
	
	protected boolean isNullOrNone(Object obj) {
		return obj==null || obj.getClass().getName().equals("scala.None$");
	}
}
