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
import javax.jdo.metadata.PrimaryKeyMetadata;

import java.util.Iterator;

import org.judal.metadata.TableDef;
import org.judal.metadata.ColumnDef;
import org.judal.serialization.BytesConverter;
import org.judal.storage.ConstraintsChecker;
import org.judal.storage.DataSource;
import org.judal.storage.EngineFactory;
import org.judal.storage.FieldHelper;
import org.judal.storage.table.ColumnGroup;
import org.judal.storage.table.Record;
import org.judal.storage.table.Table;
import org.judal.storage.table.TableDataSource;

import com.knowgate.currency.Money;
import com.knowgate.debug.DebugFile;
import com.knowgate.gis.LatLong;
import com.knowgate.stringutils.Html;

import static com.knowgate.typeutils.TypeResolver.ClassLangString;
import static com.knowgate.typeutils.TypeResolver.ClassSQLDate;
import static com.knowgate.typeutils.TypeResolver.ClassTimestamp;
import static com.knowgate.typeutils.TypeResolver.ClassUtilDate;
import static com.knowgate.typeutils.TypeResolver.ClassCalendar;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.text.NumberFormat;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;


/**
 * Partial implementation of Record interface.
 * The actual storage of rows is determined by a subclass.
 * It may be storing each row as an array or as a map
 * depending on whether ArrayRecord or MapRecord is used.
 * @author Sergio Montoro Ten
 * @version 1.0
 *
 */
public abstract class AbstractRecord implements Record {

	private static final long serialVersionUID = 10000l;

	protected transient TableDef tableDef;
	protected transient ConstraintsChecker checker;
	protected transient FieldHelper fhelper;
	protected boolean hasLongVarBinaryData;
	protected HashMap<String,Long> longVarBinariesLengths;

	public AbstractRecord(TableDef tableDefinition) {
		this(tableDefinition, null, null);
	}

	public AbstractRecord(TableDef tableDefinition, ConstraintsChecker constraintsChecker) {
		this(tableDefinition, null, constraintsChecker);
	}

	public AbstractRecord(TableDef tableDefinition, FieldHelper fieldHelper) {
		this(tableDefinition, fieldHelper, null);
	}

	public AbstractRecord(TableDef tableDefinition, FieldHelper fieldHelper, ConstraintsChecker constraintsChecker) {
		if (null==tableDefinition)
			throw new NullPointerException("Table definition cannot be null");
		tableDef = tableDefinition;
		setFieldHelper(fieldHelper);
		setConstraintsChecker(constraintsChecker);
		clearLongData();
	}
	
	public AbstractRecord(TableDataSource dataSource, String tableName) throws JDOException {
		this(dataSource, tableName, null, null);
	}

	public AbstractRecord(TableDataSource dataSource, String tableName, FieldHelper fieldHelper) throws JDOException {
		this(dataSource, tableName, fieldHelper, null);
	}
	
	public AbstractRecord(TableDataSource dataSource, String tableName, ConstraintsChecker constraintsChecker) throws JDOException {
		this(dataSource, tableName, null, constraintsChecker);
	}

	public AbstractRecord(TableDataSource dataSource, String tableName, FieldHelper fieldHelper, ConstraintsChecker constraintsChecker) throws JDOException {
		if (null==dataSource)
			throw new JDOUserException("No DataSource specified and no one provided by EngineFactory neither");
		tableDef = dataSource.getTableDef(tableName);
		if (null==tableDef)
			throw new JDOException("Table "+tableName+" does not exist or could not be read from the data source");
		setFieldHelper(fieldHelper);
		setConstraintsChecker(constraintsChecker);
		clearLongData();
		System.out.println("End AbstractRecord("+dataSource+", "+tableName+", "+fieldHelper+", "+constraintsChecker+")");
	}
	
	@Override
	public abstract Object apply(String columnName);

	/**
	 * This method from Map interface is implemented by subclasses of AbstractRecord
	 */
	@Override
	public abstract Object put(String sColName, Object oValue);

	@Override
	public abstract Object remove(String colname);

	@Override
	public abstract void clear();

	@Override
	public ConstraintsChecker getConstraintsChecker() {
		return checker;
	}

	public void setConstraintsChecker(ConstraintsChecker checker) {
		this.checker = checker;
	}

	public FieldHelper getFieldHelper() {
		return fhelper;
	}

	public void setFieldHelper(FieldHelper helper) {
		this.fhelper = helper;
	}

	@Override
	public Object replace(String sColName, Object oValue) {
		Object retval = apply(sColName);
		remove(sColName);
		put(sColName, oValue);
		return retval;
	}
	
	@Override
	public boolean load(DataSource oDts, Object oKey) throws JDOException {
		Table oTbl = null;
		boolean bLoaded = false;
		try {
			oTbl = ((TableDataSource) oDts).openTable(this);
			bLoaded = oTbl.load(oKey, this);
		} finally {
			if (oTbl!=null) oTbl.close();
		}
		return bLoaded;
	}

	@Override
	public final boolean load(Object oKey) throws JDOException {
		return load(EngineFactory.DefaultThreadDataSource.get());
	}

	@Override
	public void store(DataSource oDts) throws JDOException {
		if (getConstraintsChecker()!=null)
			getConstraintsChecker().check(oDts, this);
		Table oTbl = null;
		try {
			oTbl = ((TableDataSource) oDts).openTable(this);
			oTbl.store(this);
		} finally {
			if (oTbl!=null) oTbl.close();
		}
	}

	@Override
	public final void store() throws JDOException {
		store(EngineFactory.DefaultThreadDataSource.get());
	}
	
	@Override
	public void delete(DataSource oDts) throws JDOException {
		Table oTbl = ((TableDataSource) oDts).openTable(this);
		try {
			oTbl.delete(this);
		} finally {
			if (oTbl!=null) oTbl.close();
		}
	}

	@Override
	public final void delete() throws JDOException {
		delete(EngineFactory.DefaultThreadDataSource.get());
	}
	
	public TableDef getTableDef() {
		return tableDef;
	}

	public void setTableDef(TableDef tableDef) {
		this.tableDef = tableDef;
	}
	
	public void clearLongData() {
		hasLongVarBinaryData = false;
		longVarBinariesLengths = null;
	}

	public boolean hasLongData() {
		return hasLongVarBinaryData;
	}

	public Map<String,Long> longDataLengths() {
		return longVarBinariesLengths;
	}

	@Override
	public ColumnDef[] columns() {
		return tableDef.getColumns();
	}

	@Override
	public FetchGroup fetchGroup() {
		FetchGroup group = new ColumnGroup(this);
		group.addCategory(FetchGroup.DEFAULT);
		return group;
	}

	@Override
	public ColumnDef getColumn(String colname) throws ArrayIndexOutOfBoundsException {
		return tableDef.getColumnByName(colname);
	}

	@Override
	public String getBucketName() {
		return tableDef.getName();
	}

	@Override
	public String getTableName() {
		return tableDef.getName();
	}

	/**
	 * @return Value of record primary key. Will be an array of Object if the primary key is composed of multiple columns.
	 */
	@Override
	public Object getKey() throws JDOException {
		PrimaryKeyMetadata pk = tableDef.getPrimaryKeyMetadata();
		if (pk.getNumberOfColumns()==0) return null;
		if (pk.getNumberOfColumns()==1) return apply(pk.getColumn());
		Object[] retval = new Object[pk.getNumberOfColumns()];
		int c = 0;
		for (ColumnMetadata col : pk.getColumns())
			retval[c++] = apply(col.getName());
		return retval;
	}

	@Override
	/**
	 * <p>Set value of primary key for this record.</p>
	 * @param value If primary key is composed of multiple columns then must be an array of Object
	 */
	public void setKey(Object value) throws JDOException {
		PrimaryKeyMetadata pk = tableDef.getPrimaryKeyMetadata();
		if (pk.getNumberOfColumns()==0) throw new JDOException("Table "+tableDef.getName()+" has no primary key");
		if (pk.getNumberOfColumns()==1) {
			if (DebugFile.trace) DebugFile.writeln("set "+pk.getColumn()+"="+value);
			put(pk.getColumn(), value);
		} else {
			try {
				Object[] vals = (Object[]) value;
				int c = 0;
				for (ColumnMetadata col : pk.getColumns())
					put(col.getName(), vals[c++]);
			} catch (ClassCastException cce) {
				String pkcols = "";
				for (ColumnMetadata col : pk.getColumns())
					pkcols += "," + col.getName();
				throw new JDOException("AbstractRecord.setKey() Key value bust be either a basic type or and Object[] but was "+value.getClass().getName()+" for ("+pkcols.substring(1)+")", cce);
			}
		}
	}

	@Override
	/**
	 * <p>Set value of content, contentType and contentLength fields.</p>
	 * @param bytes byte[] Value of content field
	 * @param contentType String Value of contentType field
	 */
	public void setContent(byte[] bytes, String contentType) throws JDOException {
		put("content", bytes);
		put("contentType", contentType);
		if (bytes==null)
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
	@SuppressWarnings("deprecation")
	@Override
	public Calendar getCalendar(String sKey) throws ClassCastException {
		Calendar cDt = null;
		Object oDt = apply(sKey);
		if (null!=oDt) {
			if (oDt.getClass().equals(ClassUtilDate)) {
				cDt = new GregorianCalendar();
				cDt.setTime((java.util.Date) oDt);	
			}
			else if (oDt.getClass().equals(ClassTimestamp)) {
				cDt = new GregorianCalendar();
				cDt.setTimeInMillis(((java.sql.Timestamp) oDt).getTime());
			}
			else if (oDt.getClass().equals(ClassSQLDate)) {
				cDt = new GregorianCalendar();
				cDt.set(((java.sql.Date) oDt).getYear(), ((java.sql.Date) oDt).getMonth(), ((java.sql.Date) oDt).getDate());
			}
			else if (oDt.getClass().equals(ClassCalendar)) {
				cDt = (java.util.Calendar) oDt;
			}
			else if (oDt.getClass().equals(ClassLangString)) {
				try {
					cDt = new GregorianCalendar();
					cDt.setTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse((String) oDt));
				} catch (java.text.ParseException pe) {
					throw new ClassCastException("Cannot parse Date " + oDt);
				}
			}
		}
		return cDt;
	} // getCalendar

	/**
	 * <p>Get value for a DATETIME field<p>
	 * @param sKey Field Name
	 * @return Date value or <b>null</b>.
	 * @throws ClassCastException if sKey field is not of type DATETIME
	 */
	@SuppressWarnings("deprecation")
	@Override
	public Date getDate(String sKey) throws ClassCastException {
		Date dDt = null;
		Object oDt = apply(sKey);
		if (null!=oDt) {
			if (oDt.getClass().equals(ClassUtilDate))
				dDt = (java.util.Date) oDt;
			else if (oDt.getClass().equals(ClassTimestamp))
				dDt = new java.util.Date(((java.sql.Timestamp) oDt).getTime());
			else if (oDt.getClass().equals(ClassSQLDate))
				dDt = new java.util.Date(((java.sql.Date) oDt).getYear(), ((java.sql.Date) oDt).getMonth(), ((java.sql.Date) oDt).getDate());
			else if (oDt.getClass().equals(ClassCalendar))
				dDt = ((java.util.Calendar) oDt).getTime();
			else if (oDt.getClass().equals(ClassLangString)) {
				try {
					dDt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse((String) oDt);    	  
				} catch (java.text.ParseException pe) {
					throw new ClassCastException("Cannot parse Date " + oDt);
				}
			}
		}
		return dDt;
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
	public String getDateShort(String sColName) throws ClassCastException {
		Date dDt = getDate(sColName);
		if (null!=dDt) {
			int y = dDt.getYear()+1900, m=dDt.getMonth()+1, d=dDt.getDate();
			return String.valueOf(y)+"-"+(m<10 ? "0" : "")+String.valueOf(m)+"-"+(d<10 ? "0" : "")+String.valueOf(d);
		}
		else
			return null;
	} // getDateShort

	@Override
	public int getIntervalPart(String sColName, String sPart) throws ClassCastException, ClassNotFoundException, NullPointerException, NumberFormatException, IllegalArgumentException {
		if (null==fhelper)
			throw new ClassNotFoundException("No FieldHelper class has been specified for " + getTableName());
		return fhelper.getIntervalPart(this, sColName, sPart);
	}

	@Override
	public LatLong getLatLong(String sColName) throws ClassCastException, ClassNotFoundException, NumberFormatException, ArrayIndexOutOfBoundsException {
		if (null==fhelper)
			throw new ClassNotFoundException("No FieldHelper class has been specified for " + getTableName());
		return fhelper.getLatLong(this, sColName);
	}
	
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
	 * @param Locale
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
	 * @param sColName Field Name
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
	 * @param sColName Field Name
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

	@Override
	public Object getMap(String sColName) throws ClassCastException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, ClassNotFoundException {
		if (null==fhelper)
			throw new ClassNotFoundException("No FieldHelper class has been specified for " + getTableName());
		return fhelper.getMap(this, sColName);
	}
	
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

	@Override
	public Integer[] getIntegerArray(String sColName) throws ClassCastException, ClassNotFoundException {
		if (null==fhelper)
			throw new ClassNotFoundException("No FieldHelper class has been specified for " + getTableName());
		return fhelper.getIntegerArray(this, sColName);
	}

	@Override
	public Long[] getLongArray(String sColName) throws ClassCastException, ClassNotFoundException {
		if (null==fhelper)
			throw new ClassNotFoundException("No FieldHelper class has been specified for " + getTableName());
		return fhelper.getLongArray(this, sColName);
	}

	@Override
	public Float[] getFloatArray(String sColName) throws ClassCastException, ClassNotFoundException {
		if (null==fhelper)
			throw new ClassNotFoundException("No FieldHelper class has been specified for " + getTableName());
		return fhelper.getFloatArray(this, sColName);
	}

	@Override
	public Double[] getDoubleArray(String sColName) throws ClassCastException, ClassNotFoundException {
		if (null==fhelper)
			throw new ClassNotFoundException("No FieldHelper class has been specified for " + getTableName());
		return fhelper.getDoubleArray(this, sColName);
	}

	@Override
	public Date[] getDateArray(String sColName) throws ClassCastException, ClassNotFoundException {
		if (null==fhelper)
			throw new ClassNotFoundException("No FieldHelper class has been specified for " + getTableName());
		return fhelper.getDateArray(this, sColName);
	}
	
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

	@Override
	public float getFloat(String sColName)
			throws NullPointerException, ClassCastException, NumberFormatException {
		Object oVal = apply(sColName);
		Class oCls;
		float fRetVal;
		if (oVal==null) throw new NullPointerException(sColName + " is null");
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
	 * @param sColName Column Name
	 * @return Column value.
	 * @throws NullPointerException if column is <b>null</b> or no column with
	 * such name was found at internal value collection.
	 * @throws NumberFormatException
	 */  
	@Override
	public double getDouble(String sColName)
			throws NullPointerException, ClassCastException, NumberFormatException {
		Object oVal = apply(sColName);
		Class oCls;
		double dRetVal;
		if (oVal==null) throw new NullPointerException(sColName + " is null");
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

	@Override
	public String getString(String sColName) throws ClassCastException {
		return (String) apply(sColName);
	}

	@Override
	public String getString(String sColName, String sDefault) throws ClassCastException {
		if (isNull(sColName))
			return sDefault;
		else
			return (String) apply(sColName);
	}


	/**
	 * <p>Get value for a CHAR, VARCHAR or LONGVARCHAR field replacing <b>null</b>
	 * with a default value and replacing non-ASCII and quote values with &#<i>code</i>;<p>
	 * @param sColName Column Name
	 * @param iRow Row position [0..getRowCount()-1]
	 * @param sDefault Default value
	 * @return Field value or default value encoded as HTML numeric entities.
	 */

	@Override
	public String getStringHtml(String sColName, String sDefault)
			throws ArrayIndexOutOfBoundsException {
		String sStr = getString(sColName, sDefault);
		return Html.encode(sStr);
	} // getStringHtml

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
		if (obj==null)
			return true;
		else if (obj instanceof String)
			return ((String) obj).length()==0;
		else
			return false;
	}

	/**
	 * <p>Test is a field is null.</p>
	 * @param sColName Column Name
	 * @return <b>true</b> if field is null or if it is a String value "null" or if no column with given name was found 
	 */
	@Override  
	public boolean isNull(String sColName) {
		Object obj = apply(sColName);
		if (obj==null)
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
	 */
	@Override
	public Boolean put(String sColName, boolean bVal) {
		return (Boolean) put(sColName, new Boolean(bVal));
	}

	/** <p>Set byte value at internal collection</p>
	 * @param sColName String Column Name
	 * @param byVal byte Field Value
	 */
	@Override
	public Byte put(String sColName, byte byVal) {
		return (Byte) put(sColName,new Byte(byVal));
	}

	/** <p>Set short value at internal collection</p>
	 * @param sColName String Column Name
	 * @param iVal short Field Value
	 */
	@Override
	public Short put(String sColName, short iVal) {
		return (Short) put(sColName, new Short(iVal));
	}

	/** <p>Set int value at internal collection</p>
	 * @param sColName String Column Name
	 * @param iVal int Field Value
	 */
	@Override
	public Integer put(String sColName, int iVal) {
		return (Integer) put(sColName, new Integer(iVal));
	}

	/** <p>Set long value at internal collection</p>
	 * @param sColName String Column Name
	 * @param lVal long Field Value
	 */
	@Override
	public Long put(String sColName, long lVal) {
		return (Long) put(sColName, new Long(lVal));
	}

	/** <p>Set float value at internal collection</p>
	 * @param sColName String Column Name
	 * @param fVal float Field Value
	 */
	@Override
	public Float put(String sColName, float fVal) {
		return (Float) put(sColName, new Float(fVal));
	}

	/** <p>Set double value at internal collection</p>
	 * @param sColName String Column Name
	 * @param dVal double Field Value
	 */
	@Override
	public Double put(String sColName, double dVal) {
		return (Double) put(sColName, new Double(dVal));
	}

	/**
	 * Put Date value using specified format
	 * @param sColName String Column Name
	 * @param sDate String Field Value as String
	 * @param oPattern SimpleDateFormat Date format to be used   
	 * @throws ParseException
	 */
	@Override
	public Date put(String sKey, String sDate, SimpleDateFormat oPattern) throws ParseException {
		return (Date) put(sKey, oPattern.parse(sDate));
	}

	/**
	 * Parse BigDecimal value and put it at internal collection
	 * @param sColName String Column Name
	 * @param sDecVal String Field Value
	 * @param oPattern DecimalFormat
	 * @throws ParseException
	 */
	@Override
	public BigDecimal put(String sColName, String sDecVal, DecimalFormat oPattern) throws ParseException {
		return (BigDecimal) put(sColName, oPattern.parse(sDecVal));
	}

	public String toXML(String sIdent, Map<String,String> oAttrs, Locale oLoc) {
		if (oLoc.getLanguage().equals("es"))
			return toXML(sIdent, oAttrs, new SimpleDateFormat("DD/MM/yyyy HH:mm:ss"),
					DecimalFormat.getNumberInstance(oLoc));
		else
			return toXML(sIdent, oAttrs, new SimpleDateFormat("yyyy-MM-DD HH:mm:ss"),
					DecimalFormat.getNumberInstance());
	}

	public String toXML(String sIdent, Map<String,String> oAttrs, SimpleDateFormat oXMLDate, NumberFormat oXMLDecimal) {

		final String LF = "\n";
		StringBuffer oBF = new StringBuffer(4000);
		Object oColValue;
		String sColName;
		String sStartElement = sIdent + sIdent + "<";
		String sEndElement = ">" + LF;
		Class oColClass, ClassString = null, ClassDate = null, ClassDecimal = null;

		try {
			ClassString = Class.forName("java.lang.String");
			ClassDate = Class.forName("java.util.Date");
			ClassDecimal = Class.forName("java.math.BigDecimal");
		} catch (ClassNotFoundException ignore) { }

		if (null==oAttrs) {
			oBF.append(sIdent + "<" + getClass().getName() + ">" + LF);
		} else {
			oBF.append(sIdent + "<" + getClass().getName());
			Iterator<String> oNames = oAttrs.keySet().iterator();
			while (oNames.hasNext()) {
				String sName = oNames.next();
				oBF.append(" "+sName+"=\""+oAttrs.get(sName)+"\"");
			} // wend
			oBF.append(">" + LF);
		} // fi

		for (ColumnDef oCol : columns()) {
			sColName = oCol.getName();
			oColValue = apply(sColName);

			oBF.append(sStartElement);
			oBF.append(sColName);
			oBF.append(">");
			if (null!=oColValue) {
				oColClass = oColValue.getClass();
				if (oColClass.equals(ClassString))
					oBF.append("<![CDATA[" + oColValue + "]]>");
				else if (oColClass.equals(ClassDate))
					oBF.append(oXMLDate.format((java.util.Date) oColValue));
				else if (oColClass.equals(ClassDecimal))
					oBF.append(oXMLDecimal.format((java.math.BigDecimal) oColValue));
				else
					oBF.append(oColValue);
			}
			oBF.append("</");
			oBF.append(sColName);
			oBF.append(sEndElement);
		} // wend

		oBF.append(sIdent + "</" + getClass().getName() + ">");

		return oBF.toString();
	} // toXML

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
	
}
