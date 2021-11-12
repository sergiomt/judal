package org.judal.storage.table.impl;

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

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

import javax.jdo.FetchGroup;
import javax.jdo.JDOException;

import org.judal.metadata.ColumnDef;
import org.judal.storage.ConstraintsChecker;
import org.judal.storage.DataSource;
import org.judal.storage.EngineFactory;
import org.judal.storage.FieldHelper;
import org.judal.storage.table.ColumnGroup;
import org.judal.storage.table.SingleColumnRecord;
import org.judal.storage.table.Table;
import org.judal.storage.table.TableDataSource;

import com.knowgate.currency.Money;
import com.knowgate.gis.LatLong;

/**
 * Base class for Record implementations that hold only a single column
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public abstract class AbstractSingleColumnRecord implements SingleColumnRecord {

	private static final long serialVersionUID = 1L;

	protected Object value;
	protected String columnName;
	protected String tableName;
	private final FieldHelper fhelper;

	public AbstractSingleColumnRecord() {
		this(null,"value");
	}

	public AbstractSingleColumnRecord(String tableName) {
		this(tableName, "value");
		this.columnName = "value";
	}

	public AbstractSingleColumnRecord(String tableName, String columnName) {
		this(tableName,columnName,null);
	}

	public AbstractSingleColumnRecord(String tableName, String columnName, FieldHelper fhelper) {
		this.tableName = tableName;
		this.columnName = columnName;
		this.fhelper = fhelper;
	}

	@Override
	public ColumnDef[] columns() {
		return new ColumnDef[]{getColumn(columnName)};
	}
	
	@Override
	public boolean isNull(String colname) {
		return getValue()== null;
	}

	@Override
	public void clear() {
		setValue(null);
	}

	@Override
	public FieldHelper getFieldHelper() throws ArrayIndexOutOfBoundsException {
		return fhelper;
	}

	@Override
	public String getTableName() {
		return tableName;
	}

	@Override
	public FetchGroup fetchGroup() {
		return new ColumnGroup(columnName);
	}

	@Override
	public ConstraintsChecker getConstraintsChecker() {
		return null;
	}

	@Override
	public BigDecimal getDecimal(String colname)
			throws NullPointerException, ClassCastException, NumberFormatException {
		throw new ClassCastException("Cannot cast from Object to BigDecimal");
	}

	@Override
	public String getDecimalFormated(String sKey, String sPattern)
			throws ClassCastException, NumberFormatException, NullPointerException, IllegalArgumentException {
		throw new ClassCastException("Cannot cast from Object to BigDecimal");
	}

	@Override
	public String getDecimalFormated(String sKey, Locale oLoc, int nFractionDigits)
			throws ClassCastException, NumberFormatException, NullPointerException, IllegalArgumentException {
		throw new ClassCastException("Cannot cast from Object to BigDecimal");
	}

	@Override
	public byte[] getBytes(String colname) throws ClassCastException {
		throw new ClassCastException("Cannot cast from Object to byte array");
	}

	@Override
	public String getFloatFormated(String colname, String pattern)
			throws ClassCastException, NumberFormatException, NullPointerException, IllegalArgumentException {
		throw new ClassCastException("Cannot cast from Object to Float");
	}

	@Override
	public Integer getInteger(String colname) throws NumberFormatException {
		throw new ClassCastException("Cannot cast from Object to Integer");
	}

	@Override
	public int getInt(String colname) throws NullPointerException, ClassCastException, NumberFormatException {
		throw new ClassCastException("Cannot cast from Object to Int");
	}

	@Override
	public long getLong(String colname) throws NullPointerException, NumberFormatException {
		throw new ClassCastException("Cannot cast from Object to Long");
	}

	@Override
	public Money getMoney(String colname) throws NumberFormatException {
		throw new ClassCastException("Cannot cast from Object to Money");
	}

	@Override
	public short getShort(String colname) throws NullPointerException, NumberFormatException {
		throw new ClassCastException("Cannot cast from Object to Short");
	}

	@Override
	public float getFloat(String colname) throws NullPointerException, ClassCastException, NumberFormatException {
		throw new ClassCastException("Cannot cast from Object to Float");
	}

	@Override
	public double getDouble(String colname) throws NullPointerException, ClassCastException, NumberFormatException {
		throw new ClassCastException("Cannot cast from Object to Double");
	}

	@Override
	public String getDoubleFormated(String colname, String sPattern)
			throws ClassCastException, NumberFormatException, NullPointerException, IllegalArgumentException {
		throw new ClassCastException("Cannot cast from Object to Double");
	}

	@Override
	public Calendar getCalendar(String colname) {
		throw new ClassCastException("Cannot cast from Object to Calendar");
	}

	@Override
	public Date getDate(String colname) {
		throw new ClassCastException("Cannot cast from Object to Date");
	}

	@Override
	public LocalDate getLocalDate(String colname) {
		throw new ClassCastException("Cannot cast from Object to LocalDate");
	}

	@Override
	public LocalDateTime getLocalDateTime(String colname) {
		throw new ClassCastException("Cannot cast from Object to LocalDateTime");
	}

	@Override
	public Date getDate(String colname, Date defvalue) {
		throw new ClassCastException("Cannot cast from Object to Date");
	}

	@Override
	public String getDateFormated(String colname, String format) throws ClassCastException {
		throw new ClassCastException("Cannot cast from Object to Date");
	}

	@Override
	public String getDateTime(String colname) throws ClassCastException {
		throw new ClassCastException("Cannot cast from Object to Date");
	}

	@Override
	public String getDateShort(String colname) throws ClassCastException {
		throw new ClassCastException("Cannot cast from Object to Date");
	}

	@Override
	public int getIntervalPart(String colname, String part) throws ClassCastException, ClassNotFoundException,
			NullPointerException, NumberFormatException, IllegalArgumentException {
		throw new ClassCastException("Cannot cast from Object to Interval");
	}

	@Override
	public Integer[] getIntegerArray(String colname) throws ClassCastException, ClassNotFoundException {
		throw new ClassCastException("Cannot cast from Object to Integer array");
	}

	@Override
	public Long[] getLongArray(String colname) throws ClassCastException, ClassNotFoundException {
		throw new ClassCastException("Cannot cast from Object to Long array");
	}

	@Override
	public Float[] getFloatArray(String colname) throws ClassCastException, ClassNotFoundException {
		throw new ClassCastException("Cannot cast from Object to Float array");
	}

	@Override
	public Double[] getDoubleArray(String colname) throws ClassCastException, ClassNotFoundException {
		throw new ClassCastException("Cannot cast from Object to Double array");
	}

	@Override
	public Date[] getDateArray(String colname) throws ClassCastException, ClassNotFoundException {
		throw new ClassCastException("Cannot cast from Object to Date array");
	}

	@Override
	public LatLong getLatLong(String columname)
			throws ClassCastException, NumberFormatException, ArrayIndexOutOfBoundsException, ClassNotFoundException {
		throw new ClassCastException("Cannot cast from Object to LatLong");
	}

	@Override
	public String getString(String colname) throws ClassCastException {
		throw new ClassCastException("Cannot cast from Object to String");
	}

	@Override
	public String getString(String colname, String defvalue) throws ClassCastException {
		throw new ClassCastException("Cannot cast from Object to String");
	}

	@Override
	public String[] getStringArray(String colname) throws ClassCastException, ClassNotFoundException {
		throw new ClassCastException("Cannot cast from Object to String array");
	}

	@Override
	public String getStringHtml(String colname, String defvalue) {
		throw new ClassCastException("Cannot cast from Object to String");
	}

	@Override
	public boolean getBoolean(String colname, boolean defvalue) throws ClassCastException {
		throw new ClassCastException("Cannot cast from Object to Boolean");
	}

	@Override
	public Object getMap(String colname)
	throws ClassCastException, InstantiationException, IllegalAccessException, IllegalArgumentException,
			NoSuchMethodException, SecurityException, ClassNotFoundException {
		throw new ClassCastException("Cannot cast from Object to Map");
	}

	@Override
	public Object put(int colpos, Object obj) throws IllegalArgumentException, ArrayIndexOutOfBoundsException {
		if (colpos!=1)
			throw new ArrayIndexOutOfBoundsException("Column index must be 1 for single value records");
		Object retval = value;
		value = obj;
		return retval;
	}

	@Override
	public Object put(String colname, Object obj) throws IllegalArgumentException {
		Object retval = value;
		columnName = colname;
		value = obj;
		return retval;
	}

	@Override
	public Object replace(String colname, Object obj) throws IllegalArgumentException {
		return put(colname, obj);
	}

	@Override
	public Boolean put(String colname, boolean boolval) throws IllegalArgumentException {
		throw new ClassCastException("Cannot cast from boolean to Object");
	}

	@Override
	public Object put(String colname, byte[] bytearray) throws IllegalArgumentException {
		throw new ClassCastException("Cannot cast from byte array to Object");
	}

	@Override
	public Byte put(String colname, byte bytevalue) throws IllegalArgumentException {
		throw new ClassCastException("Cannot cast from byte to Object");
	}

	@Override
	public Short put(String colname, short shortvalue) throws IllegalArgumentException {
		throw new ClassCastException("Cannot cast from short to Object");
	}

	@Override
	public Integer put(String colname, int intvalue) throws IllegalArgumentException {
		throw new ClassCastException("Cannot cast from int to Object");
	}

	@Override
	public Long put(String colname, long longvalue) throws IllegalArgumentException {
		throw new ClassCastException("Cannot cast from long to Object");
	}

	@Override
	public Float put(String colname, float floatvalue) throws IllegalArgumentException {
		throw new ClassCastException("Cannot cast from float to Object");
	}

	@Override
	public Double put(String colname, double doublevalue) throws IllegalArgumentException {
		throw new ClassCastException("Cannot cast from double to Object");
	}

	@Override
	public Date put(String colname, String strdate, SimpleDateFormat pattern) throws ParseException, IllegalArgumentException {
		throw new ClassCastException("Cannot cast from Date to Object");
	}

	@Override
	public BigDecimal put(String colname, String decimalvalue, DecimalFormat pattern) throws ParseException, IllegalArgumentException {
		throw new ClassCastException("Cannot cast from decimal to Object");
	}

	@Override
	public Character put(String colname, char charvalue) throws IllegalArgumentException {
		throw new ClassCastException("Cannot cast from char to Object");
	}

	@Override
	public Object remove(String colname) {
		Object retval = value;
		setValue(null);
		return retval;
	}

	@Override
	public int size() {
		return 1;
	}

	@Override
	public void setKey(Object key) throws JDOException {
		value = key;
	}

	@Override
	public Object getKey() throws JDOException {
		return getValue();
	}

	@Override
	public void setContent(byte[] bytes, String contentType) throws JDOException {
		throw new ClassCastException("Cannot cast from byte array to Object");
	}

	@Override
	public Object getValue() throws JDOException {
		return value;
	}

	@Override
	public void setValue(Serializable newvalue) throws JDOException {
		value = newvalue;
	}
	
	@Override
	public String getBucketName() {
		return tableName;
	}

	@Override
	public boolean load(Object key) throws JDOException {
		return load(EngineFactory.DefaultThreadDataSource.get());
	}

	@Override
	public boolean load(DataSource dataSource, Object key) throws JDOException {
		Table oTbl = null;
		boolean bLoaded = false;
		try {
			oTbl = ((TableDataSource) dataSource).openTable(this);
			bLoaded = oTbl.load(key, this);
		} finally {
			if (oTbl!=null) oTbl.close();
		}
		return bLoaded;
	}

	@Override
	public void store() throws JDOException {
		load(EngineFactory.DefaultThreadDataSource.get());
	}

	@Override
	public void store(DataSource dataSource) throws JDOException {
		if (getConstraintsChecker()!=null)
			getConstraintsChecker().check(dataSource, this);
		Table oTbl = null;
		try {
			oTbl = ((TableDataSource) dataSource).openTable(this);
			oTbl.store(this);
		} finally {
			if (oTbl!=null) oTbl.close();
		}
	}

	@Override
	public void delete(DataSource dataSource) throws JDOException {
		Table oTbl = ((TableDataSource) dataSource).openTable(this);
		try {
			oTbl.delete(this);
		} finally {
			if (oTbl!=null) oTbl.close();
		}
	}

	@Override
	public void delete() throws JDOException {
		delete(EngineFactory.DefaultThreadDataSource.get());
	}

}
