package org.judal.storage.table;

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

import java.io.IOException;
import java.io.Serializable;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.Format;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import javax.jdo.FetchGroup;

import org.judal.metadata.ColumnDef;
import org.judal.storage.ConstraintsChecker;
import org.judal.storage.FieldHelper;
import org.judal.storage.keyvalue.Stored;

/**
 * <p>Record interface.</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public interface Record extends Serializable, Stored {

	/**
	 * <p>Columns of the Record as given by a ViewDef, TableDef or FetchGroup.</p>
	 * records of subclass MapRecord may contain more columns than the ones returned
	 * by calling this method in order to accommodate growing column families in column-oriented databases.
	 * @return ColumnDef[]
	 */
	ColumnDef[] columns();

	/**
	 * <p>Get column by name.</p>
	 * @param colname  String
	 * @return ColumnDef
	 * @throws ArrayIndexOutOfBoundsException If no column with such name is found
	 */
	ColumnDef getColumn(String colname) throws ArrayIndexOutOfBoundsException;

	/**
	 * <p>Get Record column values as a Java Map.</p>
	 * @return Map&lt;String,Object&gt;
	 */
	Map<String,Object> asMap();

	/**
	 * <p>Get Record column values as an array of Entry instances.</p>
	 * @return Entry&lt;String,Object&gt;[]
	 */
	Entry<String,Object>[] asEntries();

	boolean isNull(String colname);

	/**
	 * @return <b>true</b> if Record does not contain the given key or its value is <b>null</b> or its value is an empty string ""
	 */
	boolean isEmpty(String colname);

	/**
	 * <p>Clear column values.</p>
	 */
	void clear();
	
	/**
	 * <p>Get name of underlying table or view used by this Record.</p>
	 * @return String
	 */
	String getTableName();

	/**
	 * <p>Get list of columns that must be fetched for this Record.</p>
	 * @return FetchGroup
	 */
	FetchGroup fetchGroup();

	/**
	 * <p>Get instance of ConstraintsChecker applicable to this Record  (if any).</p>
	 * @return ConstraintsChecker
	 */
	ConstraintsChecker getConstraintsChecker();

	/**
	 * <p>Get instance of FieldHelper applicable to this Record  (if any).</p>
	 * @return FieldHelper
	 */
	FieldHelper getFieldHelper();

	/**
	 * <p>Get value of a column.</p>
	 * @param colname String Column Name
	 * @return Object
	 */
	Object apply(String colname);
	
	/**
	 * <p>Get value of a decimal column.</p>
	 * @param colname String Column Name
	 * @return BigDecimal
	 * @throws NullPointerException
	 * @throws ClassCastException
	 * @throws NumberFormatException
	 */
	BigDecimal getDecimal(String colname) throws NullPointerException, ClassCastException, NumberFormatException;

	/**
	 * <p>Get value of a decimal column formatted with a given pattern.</p>
	 * @param colname String Column Name
	 * @param numberPattern String Format Pattern
	 * @return String
	 * @throws NullPointerException
	 * @throws ClassCastException
	 * @throws NumberFormatException
	 */
	String getDecimalFormated(String colname, String numberPattern) throws ClassCastException, NumberFormatException, NullPointerException, IllegalArgumentException;

	/**
	 * <p>Get value of a decimal column formatted with a given Locale and fraction digits.</p>
	 * @param colname String Column Name
	 * @param loc Locale
	 * @param nFractionDigits int Number of digits to the right of the decimal point
	 * @return String
	 * @throws NullPointerException
	 * @throws ClassCastException
	 * @throws NumberFormatException
	 */
	String getDecimalFormated(String colname, Locale loc, int nFractionDigits) throws ClassCastException, NumberFormatException,NullPointerException, IllegalArgumentException;

	/**
	 * <p>Get value of a byte array column.</p>
	 * @param colname String
	 * @return byte[]
	 * @throws ClassCastException
	 */
	byte[] getBytes(String colname) throws ClassCastException;

	/**
	 * <p>Get value of a float column formatted with a given pattern.</p>
	 * @param colname String
	 * @param pattern String Number format pattern
	 * @return String
	 * @throws ClassCastException
	 * @throws NumberFormatException
	 * @throws NullPointerException
	 * @throws IllegalArgumentException
	 */
	String getFloatFormated(String colname, String pattern) throws ClassCastException, NumberFormatException, NullPointerException, IllegalArgumentException;

	/**
	 * <p>Get value of an int column.</p>
	 * @param colname String
	 * @return Integer or <b>null</b>
	 * @throws NumberFormatException
	 */
	Integer getInteger(String colname) throws NumberFormatException;

	/**
	 * <p>Get value of an int column.</p>
	 * @param colname String
	 * @return int
	 * @throws NullPointerException if column value is <b>null</b>
	 * @throws ClassCastException
	 * @throws NumberFormatException
	 */
	int getInt(String colname) throws NullPointerException,ClassCastException,NumberFormatException;

	/**
	 * <p>Get value of a long column.</p>
	 * @param colname String
	 * @return long
	 * @throws NullPointerException if column value is <b>null</b>
	 * @throws ClassCastException
	 * @throws NumberFormatException
	 */
	long getLong(String colname) throws NullPointerException,ClassCastException,NumberFormatException;

	/**
	 * <p>Get value of a column containing a decimal value with currency symbol.</p>
	 * @param colname String
	 * @return Money
	 * @throws NumberFormatException
	 */
	Object getMoney(String colname) throws NumberFormatException;
	
	/**
	 * <p>Get value of a short column.</p>
	 * @param colname String
	 * @return short
	 * @throws NullPointerException if column value is <b>null</b>
	 * @throws ClassCastException
	 * @throws NumberFormatException
	 */
	short getShort(String colname) throws NullPointerException,NumberFormatException;

	/**
	 * <p>Get value of a float column.</p>
	 * @param colname String
	 * @return float
	 * @throws NullPointerException if column value is <b>null</b>
	 * @throws ClassCastException
	 * @throws NumberFormatException
	 */
	float getFloat(String colname) throws NullPointerException, ClassCastException, NumberFormatException;

	/**
	 * <p>Get value of a double column.</p>
	 * @param colname String
	 * @return double
	 * @throws NullPointerException if column value is <b>null</b>
	 * @throws ClassCastException
	 * @throws NumberFormatException
	 */
	double getDouble(String colname) throws NullPointerException, ClassCastException, NumberFormatException;

	/**
	 * <p>Get value of a double column formatted with a given pattern.</p>
	 * @param colname String
	 * @return double
	 * @throws ClassCastException
	 * @throws NullPointerException
	 * @throws NumberFormatException
	 * @throws IllegalArgumentException
	 */
	String getDoubleFormated(String colname, String sPattern) throws ClassCastException,NumberFormatException,NullPointerException,NumberFormatException,IllegalArgumentException;

	/**
	 * <p>Get value of a Timestamp or Date column.</p>
	 * @param colname String
	 * @return Calendar
	 * @throws ClassCastException
	 */
	Calendar getCalendar(String colname) throws ClassCastException;

	/**
	 * <p>Get value of a timestamp or date column.</p>
	 * @param colname String
	 * @return Date or <b>null</b>
	 * @throws ClassCastException
	 */
	Date getDate(String colname);

	/**
	 * <p>Get value of a Timestamp or Date column.</p>
	 * @param colname String
	 * @param defValue Date Default value
	 * @return Date or default value
	 * @throws ClassCastException
	 */
	Date getDate(String colname, Date defValue) throws ClassCastException;

	/**
	 * <p>Get value of date column.</p>
	 * @param colname String
	 * @return LocalDate or <b>null</b>
	 * @throws ClassCastException
	 */
	LocalDate getLocalDate(String colname);

	/**
	 * <p>Get value of timestamp column.</p>
	 * @param colname String
	 * @return LocalDateTime or <b>null</b>
	 * @throws ClassCastException
	 */
	LocalDateTime getLocalDateTime(String colname);

	/**
	 * <p>Get value of a timestamp or date column formatted with a given pattern.</p>
	 * @param colname String
	 * @param format String
	 * @return String
	 * @throws ClassCastException
	 */
	String getDateFormated(String colname, String format) throws ClassCastException;

	/**
	 * <p>Get value of a Timestamp or Date column formatted as yyyy-MM-dd HH:mm:ss</p>
	 * @param colname String
	 * @return String
	 * @throws ClassCastException
	 */
	String getDateTime(String colname) throws ClassCastException;

	/**
	 * <p>Get value of a Timestamp or Date column formatted as yyyy-MM-dd</p>
	 * @param colname String
	 * @return String
	 * @throws ClassCastException
	 */
	String getDateShort(String colname) throws ClassCastException;

	/**
	 * <p>Get value of a date part</p>
	 * @param colname String
	 * @param part String Part name. Allowed values are defined by the implementation.
	 * @return int
	 * @throws ClassCastException
	 * @throws ClassNotFoundException
	 * @throws NullPointerException
	 * @throws NumberFormatException
	 * @throws IllegalArgumentException
	 */
	int getIntervalPart(String colname, String part) throws ClassCastException, ClassNotFoundException, NullPointerException, NumberFormatException, IllegalArgumentException;
	
	/**
	 * <p>Get value of int array column.</p>
	 * @param colname String
	 * @return Integer[]
	 * @throws ClassCastException
	 * @throws ClassNotFoundException
	 */
	Integer[] getIntegerArray(String colname) throws ClassCastException, ClassNotFoundException;

	/**
	 * <p>Get value of long array column.</p>
	 * @param colname String
	 * @return Long[]
	 * @throws ClassCastException
	 * @throws ClassNotFoundException
	 */
	Long[] getLongArray(String colname) throws ClassCastException, ClassNotFoundException;

	/**
	 * <p>Get value of float array column.</p>
	 * @param colname String
	 * @return Float[]
	 * @throws ClassCastException
	 * @throws ClassNotFoundException
	 */
	Float[] getFloatArray(String colname) throws ClassCastException, ClassNotFoundException;

	/**
	 * <p>Get value of double array column.</p>
	 * @param colname String
	 * @return Double[]
	 * @throws ClassCastException
	 * @throws ClassNotFoundException
	 */
	Double[] getDoubleArray(String colname) throws ClassCastException, ClassNotFoundException;

	/**
	 * <p>Get value of Date array column.</p>
	 * @param colname String
	 * @return Date[]
	 * @throws ClassCastException
	 * @throws ClassNotFoundException
	 */
	Date[] getDateArray(String colname) throws ClassCastException, ClassNotFoundException;
	
	/**
	 * <p>Get value of geography coordinates column.</p>
	 * @param colname String
	 * @return LatLong
	 * @throws ClassCastException
	 * @throws ClassNotFoundException
	 */
	Object getLatLong(String colname) throws ClassCastException, NumberFormatException, ArrayIndexOutOfBoundsException, ClassNotFoundException;
	
	/**
	 * <p>Get value of string column.</p>
	 * @param colname String
	 * @return String or <b>null</b>
	 * @throws ClassCastException
	 */
	String getString(String colname) throws ClassCastException;

	/**
	 * <p>Get value of string column.</p>
	 * @param colname String
	 * @param defvalue String Default value
	 * @return String
	 * @throws ClassCastException
	 */
	String getString(String colname, String defvalue) throws ClassCastException;

	/**
	 * <p>Get value of String array column.</p>
	 * @param colname String
	 * @return String[]
	 * @throws ClassCastException
	 * @throws ClassNotFoundException
	 */
	String[] getStringArray(String colname) throws ClassCastException, ClassNotFoundException;
	
	/**
	 * <p>Get value of string column as HTML.</p>
	 * @param colname String
	 * @param defvalue String Default value
	 * @return String
	 */
	String getStringHtml(String colname, String defvalue);

	/**
	 * <p>Get value of boolean column.</p>
	 * @param colname String
	 * @param defvalue boolean Default value
	 * @return boolean
	 * @throws ClassCastException
	 */
	boolean getBoolean(String colname, boolean defvalue) throws ClassCastException;

	/**
	 * <p>Get value of column containing a Map of key-value pairs.</p>
	 * @param colname String
	 * @return Object which type depends on whether the implementation uses Java or Scala adaptor.
	 * @throws ClassCastException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws IllegalArgumentException
	 * @throws NoSuchMethodException
	 * @throws SecurityException
	 * @throws ClassNotFoundException
	 */
	Object getMap(String colname) throws ClassCastException, InstantiationException, IllegalAccessException, IllegalArgumentException, NoSuchMethodException, SecurityException, ClassNotFoundException;
	
	/**
	 * <p>Set column value.</p>
	 * @param colpos int Column position [1..columnCount()]
	 * @param obj Object Column value
	 * @return Previous column value
	 * @throws ArrayIndexOutOfBoundsException
	 */
	Object put(int colpos, Object obj) throws IllegalArgumentException, ArrayIndexOutOfBoundsException;

	/**
	 * <p>Set column value.</p>
	 * @param colname String Case insensitive column name
	 * @param obj Object Column value
	 * @return Previous column value
	 * @throws IllegalArgumentException
	 */
	Object put(String colname, Object obj) throws IllegalArgumentException;

	/**
	 * <p>Replace column value.</p>
	 * @param colname String Case insensitive column name
	 * @param obj Object Column value
	 * @return Previous column value
	 * @throws IllegalArgumentException
	 */
	Object replace(String colname, Object obj) throws IllegalArgumentException;
	
	/**
	 * <p>Set column value.</p>
	 * @param colname String Case insensitive column name
	 * @param boolval boolean Column value
	 * @return Boolean Previous column value
	 * @throws IllegalArgumentException
	 */
	Boolean put(String colname, boolean boolval) throws IllegalArgumentException;

	/**
	 * <p>Set column value.</p>
	 * @param colname String Case insensitive column name
	 * @param bytearray byte[] Column value
	 * @return Boolean Previous column value
	 * @throws IllegalArgumentException
	 */
	Object put(String colname, byte[] bytearray) throws IllegalArgumentException;

	/**
	 * <p>Set column value.</p>
	 * @param colname String Case insensitive column name
	 * @param bytevalue byte Column value
	 * @return Byte Previous column value
	 * @throws IllegalArgumentException
	 */
	Byte put(String colname, byte bytevalue) throws IllegalArgumentException;

	/**
	 * <p>Set column value.</p>
	 * @param colname String Case insensitive column name
	 * @param charvalue char Column value
	 * @return Character Previous column value
	 * @throws IllegalArgumentException
	 */
	Character put(String colname, char charvalue) throws IllegalArgumentException;

	/**
	 * <p>Set column value.</p>
	 * @param colname String Case insensitive column name
	 * @param shortvalue short Column value
	 * @return Short Previous column value
	 * @throws IllegalArgumentException
	 */
	Short put(String colname, short shortvalue) throws IllegalArgumentException;

	/**
	 * <p>Set column value.</p>
	 * @param colname String Case insensitive column name
	 * @param intvalue int Column value
	 * @return Integer Previous column value
	 * @throws IllegalArgumentException
	 */
	Integer put(String colname, int intvalue) throws IllegalArgumentException;

	/**
	 * <p>Set column value.</p>
	 * @param colname String Case insensitive column name
	 * @param longvalue long Column value
	 * @return Long Previous column value
	 * @throws IllegalArgumentException
	 */
	Long put(String colname, long longvalue) throws IllegalArgumentException;

	/**
	 * <p>Set column value.</p>
	 * @param colname String Case insensitive column name
	 * @param floatvalue float Column value
	 * @return Float Previous column value
	 * @throws IllegalArgumentException
	 */
	Float put(String colname, float floatvalue) throws IllegalArgumentException;

	/**
	 * <p>Set column value.</p>
	 * @param colname String Case insensitive column name
	 * @param doublevalue double Column value
	 * @return Double Previous column value
	 * @throws IllegalArgumentException
	 */
	Double put(String colname, double doublevalue) throws IllegalArgumentException;

	/**
	 * <p>Set Date column value by parsing a String using a pattern.</p>
	 * @param colname String Case insensitive column name
	 * @param strdate String Column value
	 * @param pattern SimpleDateFormat Pattern
	 * @return Date Previous column value
	 * @throws IllegalArgumentException
	 * @throws ParseException
	 */
	Date put(String colname, String strdate, SimpleDateFormat pattern) throws ParseException,IllegalArgumentException;

	/**
	 * <p>Set BigDecimal column value by paring a String using a pattern.</p>
	 * @param colname String Case insensitive column name
	 * @param decimalvalue String Column value
	 * @param pattern DecimalFormat Pattern
	 * @return BigDecimal Previous column value
	 * @throws IllegalArgumentException
	 * @throws ParseException
	 */
	BigDecimal put(String colname, String decimalvalue, DecimalFormat pattern) throws ParseException,IllegalArgumentException;

	/**
	 * <p>Remove column value.</p>
	 * @param colname String Case insensitive column name
	 * @return Object Previous column value
	 */
	Object remove(String colname);
	
	/**
	 * @return int Number of columns
	 */
	int size();

	/**
	 * @return String
	 * @throws IOException
	 */
	String toJSON() throws IOException;

	/**
	 * @return String
	 * @throws IOException
	 */
	String toXML() throws IOException;

	/**
	 * @param identSpaces String Spaces to put before each line
	 * @param dateFormat DateFormat
	 * @param decimalFormat NumberFormat
	 * @param textFormat Format
	 * @return String
	 * @throws IOException
	 */
	String toXML(String identSpaces, DateFormat dateFormat, NumberFormat decimalFormat, Format textFormat) throws IOException;
	
}
