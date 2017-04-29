package org.judal.storage.table;

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

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;

import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

import javax.jdo.FetchGroup;

import org.judal.metadata.ColumnDef;
import org.judal.storage.ConstraintsChecker;
import org.judal.storage.keyvalue.Stored;

import com.knowgate.currency.Money;
import com.knowgate.gis.LatLong;

/**
 * 
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public interface Record extends Serializable, Stored {

	ColumnDef[] columns();

	ColumnDef getColumn(String colname) throws ArrayIndexOutOfBoundsException;

	boolean isNull(String colname);

	/**
	 * @return <b>true</b> if Record does not contain the given key or its value is <b>null</b> or its value is an empty string ""
	 */
	boolean isEmpty(String colname);

	void clear();
	
	String getTableName();

	FetchGroup fetchGroup();

	ConstraintsChecker getConstraintsChecker();

	Object apply(String colname);
	
	BigDecimal getDecimal(String colname) throws NullPointerException, ClassCastException, NumberFormatException;

	String getDecimalFormated(String sKey, String sPattern) throws ClassCastException, NumberFormatException,NullPointerException, IllegalArgumentException;

	String getDecimalFormated(String sKey, Locale oLoc, int nFractionDigits) throws ClassCastException, NumberFormatException,NullPointerException, IllegalArgumentException;

	byte[] getBytes(String colname) throws ClassCastException;

	String getFloatFormated(String colname, String pattern) throws ClassCastException, NumberFormatException, NullPointerException, IllegalArgumentException;

	Integer getInteger(String colname) throws NumberFormatException;

	int getInt(String colname) throws NullPointerException,ClassCastException,NumberFormatException;

	long getLong(String colname) throws NullPointerException,NumberFormatException;

	Money getMoney(String colname) throws NumberFormatException;

	short getShort(String colname) throws NullPointerException,NumberFormatException;

	float getFloat(String colname) throws NullPointerException, ClassCastException, NumberFormatException;

	double getDouble(String colname) throws NullPointerException, ClassCastException, NumberFormatException;

	String getDoubleFormated(String colname, String sPattern) throws ClassCastException,NumberFormatException,NullPointerException,IllegalArgumentException;

	Calendar getCalendar(String colname);

	Date getDate(String colname);

	Date getDate(String colname, Date defvalue);

	String getDateFormated(String colname, String format) throws ClassCastException;

	String getDateTime(String colname) throws ClassCastException;

	String getDateShort(String colname) throws ClassCastException;

	int getIntervalPart(String colname, String part) throws ClassCastException, ClassNotFoundException, NullPointerException, NumberFormatException, IllegalArgumentException;
	
	Integer[] getIntegerArray(String colname) throws ClassCastException, ClassNotFoundException;

	Long[] getLongArray(String colname) throws ClassCastException, ClassNotFoundException;

	Float[] getFloatArray(String colname) throws ClassCastException, ClassNotFoundException;

	Double[] getDoubleArray(String colname) throws ClassCastException, ClassNotFoundException;

	Date[] getDateArray(String colname) throws ClassCastException, ClassNotFoundException;
	
	LatLong getLatLong(String columname) throws ClassCastException, NumberFormatException, ArrayIndexOutOfBoundsException, ClassNotFoundException;
	
	String getString(String colname) throws ClassCastException;

	String getString(String colname, String defvalue) throws ClassCastException;

	String[] getStringArray(String colname) throws ClassCastException, ClassNotFoundException;
	
	String getStringHtml(String colname, String defvalue);

	boolean getBoolean(String colname, boolean defvalue) throws ClassCastException;

	Object getMap(String colname) throws ClassCastException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, ClassNotFoundException;
	
	Object put(int colpos, Object obj) throws IllegalArgumentException;

	Object put(String colname, Object obj) throws IllegalArgumentException;

	Object replace(String colname, Object obj) throws IllegalArgumentException;
	
	Boolean put(String colname, boolean boolval) throws IllegalArgumentException;

	Object put(String colname, byte[] bytearray) throws IllegalArgumentException;

	Byte put(String colname, byte bytevalue) throws IllegalArgumentException;

	Short put(String colname, short shortvalue) throws IllegalArgumentException;

	Integer put(String colname, int intvalue) throws IllegalArgumentException;

	Long put(String colname, long longvalue) throws IllegalArgumentException;

	Float put(String colname, float floatvalue) throws IllegalArgumentException;

	Double put(String colname, double doublevalue) throws IllegalArgumentException;

	Date put(String colname, String strdate, SimpleDateFormat pattern) throws ParseException,IllegalArgumentException;

	BigDecimal put(String colname, String decimalvalue, DecimalFormat pattern) throws ParseException,IllegalArgumentException;

	Object remove(String colname);
	
	int size();

}
