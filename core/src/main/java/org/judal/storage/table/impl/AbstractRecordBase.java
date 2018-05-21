package org.judal.storage.table.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigDecimal;

import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.Format;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;

import org.judal.metadata.ColumnDef;
import org.judal.serialization.BytesConverter;
import org.judal.serialization.JSONValue;
import org.judal.storage.query.AbstractQuery;
import org.judal.storage.table.Record;

import com.knowgate.currency.Money;
import com.knowgate.dateutils.DateHelper;
import com.knowgate.stringutils.Html;
import com.knowgate.stringutils.XML;
 
/**
 * <p>Partial implementation of Record interface.</p>
 * This class contains the getter and setter methods and the JSON and XML writers.
 * @author Sergio Montoro Ten
 * @version 1.0
 *
 */
 public abstract class AbstractRecordBase implements Record {
 
	private static final long serialVersionUID = 1L;

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
	 * @throws NullPointerException if Locale is <b>null</b>
	 */
	@Override
	public String getDecimalFormated(String sColName, Locale oLoc, int nFractionDigits)
			throws ClassCastException, NumberFormatException,NullPointerException {
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

	public Object getSerializable(String sColName) throws IOException, ClassNotFoundException {
		if (isNull(sColName)) return null;
		ByteArrayInputStream oByIn = new ByteArrayInputStream((byte[]) apply(sColName));
		ObjectInputStream oObjIn = new ObjectInputStream(oByIn);
		Object retval = oObjIn.readObject();
		oObjIn.close();
		oByIn.close();
		return retval;
		
	}

	protected boolean isNullOrNone(Object obj) {
		return obj==null || obj.getClass().getName().equals("scala.None$");
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
		return isNullOrNone(prev) ? null : new Short(prev.toString());
	}

	/** <p>Set int value at internal collection</p>
	 * @param sColName String Column Name
	 * @param iVal int Field Value
	 * @return Integer Previous column value
	 */
	@Override
	public Integer put(String sColName, int iVal) {
		Object prev = put(sColName, new Integer(iVal));
		return isNullOrNone(prev) ? null : new Integer(prev.toString());
	}

	/** <p>Set long value at internal collection</p>
	 * @param sColName String Column Name
	 * @param lVal long Field Value
	 * @return Long Previous column value
	 */
	@Override
	public Long put(String sColName, long lVal) {
		Object prev = put(sColName, new Long(lVal));
		return isNullOrNone(prev) ? null : new Long(prev.toString());
	}

	/** <p>Set float value at internal collection</p>
	 * @param sColName String Column Name
	 * @param fVal float Field Value
	 * @return Float Previous column value
	 */
	@Override
	public Float put(String sColName, float fVal) {
		Object prev = put(sColName, new Float(fVal));
		return isNullOrNone(prev) ? null : new Float(prev.toString());
	}

	/** <p>Set double value at internal collection</p>
	 * @param sColName String Column Name
	 * @param dVal double Field Value
	 * @return Double Previous column value
	 */
	@Override
	public Double put(String sColName, double dVal) {
		Object prev = put(sColName, new Double(dVal));
		return isNullOrNone(prev) ? null : new Double(prev.toString());
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
	 * <p>Serialize and object and write the resulting byte array into the specified column.</p>
	 * @param colname String Column Name
	 * @param value Serializable Column value
	 * @throws IOException 
	 */
	public Serializable putSerializable(String colname, Serializable value) throws IOException {
		ByteArrayOutputStream byOut = new ByteArrayOutputStream();
		ObjectOutputStream objOut = new ObjectOutputStream(byOut);
		objOut.writeObject(value);
		put (colname, byOut.toByteArray());
		objOut.close();
		byOut.close();
		return value;
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
	
 }