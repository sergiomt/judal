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

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Locale;

import javax.jdo.JDOException;

/**
 * Base class for Record implementations that hold only a single column containing a subclass of Number
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public abstract class AbstractSingleNumberColumnRecord extends SingleObjectColumnRecord {

	private static final long serialVersionUID = 1L;

	public AbstractSingleNumberColumnRecord(String tableName) {
		super(tableName);
	}

	public AbstractSingleNumberColumnRecord(String tableName, String columnName) {
		super(tableName, columnName);
	}

	@Override
	public boolean isEmpty(String colname) {
		return value == null;
	}

	@Override
	public Number apply(String colname) {
		return (Number) value;
	}

	@Override
	public Number getKey() throws JDOException {
		return (Number) super.getKey();		
	}

	@Override
	public Number getValue() throws JDOException {
		return (Number) super.getValue();
	}

	@Override
	public String getDecimalFormated(String colname, Locale loc, int fractionDigits)
			throws ClassCastException, NumberFormatException,NullPointerException, IllegalArgumentException {
		BigDecimal oDec = getDecimal(colname);
		if (oDec==null) {
			return null;
		} else {
			DecimalFormat oNumFmt = (DecimalFormat) NumberFormat.getNumberInstance(loc);
			oNumFmt.setMaximumFractionDigits(fractionDigits);
			return oNumFmt.format(oDec);
		}
	}

	@Override
	public String getDecimalFormated(String colname, String pattern)
			throws ClassCastException, NumberFormatException,NullPointerException, IllegalArgumentException {
		BigDecimal oDec = getDecimal(colname);
		if (oDec==null) {
			return null;
		} else {
			return new DecimalFormat(pattern).format(oDec.doubleValue());
		}
	}
	
	@Override
	public double getDouble(String colname)
			throws NullPointerException, ClassCastException, NumberFormatException {
		return apply(colname).doubleValue();
	}

	@Override
	public String getDoubleFormated(String colname, String pattern)
			throws ClassCastException,NumberFormatException,NullPointerException,IllegalArgumentException {
		if (isNull(colname))
			return null;
		else
			return new DecimalFormat(pattern).format(getDouble(colname));
	}
	
	@Override
	public float getFloat(String colname)
			throws NullPointerException, ClassCastException, NumberFormatException {
		return apply(colname).floatValue();
	}

	@Override
	public String getFloatFormated(String colname, String pattern)
			throws ClassCastException, NumberFormatException, NullPointerException, IllegalArgumentException {
		if (isNull(colname))
			return null;
		else
			return new DecimalFormat(pattern).format(getFloat(colname));
	}

	@Override
	public Integer getInteger(String colname) throws NumberFormatException {
		return (Integer) value;
	}

	@Override
	public int getInt(String colname) throws NullPointerException, ClassCastException, NumberFormatException {
		return apply(colname).intValue();
	}

	@Override
	public long getLong(String colname) throws NullPointerException, NumberFormatException {
		return apply(colname).longValue();
	}

	@Override
	public String getString(String colname) throws ClassCastException {
		return isNull(colname) ? null : apply(colname).toString();
	}

	@Override
	public String getString(String colname, String defaultValue) throws ClassCastException {
		return isNull(colname) ? defaultValue : apply(colname).toString();
	}

}
