package org.judal.hbase;

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

import java.lang.reflect.InvocationTargetException;

import java.sql.Types;

import java.util.Date;

import org.judal.storage.FieldHelper;
import org.judal.storage.table.Record;

import com.knowgate.gis.LatLong;

public class HBDefaultFieldHelper implements FieldHelper {

	public static final String DEFAULT_COLUMN_FAMILY = "0";

	@Override
	public int getIntervalPart(Record rec, String colname, String part) throws ClassCastException, NullPointerException,
			NumberFormatException, IllegalArgumentException, UnsupportedOperationException {
		throw new UnsupportedOperationException("HBDefaultFieldHelper does not support method getIntervalPart()");
	}

	@Override
	public Integer[] getIntegerArray(Record rec, String colname)
			throws ClassCastException, UnsupportedOperationException {
		throw new UnsupportedOperationException("HBDefaultFieldHelper does not support method getIntegerArray()");
	}

	@Override
	public Long[] getLongArray(Record rec, String colname) throws ClassCastException, UnsupportedOperationException {
		throw new UnsupportedOperationException("HBDefaultFieldHelper does not support method getLongArray()");
	}

	@Override
	public Float[] getFloatArray(Record rec, String colname) throws ClassCastException, UnsupportedOperationException {
		throw new UnsupportedOperationException("HBDefaultFieldHelper does not support method getFloatArray()");
	}

	@Override
	public Double[] getDoubleArray(Record rec, String colname)
			throws ClassCastException, UnsupportedOperationException {
		throw new UnsupportedOperationException("HBDefaultFieldHelper does not support method getDoubleArray()");
	}

	@Override
	public Date[] getDateArray(Record rec, String colname) throws ClassCastException, UnsupportedOperationException {
		throw new UnsupportedOperationException("HBDefaultFieldHelper does not support method getDateArray()");
	}

	@Override
	public String[] getStringArray(Record rec, String colname) throws ClassCastException, ClassNotFoundException {
		throw new UnsupportedOperationException("HBDefaultFieldHelper does not support method getStringArray()");
	}

	@Override
	public LatLong getLatLong(Record rec, String columName) throws ClassCastException, NumberFormatException,
			ArrayIndexOutOfBoundsException, UnsupportedOperationException {
		throw new UnsupportedOperationException("HBDefaultFieldHelper does not support method getLatLong()");
	}

	@Override
	public Object getMap(Record rec, String columName)
			throws ClassCastException, InstantiationException, IllegalAccessException, IllegalArgumentException,
			InvocationTargetException, NoSuchMethodException, SecurityException, ClassNotFoundException {
		throw new UnsupportedOperationException("HBDefaultFieldHelper does not support method getMap()");
	}

	@Override
	public String getFamilyName(Record rec, String columName)
			throws IllegalArgumentException, UnsupportedOperationException {
		// TODO Auto-generated method stub
		return DEFAULT_COLUMN_FAMILY;
	}

	@Override
	public int getType(Record rec, String columName) throws IllegalArgumentException, UnsupportedOperationException {
		return Types.NVARCHAR;
	}

}
