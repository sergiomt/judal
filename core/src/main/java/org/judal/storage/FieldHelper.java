package org.judal.storage;

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
import java.util.Date;

import org.judal.storage.table.Record;

import com.knowgate.gis.LatLong;

public interface FieldHelper {

	int getIntervalPart(Record rec, String colname, String part) throws ClassCastException, NullPointerException, NumberFormatException, IllegalArgumentException;
	
	Integer[] getIntegerArray(Record rec, String colname) throws ClassCastException;

	Long[] getLongArray(Record rec, String colname) throws ClassCastException;

	Float[] getFloatArray(Record rec, String colname) throws ClassCastException;

	Double[] getDoubleArray(Record rec, String colname) throws ClassCastException;

	Date[] getDateArray(Record rec, String colname) throws ClassCastException;
	
	String[] getStringArray(Record rec, String colname) throws ClassCastException, ClassNotFoundException;
	
	LatLong getLatLong(Record rec, String columname) throws ClassCastException, NumberFormatException, ArrayIndexOutOfBoundsException;
	
	public Object getMap(Record rec, String key) throws ClassCastException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, ClassNotFoundException;
	
}
