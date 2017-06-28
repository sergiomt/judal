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

/**
 * <p>Interface for adaptors of non-standard field types.</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public interface FieldHelper {

	/**
	 * <p>Get part of a time interval.</p>
	 * @param rec Record
	 * @param colname String Column name
	 * @param part String Part name
	 * @return int Value of part
	 * @throws ClassCastException
	 * @throws NullPointerException
	 * @throws NumberFormatException
	 * @throws IllegalArgumentException
	 */
	int getIntervalPart(Record rec, String colname, String part) throws ClassCastException, NullPointerException, NumberFormatException, IllegalArgumentException;
	
	/**
	 * <p>Get Integer array.</p>
	 * @param rec Record
	 * @param colname String Column name
	 * @return Integer[]
	 * @throws ClassCastException
	 */
	Integer[] getIntegerArray(Record rec, String colname) throws ClassCastException;

	/**
	 * <p>Get Long array.</p>
	 * @param rec Record
	 * @param colname String Column name
	 * @return Long[]
	 * @throws ClassCastException
	 */
	Long[] getLongArray(Record rec, String colname) throws ClassCastException;

	/**
	 * <p>Get Float array.</p>
	 * @param rec Record
	 * @param colname String Column name
	 * @return Float[]
	 * @throws ClassCastException
	 */
	Float[] getFloatArray(Record rec, String colname) throws ClassCastException;

	/**
	 * <p>Get Double array.</p>
	 * @param rec Record
	 * @param colname String Column name
	 * @return Double[]
	 * @throws ClassCastException
	 */
	Double[] getDoubleArray(Record rec, String colname) throws ClassCastException;

	/**
	 * <p>Get Date array.</p>
	 * @param rec Record
	 * @param colname String Column name
	 * @return Date[]
	 * @throws ClassCastException
	 */
	Date[] getDateArray(Record rec, String colname) throws ClassCastException;
	
	/**
	 * <p>Get String array.</p>
	 * @param rec Record
	 * @param colname String Column name
	 * @return String[]
	 * @throws ClassCastException
	 * @throws ClassNotFoundException
	 */
	String[] getStringArray(Record rec, String colname) throws ClassCastException, ClassNotFoundException;
	
	/**
	 * <p>Get LatLong.</p>
	 * @param rec Record
	 * @param columName String Column name
	 * @return LatLong
	 * @throws ClassCastException
	 */
	LatLong getLatLong(Record rec, String columName) throws ClassCastException, NumberFormatException, ArrayIndexOutOfBoundsException;
	
	/**
	 * <p>Get name-&gt;value Map.</p>
	 * @param rec Record
	 * @param columName String Column name
	 * @return Object (usually an instance of java.util.Map&lt;String,String&gt;)
	 * @throws ClassCastException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws IllegalArgumentException
	 * @throws InvocationTargetException
	 * @throws NoSuchMethodException
	 * @throws SecurityException
	 * @throws ClassNotFoundException
	 */
	Object getMap(Record rec, String columName) throws ClassCastException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, ClassNotFoundException;
	
}
