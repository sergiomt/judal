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
	 * @throws UnsupportedOperationException
	 */
	int getIntervalPart(Record rec, String colname, String part)
			throws ClassCastException, NullPointerException, NumberFormatException, IllegalArgumentException, UnsupportedOperationException;

	/**
	 * <p>Get Integer array.</p>
	 * @param rec Record
	 * @param colname String Column name
	 * @return Integer[]
	 * @throws ClassCastException
	 * @throws UnsupportedOperationException
	 */
	Integer[] getIntegerArray(Record rec, String colname) throws ClassCastException, UnsupportedOperationException;

	/**
	 * <p>Get Long array.</p>
	 * @param rec Record
	 * @param colname String Column name
	 * @return Long[]
	 * @throws ClassCastException
	 * @throws UnsupportedOperationException
	 */
	Long[] getLongArray(Record rec, String colname) throws ClassCastException, UnsupportedOperationException;

	/**
	 * <p>Get Float array.</p>
	 * @param rec Record
	 * @param colname String Column name
	 * @return Float[]
	 * @throws ClassCastException
	 * @throws UnsupportedOperationException
	 */
	Float[] getFloatArray(Record rec, String colname) throws ClassCastException, UnsupportedOperationException;

	/**
	 * <p>Get Double array.</p>
	 * @param rec Record
	 * @param colname String Column name
	 * @return Double[]
	 * @throws ClassCastException
	 */
	Double[] getDoubleArray(Record rec, String colname) throws ClassCastException, UnsupportedOperationException;

	/**
	 * <p>Get Date array.</p>
	 * @param rec Record
	 * @param colname String Column name
	 * @return Date[]
	 * @throws ClassCastException
	 * @throws UnsupportedOperationException
	 */
	Date[] getDateArray(Record rec, String colname) throws ClassCastException, UnsupportedOperationException;
	
	/**
	 * <p>Get String array.</p>
	 * @param rec Record
	 * @param colname String Column name
	 * @return String[]
	 * @throws ClassCastException
	 * @throws ClassNotFoundException
	 * @throws UnsupportedOperationException
	 */
	String[] getStringArray(Record rec, String colname) throws ClassCastException, ClassNotFoundException;
	
	/**
	 * <p>Get LatLong.</p>
	 * @param rec Record
	 * @param columName String Column name
	 * @return LatLong
	 * @throws ClassCastException
	 * @throws UnsupportedOperationException
	 */
	LatLong getLatLong(Record rec, String columName) throws ClassCastException, NumberFormatException, ArrayIndexOutOfBoundsException, UnsupportedOperationException;

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
	 * @throws UnsupportedOperationException
	 */
	Object getMap(Record rec, String columName) throws ClassCastException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, ClassNotFoundException;

	
	/**
	 * <p>Get the family to which a given column belongs.</p>
	 * @param rec Record
	 * @param columName String
	 * @return String Family name or null if the family could not be determined or the column does not belong to any known family
	 * @throws IllegalArgumentException
	 * @throws ArrayIndexOutOfBoundsException
	 * @throws UnsupportedOperationException
	 */
	String getFamilyName(Record rec, String columName) throws IllegalArgumentException, ArrayIndexOutOfBoundsException, UnsupportedOperationException;

	
	/**
	 * <p>Get the type of a column</p>
	 * @param rec Record
	 * @param columName String
	 * @return int One of java.sql.Types
	 * @throws IllegalArgumentException
	 * @throws ArrayIndexOutOfBoundsException
	 * @throws UnsupportedOperationException
	 */
	 int getType(Record rec, String columName) throws IllegalArgumentException, ArrayIndexOutOfBoundsException, UnsupportedOperationException;
}
