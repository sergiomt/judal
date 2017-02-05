package org.judal.storage.java;

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

import java.sql.Types;
import java.util.Map;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import javax.jdo.JDOException;

import org.judal.metadata.TableDef;
import org.judal.metadata.ColumnDef;
import org.judal.serialization.BytesConverter;
import org.judal.storage.AbstractRecord;
import org.judal.storage.TableDataSource;

/**
 * Implementation of Record interface using an hash map to hold column values
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class MapRecord extends AbstractRecord implements JavaRecord {

	private static final long serialVersionUID = 10000l;

	/**
	 * Column names are case insensitive, so specialize a HashMap to implement case insensitive lookups
	*/	
	@SuppressWarnings("serial")
	private class CaseInsensitiveValuesMap extends HashMap<String,Object> {
		@Override
		public Object put (String key, Object value) {
			return super.put(key.toLowerCase(), value);
		}
		@Override
		public Object remove (Object key) {
			return super.remove(((String) key).toLowerCase());
		}
		@Override
		public Object get (Object key) {
			return super.get(((String) key).toLowerCase());
		}
		@Override
		public boolean containsKey (Object key) {
			return super.containsKey(((String) key).toLowerCase());
		}
	}
	
	private CaseInsensitiveValuesMap values;
	private HashMap<Integer, String> columnIndexes;

	/**
	 * Constructor
	 */
	public MapRecord(TableDef tableDefinition) {
		super(tableDefinition);
		values = new CaseInsensitiveValuesMap();
		columnIndexes = new HashMap<Integer, String>();
		int i = 0;
		for (ColumnDef cdef: tableDefinition.getColumns())
			columnIndexes.put(++i, cdef.getName());
	}

	/**
	 * Constructor
	 * @throws JDOException 
	 */
	public MapRecord(TableDataSource dataSource, String tableName) throws JDOException {
		super(dataSource, tableName);
		values = new CaseInsensitiveValuesMap();
		columnIndexes = new HashMap<Integer, String>();
	}

	public MapRecord(TableDataSource dataSource, String tableName, String... columnNames) throws JDOException {
		super(new TableDef(tableName, dataSource.getTableDef(tableName).filterColumns(columnAliases(columnNames))));
		values = new CaseInsensitiveValuesMap();
		columnIndexes = new HashMap<Integer, String>();
		int c = 0;
		for (String columnName : columnNames)
			columnIndexes.put(new Integer(++c), getColumnAlias(columnName));
	}

	private static String[] columnAliases(String... columnNames) {
		final int colCount = columnNames.length;
		String[] aliases = new String[colCount];
		for (int c=0; c<colCount; c++)
			aliases[c] = getColumnAlias(columnNames[c]);
		return aliases;
	}

	@Override
	public Set<String> keySet() {
		TreeSet<String> keys = new TreeSet<String>();
		for (ColumnDef c : getTableDef().getColumns())
			keys.add(c.getName());
		return keys;
	}
	
	@Override
	public Object put(int colpos, Object value) {
		return values.put(columnIndexes.get(colpos), value);
	}

	/**
	 * Set or replace the value of a column
	 * @param colname String
	 * @param bytes byte[]
	 * @return byte[] Previous value stored at the column
	 */
	@Override
	public Object put(String colname, byte[] bytes) {
		byte[] retval;
		Object former = values.get(colname);
		if (former==null)
			retval = null;
		else if (former instanceof byte[])
			retval = Arrays.copyOf((byte[]) former, ((byte[]) former).length);
		else 
			retval = BytesConverter.toBytes(former, Types.JAVA_OBJECT);
		values.put(colname, bytes);
		return retval;
	}

	@Override
	public Object remove(String colname) {
		Object retval = containsKey(colname) ? get(colname) : null;
		values.remove(colname);
		return retval;
	}

	@Override
	public Object remove(Object colname) {
		Object retval = containsKey(colname) ? get(colname) : null;
		values.remove(colname);
		return retval;
	}

	@Override
	public void clear() {
		values.clear();
	}

	@Override
	public boolean containsKey(Object key) {
		return values.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return values.containsValue(value);
	}

	@Override
	public Set<java.util.Map.Entry<String, Object>> entrySet() {
		HashMap<String,Object> entries = new HashMap<String,Object>();
		for (Integer columnIndex : columnIndexes.keySet()) {
			String columnName = columnIndexes.get(columnIndex);
			entries.put(columnName, values.get(columnName));
		}
		return Collections.unmodifiableSet(entries.entrySet());
	}

	@Override
	public Object apply(String key) {
		return values.get(key);
	}

	@Override
	public Object get(Object key) {
		return values.get(key);
	}
	
	@Override
	public Map<String,String> getMap(String sKey) throws ClassCastException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, ClassNotFoundException {
		return MapFieldHelper.getMap(this, sKey);
	} // getMap

	@Override
	public boolean isEmpty() {
		return values.isEmpty();
	}

	@Override
	public Object put(String key, Object value) {
		return values.put(key, value);
	}

	@Override
	public int size() {
		return values.size();
	}

	@Override
	public Collection<Object> values() {
		return values.values();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void setValue(Serializable value) throws JDOException {
		if (value instanceof Map) {
			values = new CaseInsensitiveValuesMap();
			columnIndexes = new HashMap<Integer, String>();				
			Map valmap = (Map) value;
			Iterator iter = valmap.keySet().iterator();
			int n = 0;
			while (iter.hasNext()) {
				Object key = iter.next();
				values.put(key.toString(), valmap.get(key));
				columnIndexes.put(++n, key.toString());
			}
		} else {
			throw new ClassCastException("MapRecord.setValue() Cannot cast from "+value.getClass().getName()+" to java.util.Map");
		}
	}

	@Override
	public Object getValue() throws JDOException {
		return Collections.unmodifiableMap(values);
	}

	@Override
	public void putAll(Map<? extends String,? extends Object> mValues) {
		for (Entry<? extends String,? extends Object> entry : mValues.entrySet()) {
			put(entry.getKey(), entry.getValue());
		}
	}
	
}
