package org.judal.storage.java;

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

import java.io.Serializable;

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
import javax.jdo.JDOUserException;

import org.judal.metadata.TableDef;
import org.judal.metadata.ViewDef;
import org.judal.metadata.ColumnDef;
import org.judal.serialization.BytesConverter;
import org.judal.storage.ConstraintsChecker;
import org.judal.storage.FieldHelper;
import org.judal.storage.table.TableDataSource;
import org.judal.storage.table.impl.AbstractRecord;
import org.judal.storage.java.internal.CaseInsensitiveValuesMap;

import com.knowgate.debug.DebugFile;

/**
 * Implementation of Record interface using an hash map to hold column values
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class MapRecord extends AbstractRecord implements JavaRecord {

	private static final long serialVersionUID = 10000l;

	private CaseInsensitiveValuesMap values;
	private HashMap<Integer, String> columnIndexes;

	/**
	 * Constructor
	 */
	public MapRecord(ViewDef tableDefinition) {
		this(tableDefinition,null,null);
	}

	/**
	 * Constructor
	 */
	public MapRecord(ViewDef tableDefinition, FieldHelper fieldHelper) {
		this(tableDefinition, fieldHelper, null);
	}

	/**
	 * Constructor
	 */
	public MapRecord(ViewDef tableDefinition, ConstraintsChecker constraintsChecker) {
		this(tableDefinition, null, constraintsChecker);
	}
	
	/**
	 * Constructor
	 */
	public MapRecord(ViewDef tableDefinition, FieldHelper fieldHelper, ConstraintsChecker constraintsChecker) {
		super(tableDefinition, fieldHelper, constraintsChecker);
		values = new CaseInsensitiveValuesMap();
		columnIndexes = new HashMap<Integer, String>();
		int i = 0;
		for (ColumnDef cdef: tableDefinition.getColumns())
			columnIndexes.put(++i, cdef.getName());
	}

	/**
	 * Constructor
	 * @param dataSource TableDataSource
	 * @param tableName String
	 * @throws JDOException 
	 * @throws JDOUserException if table or view is not found at DataSource metadata
	 */
	public MapRecord(TableDataSource dataSource, String tableName) throws JDOException {
		this(getViewDefForName(dataSource, tableName), dataSource.getFieldHelper());	
	}

	/**
	 * Constructor
	 * @param dataSource TableDataSource
	 * @param tableName String
	 * @param fieldHelper FieldHelper
	 * @throws JDOException 
	 * @throws JDOUserException if table or view is not found at DataSource metadata
	 */
	public MapRecord(TableDataSource dataSource, String tableName, FieldHelper fieldHelper) throws JDOException {
		this(getViewDefForName(dataSource, tableName), fieldHelper);
	}

	/**
	 * Constructor
	 * @param dataSource TableDataSource
	 * @param tableName String
	 * @param constraintsChecker ConstraintsChecker
	 * @throws JDOException 
	 * @throws JDOUserException if table or view is not found at DataSource metadata
	 */
	public MapRecord(TableDataSource dataSource, String tableName, ConstraintsChecker constraintsChecker) throws JDOException {
		this(dataSource, tableName, dataSource.getFieldHelper(), constraintsChecker);
	}
	
	/**
	 * Constructor
	 * @param dataSource TableDataSource
	 * @param tableName String
	 * @param fieldHelper FieldHelper
	 * @param constraintsChecker ConstraintsChecker
	 * @throws JDOException 
	 * @throws JDOUserException if table or view is not found at DataSource metadata
	 */
	public MapRecord(TableDataSource dataSource, String tableName, FieldHelper fieldHelper, ConstraintsChecker constraintsChecker) throws JDOException {
		this(getViewDefForName(dataSource, tableName), fieldHelper, constraintsChecker);
	}
	
	/**
	 * Constructor
	 * @throws JDOException 
	 */
	public MapRecord(TableDataSource dataSource, String tableName, String... columnNames) throws JDOException {
		this(dataSource, tableName, dataSource.getFieldHelper(), null, columnNames);
	}

	/**
	 * Constructor
	 * @throws JDOException 
	 */
	public MapRecord(TableDataSource dataSource, String tableName, FieldHelper fieldHelper, String... columnNames) throws JDOException {
		this(dataSource, tableName, fieldHelper, null, columnNames);
	}

	/**
	 * Constructor
	 * @throws JDOException 
	 */
	public MapRecord(TableDataSource dataSource, String tableName, ConstraintsChecker constraintsChecker, String... columnNames) throws JDOException {
		this(dataSource, tableName, null, constraintsChecker, columnNames);
	}
	
	/**
	 * Constructor
	 * @throws JDOException 
	 */
	public MapRecord(TableDataSource dataSource, String tableName, FieldHelper fieldHelper, ConstraintsChecker constraintsChecker, String... columnNames) throws JDOException {
		super(new TableDef(tableName, filterColumns(dataSource, tableName, columnAliases(columnNames))), fieldHelper, constraintsChecker);
		values = new CaseInsensitiveValuesMap();
		columnIndexes = new HashMap<Integer, String>();
		int c = 0;
		for (String columnName : columnNames)
			columnIndexes.put(new Integer(++c), getColumnAlias(columnName));
	}
	
	private static ColumnDef[] filterColumns(TableDataSource dataSource, String tableName, String[] columnNames) {
		ColumnDef[] retval;
		ViewDef vdef = dataSource.getTableOrViewDef(tableName);
		if (null==vdef) {
			retval = new ColumnDef[columnNames.length];
			for (int c=columnNames.length-1; c>=0; c--)
				retval[c] = dataSource.createColumnDef(columnNames[c], c+1, (short) Types.NULL, null);
		} else {
			retval = vdef.filterColumns(columnAliases(columnNames));
		}
		return retval;
	}

	private static String[] columnAliases(String[] columnNames) {
		final int colCount = columnNames.length;
		String[] aliases = new String[colCount];
		for (int c=0; c<colCount; c++)
			aliases[c] = getColumnAlias(columnNames[c]);
		return aliases;
	}

	@Override
	@SuppressWarnings("unchecked")
	public Entry<String,Object>[] asEntries() {
		return values.entrySet().toArray(new Entry[values.size()]);
	}

	@Override
	public Map<String,Object> asMap() {
		return values;
	}

	@Override
	public Set<String> keySet() {
		TreeSet<String> keys = new TreeSet<String>();
		for (ColumnDef c : getTableDef().getColumns())
			keys.add(c.getName());
		return keys;
	}
	
	@Override
	public Object put(int colpos, Object value) throws ArrayIndexOutOfBoundsException {
		final String columnName = columnIndexes.get(colpos);
		if (null==columnName)
				throw new ArrayIndexOutOfBoundsException("Cannot find column at position " + String.valueOf(colpos)+" of "+String.valueOf(columnIndexes.size())+" columns");
		return values.put(columnName, value);
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

	@SuppressWarnings("unchecked")
	@Override
	public Map<String,String> getMap(String sKey) throws ClassCastException, InstantiationException, IllegalAccessException, IllegalArgumentException, NoSuchMethodException, SecurityException, ClassNotFoundException {
		return (Map<String,String>) super.getMap(sKey);
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

	private static ViewDef getViewDefForName(TableDataSource dataSource, String name) {
		final ViewDef vdef = dataSource.getTableOrViewDef(name);
		if (null==vdef) {
			if (DebugFile.trace) {
				DebugFile.writeln("JDOException at MapRecord.getViewDefForName(). Table or View not found " + name);
				DebugFile.writeln("SchemaMetadata contains " + dataSource.getMetaData().tables().size()+" tables");
				DebugFile.writeln("SchemaMetadata contains " + dataSource.getMetaData().views().size()+" views");
				StringBuilder tableList = new StringBuilder();
				tableList.append("JDC tables list = [");
				try {
					boolean first = true;
					tableList.append("SchemaMetaData tables = [");
					first = true;
					for (TableDef t : dataSource.getMetaData().tables()) {
						if (first)
							first = false;
						else
							tableList.append(",");
						tableList.append(t.getName());
					}
					tableList.append("]");
					DebugFile.writeln(tableList.toString());
					tableList.setLength(0);
					tableList.append("SchemaMetaData views = [");
					first = true;
					for (ViewDef t : dataSource.getMetaData().views()) {
						if (first)
							first = false;
						else
							tableList.append(",");
						tableList.append(t.getName());
					}
					tableList.append("]");
					DebugFile.writeln(tableList.toString());
				} catch (Exception e) {
					if (DebugFile.trace) {
						DebugFile.writeln(e.getClass().getName() + " " + e.getMessage());
					}
				}
			}
			throw new JDOUserException("MapRecord constructor. Table or view " + name + " not found");
		}
		return vdef;
	}
}
