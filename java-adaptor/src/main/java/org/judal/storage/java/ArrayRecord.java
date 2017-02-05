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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import javax.jdo.JDOException;
import javax.jdo.JDOUserException;

import org.judal.metadata.ColumnDef;
import org.judal.metadata.TableDef;
import org.judal.serialization.BytesConverter;
import org.judal.storage.AbstractRecord;
import org.judal.storage.TableDataSource;

import com.knowgate.typeutils.TypeResolver;

/**
 * Implementation of Record interface using an array to hold column values
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class ArrayRecord extends AbstractRecord implements JavaRecord {

	private static final long serialVersionUID = 10000l;

	private Object[] values;
	private Map<String, Integer> columnsMap;

	/**
	 * Constructor
	 */
	public ArrayRecord(TableDef tableDefinition) {
		super(tableDefinition);
		columnsMap = null;
		values = new Object[getTableDef().getNumberOfColumns()];
		Arrays.fill(values, null);
	}

	/**
	 * Constructor
	 * @throws JDOException 
	 */
	public ArrayRecord(TableDataSource dataSource, String tableName) throws JDOException {
		super(dataSource, tableName);
		columnsMap = null;
		values = new Object[getTableDef().getNumberOfColumns()];
		Arrays.fill(values, null);
	}

	public ArrayRecord(TableDataSource dataSource, String tableName, String... columnNames) throws JDOException {
		super(new TableDef(tableName, dataSource.getTableDef(tableName).filterColumns(columnAliases(columnNames))));
		columnsMap = new HashMap<String, Integer>(columnNames.length);
		int pos = 0;
		for (String columnName : columnNames) {
			String columnAlias = getColumnAlias(columnName);
			ColumnDef cdef = getTableDef().getColumnByName(columnAlias);
			if (cdef==null) throw new JDOUserException("Column "+columnAlias+" not found at "+getTableName());
			columnsMap.put(cdef.getName(), ++pos);
		}
		values = new Object[columnNames.length];
		Arrays.fill(values, null);
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

	private int getColumnIndex(String columnName) {
		if (null==columnsMap)
			return getTableDef().getColumnIndex(columnName);
		else
			return columnsMap.get(columnName);
	}

	/**
	 * Get value for column
	 * @param sColName String Column Name
	 * @return Object Column value or <b>null</b> if this record contains no column with such name
	 */
	@Override
	public Object apply(String sColName) {
		int index = getColumnIndex(sColName);
		if (index==-1)
			return null;
		else
			return values[--index];
	}

	/**
	 * Get value for column
	 * @param sColName String Column Name
	 * @return Object Column value or <b>null</b> if this record contains no column with such name
	 */
	@Override
	public Object get(Object sColName) {
		int index = getColumnIndex((String) sColName);
		if (index==-1)
			return null;
		else
			return values[--index];
	}

	@Override
	  public Map<String,String> getMap(String sKey) throws ClassCastException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, ClassNotFoundException {
		return MapFieldHelper.getMap(this, sKey);
	  } // getMap

	@Override
	public Object getValue() throws JDOException {
		boolean isBucket = (values.length==2);
		if (isBucket) {
			int pkIndex = getColumnIndex(getTableDef().getPrimaryKeyMetadata().getColumn());
			int valIndex = (pkIndex==1 ? 2 : 1) - 1;
			return values[valIndex];
		} else {
			return values;
		}
	}

	@Override
	public void setValue(Serializable newvalue) throws JDOException {
		if (newvalue==null) {
			values = new Object[getTableDef().getNumberOfColumns()];
			Arrays.fill(values,  null);
		} else {
			int pkIndex = getColumnIndex(getTableDef().getPrimaryKeyMetadata().getColumn());
			int valIndex = (pkIndex==1 ? 2 : 1) - 1;
			boolean isBucket = (values.length==2);
			if (newvalue.getClass().isArray()) {
				if (isBucket && (newvalue instanceof byte[] || newvalue instanceof char[])) {
					values[valIndex] = newvalue;
				} else {
					try {
						Object[] newvalues = (Object[]) newvalue;					
						values = new Object[getTableDef().getNumberOfColumns()];
						if (values.length<newvalues.length)
							throw new JDOException("Supplied more values ("+String.valueOf(newvalues.length)+") than columns at table "+getTableDef().getName()+"("+String.valueOf(values.length)+")");
						for (int k=0; k<newvalues.length; k++)
							values[k] = newvalues[k];
					} catch (ClassCastException cce) {
						throw new JDOException("Type mismatch. Object[] expected but got "+newvalue.getClass().getName()+" for "+String.valueOf(values.length)+" values");
					}
				}
			} else {
				if (isBucket)
					values[valIndex] = newvalue;
				else
					throw new JDOException("Type mismatch. Array expected but got "+newvalue.getClass().getName());
			}
		}
	}
	
	@Override
	public int size() {
		return values.length;
	}

	@Override
	public boolean isEmpty() {
		for (Object o : values)
			if (o!=null) return false;
		return true;
	}

	@Override
	public boolean containsKey(Object colname) {
		return getColumnIndex((String) colname)!=-1;
	}

	@Override
	public boolean containsValue(Object value) {
		for (Object o : values)
			if (o!=null)
				if (o.equals(value))
					return true;
		return false;
	}

	@Override
	public Set<java.util.Map.Entry<String, Object>> entrySet() {
		HashMap<String,Object> entries = new HashMap<String,Object>();
		int n = 0;
		for (ColumnDef c : getTableDef().getColumns())
			entries.put(c.getName(), values[n++]);
		return entries.entrySet();
	}

	/**
	 * Remove value for column
	 * @param colname String Column Name
	 */
	@Override
	public Object remove(String colname) {
		Object retval = null;
		int index = getColumnIndex(colname);
		if (index!=-1) {
			retval = values[index-1];
			values[index-1] = null;
		}
		return retval;
	}

	/**
	 * Remove value for column
	 * @param colname String Column Name
	 */
	@Override
	public Object remove(Object colname) {
		Object retval = null;
		int index = getColumnIndex((String) colname);
		if (index!=-1) {
			retval = values[index-1];
			values[index-1] = null;
		}
		return retval;
	}

	@Override
	public void clear() {
		Arrays.fill(values, null);		
	}

	/**
	 * 
	 * @colpos int [1..columnCount()]
	 * @obj Object
	 * @throws ArrayIndexOutOfBoundsException
	 */
	@Override
	public Object put(int colpos, Object obj) {
		Object retval = values[colpos-1];
		values[colpos-1] = obj;
		return retval;
	}

	@Override
	public Object put(String colname, byte[] bytes) {
		byte[] retval;
		int index = getColumnIndex(colname) - 1;		
		if (null==values[index])
			retval = null;
		else if (values[index] instanceof byte[])
			retval = Arrays.copyOf((byte[]) values[index], ((byte[]) values[index]).length);
		else 
			retval = BytesConverter.toBytes(values[index], Types.JAVA_OBJECT);
		values[index] = bytes;
		return retval;
	}

	@Override
	public Collection<Object> values() {
		final int len = values.length;
		ArrayList<Object> vals = new ArrayList<Object>(len);
		for (int n=0; n<len; n++)
			vals.add(values[n]);
		return vals;
	}
	
	/**
	 * <p>Set value at internal collection</p>
	 * @param sKey Column Name
	 * @param oObj Field Value
	 * @throws NullPointerException If sKey is <b>null</b>
	 */
	@Override
	public Object put(String sKey, Object oObj) {

		if (sKey==null)
			throw new NullPointerException("ArrayRecord.put(String,Object) field name cannot be null");

		int index = getColumnIndex(sKey) - 1;
		if (index<0)
			throw new IllegalArgumentException("Column "+sKey+" not found at table "+getTableDef().getName());

		Object retval = values[index];

		if (null==oObj) {
			values[index] = null;
		} else if (TypeResolver.isOfStandardType(oObj)) {
			values[index] = oObj;
		} else {
			ColumnDef oCol = getTableDef().getColumnByName(sKey);
			if (oCol!=null) {
				values[index] = oCol.isOfBinaryType() ? getBinaryData(sKey, oObj) : oObj;
			} else {
				throw new IllegalArgumentException("Column "+sKey+" does not exist at "+getTableDef().getName());
			}
		}
		return retval;
	}

	@Override
	public void putAll(Map<? extends String,? extends Object> mValues) {
		for (Entry<? extends String,? extends Object> entry : mValues.entrySet()) {
			put(entry.getKey(), entry.getValue());
		}
	}
	
}
