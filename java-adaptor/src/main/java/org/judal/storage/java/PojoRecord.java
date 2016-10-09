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
import java.lang.reflect.Field;
import java.util.Map;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;

import javax.jdo.JDOException;
import javax.jdo.JDOUnsupportedOptionException;

import org.judal.metadata.TableDef;
import org.judal.storage.AbstractRecord;
import org.judal.storage.ConstraintsChecker;
import org.judal.storage.TableDataSource;

public class PojoRecord extends AbstractRecord implements JavaRecord {

	private static final long serialVersionUID = 10000l;

	public PojoRecord(TableDef tableDefinition) {
		this(tableDefinition, null);
	}
	
	public PojoRecord(TableDef tableDefinition, ConstraintsChecker constraintsChecker) {
		super(tableDefinition, constraintsChecker);
	}
	
	public PojoRecord(TableDataSource dataSource, String tableName) throws JDOException {
		super(dataSource, tableName, null);
	}

	public PojoRecord(TableDataSource dataSource, String tableName, ConstraintsChecker constraintsChecker) throws JDOException {
		super(dataSource, tableName, constraintsChecker);
	}

	/**
	 * 
	 * @param colpos int [1..columnCount()]
	 * @param obj Object
	 * @throws IllegalArgumentException 
	 * @throws ArrayIndexOutOfBoundsException
	 */	
	@Override
	public Object put(int colpos, Object obj) throws IllegalArgumentException {
		Field fld = getClass().getDeclaredFields()[colpos-1];
		Object formerValue = null;
		try {
			formerValue = fld.get(this);
			fld.set(this, obj);
		} catch (IllegalAccessException neverthrown) { }
		return formerValue;
	}

	@Override
	public Object put(String colname, byte[] bytearray) throws IllegalArgumentException {
		Object formerValue = null;
		for (Field fld : getClass().getDeclaredFields()) {
			if (fld.getName().equalsIgnoreCase(colname)) {
				try {
					formerValue = fld.get(this);
					fld.set(this, bytearray);
				} catch (IllegalAccessException neverthrown) { }
			}
		}
		return formerValue;
	}

	@Override
	public int size() {
		return getClass().getDeclaredFields().length;
	}

	@Override
	public void setValue(Serializable value) throws JDOException {
		// TODO Auto-generated method stub
		
	}

	/**
	 * This method is not supported by PojoRecordand will always raise an exception
	 * @throws JDOUnsupportedOptionException
	 */
	@Override
	public Object getValue() throws JDOException {
		throw new JDOUnsupportedOptionException("PojoRecord.getValue()");
	}

	@Override
	public boolean containsKey(Object colname) {
		String columnName = (String) colname;
		for (Field fld : getClass().getDeclaredFields())
			if (fld.getName().equalsIgnoreCase(columnName))
				return true;
		return false;
	}

	@Override
	public boolean containsValue(Object obj) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Set<java.util.Map.Entry<String, Object>> entrySet() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object get(Object colname) {
		Object retval = null;
		String columnName = (String) colname;
		Field col = null;
		
		for (Field fld : getClass().getDeclaredFields()) {
			if (fld.getName().equalsIgnoreCase(columnName)) {
				col = fld;
				break;
			}
		}
		if (null!=col) {
			Class<?> ctype = col.getType();
			try {				
				if (ctype.equals(String.class))
					retval = col.get(this);
				else if (ctype.equals(boolean.class))
					retval = col.getBoolean(this);
				else if (ctype.equals(short.class))
					retval = col.getShort(this);		
				else if (ctype.equals(int.class))
					retval = col.getInt(this);		
				else if (ctype.equals(long.class))
					retval = col.getLong(this);		
				else if (ctype.equals(float.class))
					retval = col.getFloat(this);		
				else if (ctype.equals(double.class))
					retval = col.getDouble(this);
				else if (ctype.equals(byte.class))
					retval = col.getByte(this);
				else if (ctype.equals(char.class))
					retval = col.getByte(this);
				else
					retval = col.get(this);
			} catch (IllegalArgumentException | IllegalAccessException e) { }
		}
		return retval;
	}

	@Override
	public boolean isEmpty() {
		return false;
	}

	@Override
	public Set<String> keySet() {
		TreeSet<String> keys = new TreeSet<String>();
		for (Field fld : getClass().getDeclaredFields())
			keys.add(fld.getName());
		return keys;
	}

	@Override
	public void putAll(Map<? extends String, ? extends Object> entries) {
		for (Map.Entry<? extends String, ? extends Object> e : entries.entrySet())
			put (e.getKey(), e.getValue());
	}

	@Override
	public Object remove(Object obj) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Collection<Object> values() {
		Field[] fields = getClass().getDeclaredFields();
		ArrayList<Object> vals = new ArrayList<Object>(fields.length);
		for (Field fld : fields)
			vals.add(get(fld.getName()));
		return vals;
	}

	@Override
	public Object apply(String columnName) {
		return get(columnName);
	}

	@Override
	public Object put(String colname, Object value) {
		Object retval = null;
		String columnName = (String) colname;
		for (Field fld : getClass().getDeclaredFields()) {
			if (fld.getName().equalsIgnoreCase(columnName)) {
				retval = get(colname);
				try {
					fld.set(this, value);
				} catch (IllegalArgumentException | IllegalAccessException e) {
				}
				break;
			}
		}
		return retval;
	}

	@Override
	public Object remove(String colname) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void clear() {
		// TODO Auto-generated method stub
		
	}

}
