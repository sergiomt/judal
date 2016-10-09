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
import java.lang.reflect.Modifier;

import java.util.Map;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;

import javax.jdo.JDOException;
import javax.jdo.JDOUserException;
import javax.jdo.JDOUnsupportedOptionException;

import org.judal.metadata.TableDef;
import org.judal.storage.AbstractRecord;
import org.judal.storage.ConstraintsChecker;
import org.judal.storage.TableDataSource;

public class PojoRecord extends AbstractRecord implements JavaRecord {

	private static final long serialVersionUID = 10000l;

	private ArrayList<Field> persistentFields;
	
	public PojoRecord(TableDef tableDefinition) {
		this(tableDefinition, null);
	}
	
	public PojoRecord(TableDef tableDefinition, ConstraintsChecker constraintsChecker) {
		super(tableDefinition, constraintsChecker);
		persistentFields =  null;
	}
	
	public PojoRecord(TableDataSource dataSource, String tableName) throws JDOException {
		this(dataSource, tableName, null);
	}

	public PojoRecord(TableDataSource dataSource, String tableName, ConstraintsChecker constraintsChecker) throws JDOException {
		super(dataSource, tableName, constraintsChecker);
		persistentFields =  null;
	}

	private ArrayList<Field> getPersistentFields() {
		if (null==persistentFields) {
			Field[] declaredFields = getClass().getDeclaredFields();
			persistentFields = new ArrayList<Field>(declaredFields.length);
			for (Field fld : getPersistentFields()) {
				final int mods = fld.getModifiers();
				if (!Modifier.isFinal(mods) && ! Modifier.isTransient(mods) && !Modifier.isStatic(mods))
					persistentFields.add(fld);
			}
		}
		return persistentFields;
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
		Field fld = getPersistentFields().get(colpos-1);
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
		for (Field fld : getPersistentFields()) {
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
		return getPersistentFields().size();
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
		for (Field fld : getPersistentFields())
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
		
		for (Field fld : getPersistentFields()) {
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
			} catch (IllegalArgumentException | IllegalAccessException e) {
				throw new JDOUserException("PojoRecord.get("+colname+")");
			}
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
		for (Field fld : getPersistentFields())
			keys.add(fld.getName());
		return keys;
	}

	@Override
	public void putAll(Map<? extends String, ? extends Object> entries) {
		for (Map.Entry<? extends String, ? extends Object> e : entries.entrySet())
			put (e.getKey(), e.getValue());
	}

	@Override
	public Collection<Object> values() {
		ArrayList<Field> fields = getPersistentFields();
		ArrayList<Object> vals = new ArrayList<Object>(fields.size());
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
		for (Field fld : getPersistentFields()) {
			if (fld.getName().equalsIgnoreCase(columnName)) {
				retval = get(colname);
				try {
					fld.set(this, value);
				} catch (IllegalArgumentException | IllegalAccessException e) {
					throw new JDOUserException("PojoRecord.put("+colname+","+value+")");
				}
				break;
			}
		}
		return retval;
	}

	@Override
	public Object remove(String colname) {
		throw new JDOUnsupportedOptionException("PojoRecord.remove()");	
	}

	@Override
	public Object remove(Object obj) {
		throw new JDOUnsupportedOptionException("PojoRecord.remove()");	
	}
	
	@Override
	public void clear() {
		throw new JDOUnsupportedOptionException("PojoRecord.clear()");	
	}
}
