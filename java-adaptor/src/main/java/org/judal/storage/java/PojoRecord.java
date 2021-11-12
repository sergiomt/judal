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

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;

import java.util.Map;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Set;
import java.util.TreeSet;

import javax.jdo.JDOException;
import javax.jdo.JDOUserException;
import javax.jdo.JDOUnsupportedOptionException;

import org.judal.metadata.ViewDef;
import org.judal.storage.ConstraintsChecker;
import org.judal.storage.FieldHelper;
import org.judal.storage.table.TableDataSource;
import org.judal.storage.table.impl.AbstractRecord;

import com.knowgate.dateutils.DateHelper;
import com.knowgate.debug.DebugFile;

public class PojoRecord extends AbstractRecord implements JavaRecord {

	private static final long serialVersionUID = 10000L;

	private ArrayList<Field> persistentFields;
	
	public PojoRecord(ViewDef tableDefinition) {
		this(tableDefinition, null, null);
	}
	
	public PojoRecord(ViewDef tableDefinition, ConstraintsChecker constraintsChecker) {
		this(tableDefinition, null, constraintsChecker);
	}

	public PojoRecord(ViewDef tableDefinition, FieldHelper fieldHelper) {
		this(tableDefinition, fieldHelper, null);
	}
	
	public PojoRecord(ViewDef tableDefinition, FieldHelper fieldHelper, ConstraintsChecker constraintsChecker) {
		super(tableDefinition, fieldHelper, constraintsChecker);
		persistentFields =  null;
	}
	
	public PojoRecord(TableDataSource dataSource, String tableName) throws JDOException {
		this(dataSource, tableName, null, null);
	}

	public PojoRecord(TableDataSource dataSource, String tableName, FieldHelper fieldHelper) throws JDOException {
		this(dataSource, tableName, fieldHelper, null);
	}
	
	public PojoRecord(TableDataSource dataSource, String tableName, ConstraintsChecker constraintsChecker) throws JDOException {
		this(dataSource, tableName, null, constraintsChecker);
	}

	public PojoRecord(TableDataSource dataSource, String tableName, FieldHelper fieldHelper, ConstraintsChecker constraintsChecker) throws JDOException {
		super(dataSource, tableName, fieldHelper, constraintsChecker);
		persistentFields =  null;
	}

	@Override
	public Map<String, Object> asMap() {
		ArrayList<Field> fields = getPersistentFields();
		HashMap<String,Object> retval = new HashMap<>(fields.size()*2);
		for (Field fld : fields)
			retval.put(fld.getName(), get(fld.getName()));
		return retval;
	}

	@Override
	public Set<java.util.Map.Entry<String, Object>> entrySet() {
		return asMap().entrySet();
	}

	@Override
	@SuppressWarnings("unchecked")
	public java.util.Map.Entry<String, Object>[] asEntries() {
		return entrySet().toArray(new java.util.Map.Entry[getPersistentFields().size()]);
	}
	
	private ArrayList<Field> getPersistentFields() {
		if (null==persistentFields) {
			Field[] declaredFields = getClass().getDeclaredFields();
			if (null==declaredFields) {
				if (DebugFile.trace)
					DebugFile.writeln("PojoRecord.getPersistentFields() NullPointerException getClass().getDeclaredFields()  returned null for " + getClass());
			} else {
				DebugFile.writeln("found " + String.valueOf(declaredFields.length) + " declared fields");
			}
			persistentFields = new ArrayList<Field>(declaredFields.length);
			for (Field fld : declaredFields) {
				final int mods = fld.getModifiers();
				if (!Modifier.isFinal(mods) && !Modifier.isTransient(mods) && !Modifier.isStatic(mods)) {
					persistentFields.add(fld);					
				}
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
			final boolean accesible = fld.isAccessible();
			fld.setAccessible(true);
			formerValue = fld.get(this);
			castAndSet(fld, obj);
			fld.setAccessible(accesible);
		} catch (IllegalAccessException neverthrown) { }
		return formerValue;
	}

	@Override
	public Object put(String colname, byte[] bytearray) throws IllegalArgumentException {
		Object formerValue = null;
		for (Field fld : getPersistentFields()) {
			if (fld.getName().equalsIgnoreCase(colname)) {
				try {
					final boolean accesible = fld.isAccessible();
					fld.setAccessible(true);
					formerValue = fld.get(this);
					fld.set(this, bytearray);
					fld.setAccessible(accesible);
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
		throw new JDOUnsupportedOptionException("PojoRecord.setValue()");
	}

	/**
	 * This method is not supported by PojoRecord and will always raise an exception
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
		if (obj==null) {
			for (Field fld : getPersistentFields())
				if (get(fld.getName())==null)
					return true;
			return false;
		} else {
			for (Field fld : getPersistentFields())
				if (obj.equals(get(fld.getName())))
					return true;
			return false;
		}
	}

	@Override
	public Object get(Object colname) throws JDOUserException {
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
			final boolean accesible = col.isAccessible();
			col.setAccessible(true);
			Class<?> ctype = col.getType();
			try {				
				if (ctype==null) {
					if (DebugFile.trace)
						DebugFile.writeln("PojoRecord.get() Cannot determine type of column " + columnName);
					throw new JDOUserException("PojoRecord.get() cannot determine type of column "+ columnName);
				}
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
				if (DebugFile.trace)
					DebugFile.writeln("PojoRecord.get() " + e.getClass().getName() + " " + e.getMessage());
				try { col.setAccessible(accesible); } catch (Exception ignore) { }
				throw new JDOUserException("PojoRecord.get("+colname+")");
			}
		} else {
			if (DebugFile.trace)
				DebugFile.writeln("PojoRecord.get() Column not found " + columnName);
		}
		return retval;
	}

	@Override
	public Map<String,String> getMap(String sKey) throws ClassCastException, InstantiationException, IllegalAccessException, IllegalArgumentException, NoSuchMethodException, SecurityException, ClassNotFoundException {
		return getMap(sKey);
	} // getMap
	
	@Override
	public boolean isEmpty() {
		return false;
	}

	@Override
	public Set<String> keySet() {
		TreeSet<String> keys = new TreeSet<>();
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
					final boolean accesible = fld.isAccessible();
					fld.setAccessible(true);
					castAndSet(fld, value);
					fld.setAccessible(accesible);
				} catch (IllegalArgumentException | IllegalAccessException e) {
					throw new JDOUserException("PojoRecord.put("+colname+","+value+") " + e.getClass().getName() + " " + e.getMessage());
				}
				break;
			}
		}
		return retval;
	}

	private void castAndSet(Field fld, Object value) throws IllegalArgumentException, IllegalAccessException, ClassCastException {
		Class<?> cls = fld.getType();
		Object castedValue = value;
		if (java.util.Date.class.equals(cls))
			castedValue = DateHelper.toDate(value);
		else if (java.util.Calendar.class.isAssignableFrom(cls))
			castedValue = DateHelper.toCalendar(value);
		else if (java.sql.Timestamp.class.equals(cls))
			castedValue = DateHelper.toTimestamp(value);
		else if (java.sql.Date.class.equals(cls))
			castedValue = DateHelper.toSQLDate(value);
		fld.set(this, castedValue);
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
		for (Field fld : getPersistentFields()) {
			final boolean accesible = fld.isAccessible();
			fld.setAccessible(true);
			Class<?> ctype = fld.getType();
			try {
				if (ctype.equals(boolean.class))
					fld.setBoolean(this, false);
				else if (ctype.equals(short.class))
					fld.setShort(this, (short) 0);
				else if (ctype.equals(int.class))
					fld.setInt(this, 0);
				else if (ctype.equals(long.class))
					fld.setLong(this, 0l);
				else if (ctype.equals(float.class))
					fld.setFloat(this, 0f);
				else if (ctype.equals(double.class))
					fld.setDouble(this, 0d);
				else if (ctype.equals(byte.class))
					fld.setByte(this, (byte) 0);
				else if (ctype.equals(char.class))
					fld.setChar(this, (char) 0);
				else
					fld.set(this, null);
			} catch (IllegalArgumentException | IllegalAccessException e) {
				throw new JDOUserException("PojoRecord.clear() " + e.getClass().getName() + " " + e.getMessage());
			}
			fld.setAccessible(accesible);
		}
	}

}
