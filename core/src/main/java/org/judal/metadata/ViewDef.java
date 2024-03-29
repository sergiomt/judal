package org.judal.metadata;

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

import java.util.Collection;

import javax.jdo.JDOUserException;

/**
 * View metadata
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class ViewDef extends TypeDef implements SelectableDef {

	private static final long serialVersionUID = 10000l;

	private Class recordClass;
	private String recordClassName;
	private String alias;
	private String description;

	/**
	 * <p>Create ViewDef.</p>
	 * @param aliasedName String View names can be aliased.
	 * Aliased names have the form <i>name</i>.<i>alias</i> or <i>name</i> AS <i>alias</i> where both <i>name</i> and/or <i>alias</i> can be enclosed in double quotes.
	 * @throws JDOUserException
	 */
	public ViewDef(String aliasedName) throws JDOUserException {
		recordClass = null;
		recordClassName = null;
		NameAlias nameAndAlias = NameAlias.parse(aliasedName);
		setName(nameAndAlias.getName());
		setAlias(nameAndAlias.getAlias());
		setTable(nameAndAlias.getName());
	}

	/**
	 * <p>Create ViewDef and add columns.</p>
	 * @param aliasedName String View names can be aliased.
	 * @param columnDefs ColumnDef&hellip;
	 * Aliased names have the form <i>name</i>.<i>alias</i> or <i>name</i> AS <i>alias</i> where both <i>name</i> and/or <i>alias</i> can be enclosed in double quotes.
	 * @throws JDOUserException
	 */
	public ViewDef(String aliasedName, ColumnDef... columnDefs) {
		this(aliasedName);
		for (ColumnDef c : columnDefs)
			addColumnMetadata(c);
	}

	/**
	 * <p>Create ViewDef and add columns.</p>
	 * @param aliasedName String View names can be aliased.
	 * @param columnDefs Collection&lt;ColumnDef&gt;
	 * Aliased names have the form <i>name</i>.<i>alias</i> or <i>name</i> AS <i>alias</i> where both <i>name</i> and/or <i>alias</i> can be enclosed in double quotes.
	 * @throws JDOUserException
	 */
	public ViewDef(String aliasedName, Collection<ColumnDef> columnDefs) {
		this(aliasedName);
		for (ColumnDef c : columnDefs)
			addColumnMetadata(c);
	}
	
	/**
	 * <p>Create ViewDef by cloning another ViewDef.</p>
	 * @param source ViewDef
	 */
	public ViewDef(ViewDef source) {
		super(source);
		alias = source.alias;
		recordClass = source.recordClass;
		recordClassName  = source.recordClassName;		
		setName(source.getName());
		setAlias(source.getAlias());
		setDescription(source.getDescription());
		setTable(source.getTable());
	}

	/**
	 * Walk the list of columns of this ViewDef and auto-generate the corresponding PrimaryKeyMetadata.
	 */
	public void autoSetPrimaryKey() {
		if (getPrimaryKeyMetadata()!=null)
			getPrimaryKeyMetadata().clear();
		else
			pk = new PrimaryKeyDef();
		for (ColumnDef c : getColumns()) {
			if (c.isPrimaryKey()) {
				ColumnDef newPkCol = getPrimaryKeyMetadata().newColumnMetadata();
				newPkCol.setFamily(c.getFamily());
				newPkCol.setName(c.getName());
				newPkCol.setType(c.getType());
				newPkCol.setJDBCType(c.getJDBCType());
				newPkCol.setSQLType(c.getSQLType());
				newPkCol.setPosition(c.getPosition());
				newPkCol.setLength(c.getLength());
				newPkCol.setScale(c.getScale());
				newPkCol.setAllowsNull(c.getAllowsNull());
				newPkCol.setAutoIncrement(c.getAutoIncrement());
			}
		} // next
	}

	/**
	 * @return ViewDef
	 */
	@Override
	public ViewDef clone() {
		return new ViewDef(this);
	}

	/**
	 * <p>Set alias.</p>
	 * @param alias String
	 * @return ViewDef <b>this</b>
	 */
	public ViewDef setAlias(String alias) throws JDOUserException {
		if (getUnmodifiable())
			throw new JDOUserException(getClass().getName().substring(getClass().getName().lastIndexOf('.')+1)+" is set to unmodifiable");
		this.alias = alias;
		return this;
	}

	/**
	 * <p>Get view alias or view name if this view is not aliased.</p>
	 * @return String
	 */
	public String getAlias() {
		return alias==null || alias.length()==0 ? getName() : alias;
	}

	/**
	 * <p>Set description.</p>
	 * @param description String
	 * @return ViewDef <b>this</b>
	 */
	public ViewDef setDescription(String description) throws JDOUserException {
		if (getUnmodifiable())
			throw new JDOUserException(getClass().getName().substring(getClass().getName().lastIndexOf('.')+1)+" is set to unmodifiable");
		this.description = description;
		return this;
	}

	/**
	 * <p>Get description.</p>
	 * @return String
	 */
	public String getDescription() {
		return description;
	}

	/**
	 * <p>Set the class of Record object that must be used to iterate this View.</p>
	 * This method will only effectively set the class if it can be loaded by calling Class.forName()
	 * else the record class will be set to <b>null</b>.
	 * @param className String Fully qualified class name
	 * @return ViewDef <b>this</b>
	 */
	public ViewDef setRecordClassName(String className) {
		recordClassName = className;
		try {
			recordClass = Class.forName(className);
		} catch (ClassNotFoundException cnfe) {
			recordClass = null;
		}
		return this;
	}

	/**
	 * @return String
	 */
	public String getRecordClassName() {
		return recordClassName;
	}

    /**
	 * <p>Set the class of Record object that must be used to iterate this View.</p>
     * @param classRecord Class
	 * @return ViewDef <b>this</b>
     */
	@SuppressWarnings("rawtypes")
	public ViewDef setRecordClass(Class classRecord) {
		recordClass = classRecord;
		recordClassName = classRecord.getName();
		return this;
	}

	/**
	 * @return Class
	 * @throws ClassNotFoundException if the class of Record set cannot be loaded by mean of calling Class.forName()
	 */
	@SuppressWarnings("rawtypes")
	public Class getRecordClass() throws ClassNotFoundException {
		if (recordClass==null && recordClassName!=null)
			recordClass = Class.forName(recordClassName);
		if (recordClass==null)
			throw new ClassNotFoundException("Class not found "+recordClassName);
		return recordClass;
	}
	
	/**
	 * <p>Add column to this ViewDef</p>
	 * @param columnFamilyName String. Optional.
	 * @param columnName String
	 * @param columnType int one of java.sql.Types
	 * @param maxLength int
	 * @param isNullable boolean
	 * @param indexType NonUniqueIndexDef.Type
	 * @throws JDOUserException if this ViewDef is unmodifiable 
	 */
	public void addColumnMetadata(String columnFamilyName, String columnName, int columnType, int maxLength, boolean isNullable, NonUniqueIndexDef.Type indexType) throws JDOUserException {
		if (getUnmodifiable())
			throw new JDOUserException(getClass().getName().substring(getClass().getName().lastIndexOf('.')+1)+" is set to unmodifiable");
		addColumnMetadata(columnFamilyName, columnName, columnType, maxLength, 0, isNullable, indexType, null, null);
	}

	/**
	 * <p>Add column to this ViewDef</p>
	 * @param columnFamilyName String. Optional.
	 * @param columnName String
	 * @param columnType int one of java.sql.Types
	 * @param maxLength int
	 * @param isNullable boolean
	 * @throws JDOUserException if this ViewDef is unmodifiable 
	 */
	public void addColumnMetadata(String columnFamilyName, String columnName, int columnType, int maxLength, boolean isNullable) throws JDOUserException {
		if (getUnmodifiable())
			throw new JDOUserException(getClass().getName().substring(getClass().getName().lastIndexOf('.')+1)+" is set to unmodifiable");
		addColumnMetadata(columnFamilyName, columnName, columnType, maxLength, 0, isNullable, null, null, null);
	}

	/**
	 * <p>Add column to this ViewDef</p>
	 * @param columnFamilyName String. Optional.
	 * @param columnName String
	 * @param columnType int one of java.sql.Types
	 * @param precision int
	 * @param decimalDigits int
	 * @param isNullable boolean
	 * @throws JDOUserException if this ViewDef is unmodifiable 
	 */
	public void addColumnMetadata(String columnFamilyName, String columnName, int columnType, int precision, int decimalDigits, boolean isNullable) throws JDOUserException {
		if (getUnmodifiable())
			throw new JDOUserException(getClass().getName().substring(getClass().getName().lastIndexOf('.')+1)+" is set to unmodifiable");
		addColumnMetadata(columnFamilyName, columnName, columnType, precision, decimalDigits, isNullable, null, null, null);
	}
	
	/**
	 * <p>Add column to this ViewDef</p>
	 * @param columnFamilyName String. Optional.
	 * @param columnName String
	 * @param columnType int one of java.sql.Types
	 * @param isNullable boolean
	 * @param indexType NonUniqueIndexDef.Type
	 * @throws JDOUserException if this ViewDef is unmodifiable 
	 */
	public void addColumnMetadata(String columnFamilyName, String columnName, int columnType, boolean isNullable, NonUniqueIndexDef.Type indexType) throws JDOUserException {
		if (getUnmodifiable())
			throw new JDOUserException(getClass().getName().substring(getClass().getName().lastIndexOf('.')+1)+" is set to unmodifiable");
		addColumnMetadata(columnFamilyName, columnName, columnType, ColumnDef.getDefaultPrecision(columnType), 0, isNullable, indexType, null, null);
	}
	
	/**
	 * <p>Add column to this ViewDef</p>
	 * @param columnFamilyName String. Optional.
	 * @param columnName String
	 * @param columnType int one of java.sql.Types
	 * @param isNullable boolean
	 * @throws JDOUserException if this ViewDef is unmodifiable 
	 */
	public void addColumnMetadata(String columnFamilyName, String columnName, int columnType, boolean isNullable) throws JDOUserException {
		if (getUnmodifiable())
			throw new JDOUserException(getClass().getName().substring(getClass().getName().lastIndexOf('.') + 1) + " is set to unmodifiable");
		addColumnMetadata(columnFamilyName, columnName, columnType, ColumnDef.getDefaultPrecision(columnType), 0, isNullable, null, null, null);
	}

}
