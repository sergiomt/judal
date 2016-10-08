package org.judal.metadata;

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

import java.util.Collection;
import java.util.regex.Pattern;

import javax.jdo.JDOUserException;

import com.knowgate.debug.DebugFile;

/**
 * View metadata
 * @author Sergio Montoro Ten
 *
 */
public class ViewDef extends TypeDef {

	private Class recordClass;
	private String recordClassName;
	private String alias;

	private static Pattern quotedAliased = Pattern.compile("(\".+\") +(.)", Pattern.CASE_INSENSITIVE);
	private static Pattern unquotedAliased = Pattern.compile("(.+) +(.+)", Pattern.CASE_INSENSITIVE);
	private static Pattern quotedUnAliased = Pattern.compile("\".+\"", Pattern.CASE_INSENSITIVE);
	
	public ViewDef(String aliasedName) throws JDOUserException {
		recordClass = null;
		recordClassName = null;
		NameAlias nameAndAlias = NameAlias.parse(aliasedName);
		setName(nameAndAlias.getName());
		setAlias(nameAndAlias.getAlias());
	}

	public ViewDef(String aliasedName, ColumnDef... columnDefs) {
		this(aliasedName);
		for (ColumnDef c : columnDefs)
			addColumnMetadata(c);
	}

	public ViewDef(String aliasedName, Collection<ColumnDef> columnDefs) {
		this(aliasedName);
		for (ColumnDef c : columnDefs)
			addColumnMetadata(c);
	}
	
	public ViewDef(ViewDef source) {
		super(source);
		alias = source.alias;
		recordClass = source.recordClass;
		recordClassName  = source.recordClassName;		
	}

	@Override
	public ViewDef clone() {
		return new ViewDef(this);
	}
	
	public ViewDef setAlias(String alias) throws JDOUserException {
		if (getUnmodifiable())
			throw new JDOUserException(getClass().getName().substring(getClass().getName().lastIndexOf('.')+1)+" is set to unmodifiable");
		this.alias = alias;
		return this;
	}

	public String getAlias() {
		return alias==null ? getName() : alias;
	}

	public ViewDef setRecordClassName(String className) {
		recordClassName = className;
		try {
			recordClass = Class.forName(className);
		} catch (ClassNotFoundException cnfe) {
			recordClass = null;
		}
		return this;
	}

	public String getRecordClassName() {
		return recordClassName;
	}

	public ViewDef setRecordClass(Class classRecord) {
		recordClass = classRecord;
		return this;
	}

	public Class getRecordClass() throws ClassNotFoundException {
		if (recordClass==null && recordClassName!=null)
			recordClass = Class.forName(recordClassName);
		if (recordClass==null)
			throw new ClassNotFoundException("Class not found "+recordClassName);
		return recordClass;
	}
	
	public void addColumnMetadata(String columnFamilyName, String columnName, int columnType, int maxLength, boolean isNullable, NonUniqueIndexDef.Type indexType) throws JDOUserException {
		if (getUnmodifiable())
			throw new JDOUserException(getClass().getName().substring(getClass().getName().lastIndexOf('.')+1)+" is set to unmodifiable");
		if (DebugFile.trace)
			DebugFile.writeln("ViewDef.addColumnMetadata("+columnFamilyName+","+columnName+","+ColumnDef.typeName(columnType)+","+String.valueOf(maxLength)+","+String.valueOf(isNullable)+","+indexType+")");
		addColumnMetadata(columnFamilyName, columnName, columnType, maxLength, 0, isNullable, indexType, null, null);
	}

	public void addColumnMetadata(String columnFamilyName, String columnName, int columnType, int maxLength, boolean isNullable) throws JDOUserException {
		if (getUnmodifiable())
			throw new JDOUserException(getClass().getName().substring(getClass().getName().lastIndexOf('.')+1)+" is set to unmodifiable");
		if (DebugFile.trace)
			DebugFile.writeln("ViewDef.addColumnMetadata("+columnFamilyName+","+columnName+","+ColumnDef.typeName(columnType)+","+String.valueOf(maxLength)+","+String.valueOf(isNullable)+")");
		addColumnMetadata(columnFamilyName, columnName, columnType, maxLength, 0, isNullable, null, null, null);
	}

	public void addColumnMetadata(String columnFamilyName, String columnName, int columnType, int precision, int decimalDigits, boolean isNullable) throws JDOUserException {
		if (getUnmodifiable())
			throw new JDOUserException(getClass().getName().substring(getClass().getName().lastIndexOf('.')+1)+" is set to unmodifiable");
		if (DebugFile.trace)
			DebugFile.writeln("ViewDef.addColumnMetadata("+columnFamilyName+","+columnName+","+ColumnDef.typeName(columnType)+","+String.valueOf(precision)+","+String.valueOf(decimalDigits)+","+String.valueOf(isNullable)+")");
		addColumnMetadata(columnFamilyName, columnName, columnType, precision, decimalDigits, isNullable, null, null, null);
	}
	
	public void addColumnMetadata(String columnFamilyName, String columnName, int columnType, boolean isNullable, NonUniqueIndexDef.Type indexType) throws JDOUserException {
		if (getUnmodifiable())
			throw new JDOUserException(getClass().getName().substring(getClass().getName().lastIndexOf('.')+1)+" is set to unmodifiable");
		if (DebugFile.trace)
			DebugFile.writeln("ViewDef.addColumnMetadata("+columnFamilyName+","+columnName+","+ColumnDef.typeName(columnType)+String.valueOf(isNullable)+","+indexType+")");
		addColumnMetadata(columnFamilyName, columnName, columnType, ColumnDef.getDefaultPrecision(columnType), 0, isNullable, indexType, null, null);
	}
	
	public void addColumnMetadata(String columnFamilyName, String columnName, int columnType, boolean isNullable) throws JDOUserException {
		if (getUnmodifiable())
			throw new JDOUserException(getClass().getName().substring(getClass().getName().lastIndexOf('.')+1)+" is set to unmodifiable");
		if (DebugFile.trace)
			DebugFile.writeln("ViewDef.addColumnMetadata("+columnFamilyName+","+columnName+","+ColumnDef.typeName(columnType)+","+String.valueOf(isNullable)+")");
		addColumnMetadata(columnFamilyName, columnName, columnType, ColumnDef.getDefaultPrecision(columnType), 0, isNullable, null, null, null);
	}

}
