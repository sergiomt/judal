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

/**
 * Implementation of a table definition
 * @author Sergio Montoro Ten
 */
public class TableDef extends ViewDef {

	private static final long serialVersionUID = 10000l;

	private String creationTimestampColumnName;

	/**
	 * <p>Create an empty TableDef.</p>
	 * @param name String TableDef name
	 */
	public TableDef(String name) {
		super(name);
		creationTimestampColumnName = null;
	}

	/**
	 * <p>Create TableDef and add the given columns.</p>
	 * If one or more ColumnDefs are defined as belonging to the primary key,
	 * then the primary key of this TableDef will be automatically set.
	 * @param name String TableDef name
	 * @param columnDefs ColumnDef&hellip;
	 */
	public TableDef(String name, ColumnDef... columnDefs) {
		super(name, columnDefs);
		creationTimestampColumnName = null;
		autoSetPrimaryKey();
	}

	/**
	 * <p>Create TableDef and add the given columns.</p>
	 * If one or more ColumnDefs are defined as belonging to the primary key,
	 * then the primary key of this TableDef will be automatically set.
	 * @param name String TableDef name
	 * @param columnDefs Collection&lt;ColumnDef&gt;
	 */
	public TableDef(String name, Collection<ColumnDef> columnDefs) {
		super(name, columnDefs);
		creationTimestampColumnName = null;
		autoSetPrimaryKey();
	}

	/**
	 * <p>Create TableDef by cloning another one.</p>
	 * @param source TableDef
	 */
	public TableDef(TableDef source) {
		super(source);
		creationTimestampColumnName = source.creationTimestampColumnName;
	}

	/**
	 * @return TableDef clone of <b>this</b>
	 */
	@Override
	public TableDef clone() {
		return new TableDef(this);
	}
	
	/**
	 * <p>Add a column which belongs to the primary key.</p>
	 * @param columnFamilyName String. Optional. Maybe <b>null</b> or empty String.
	 * @param columnName String
	 * @param columnType int from java.sql.Types
	 * @param maxLength int
	 */
	public void addPrimaryKeyColumn(String columnFamilyName, String columnName, int columnType, int maxLength) {
		addColumnMetadata (columnFamilyName, columnName, columnType, maxLength, 0, false, null, null, null, true);
	}

	/**
	 * <p>Add a column which belongs to the primary key.</p>
	 * The column precision will be automatically set by calling ColumnDef.getDefaultPrecision()
	 * @param columnFamilyName String. Optional. Maybe <b>null</b> or empty String.
	 * @param columnName String
	 * @param columnType int from java.sql.Types
	 */
	public void addPrimaryKeyColumn(String columnFamilyName, String columnName, int columnType) {
		addColumnMetadata (columnFamilyName, columnName, columnType, ColumnDef.getDefaultPrecision(columnType), 0, false, null, null, null, true);
	}

	/**
	 * <p>Get the name of the column which will never be modified by update writes of a Record.</p>
	 * @return String Column Name or <b>null</b> if no column in the Record is prevented from being modified upon update.
	 */
	public String getCreationTimestampColumnName() {
		return creationTimestampColumnName;
	}

	/**
	 * <p>If not <b>null</b> the value held in this column must not be modified when a Record is updated.</p>
	 * It is up to the implementation of the DataSource and its child object to implement the behavior.
	 * @param columnName String Column Name or <b>null</b> if no column in the Record must be prevented from being modified on update.
	 */
	public void setCreationTimestampColumnName(String columnName) {
		creationTimestampColumnName = columnName;
	}
	
	/**
	 * Get representation of this TableDef as a JDO XML class
	 * @return String of the form<br/>&lt;class name="<i>class_name</i>" table="<i>table_name</i>" /&gt; &hellip; &lt;/class&gt;
	 */
	public String toJdoXml() {
		StringBuilder builder = new StringBuilder();
		builder.append("    <class");
		if (getRecordClassName()!=null)
			builder.append(" name=\"").append(getRecordClassName()).append("\" ");
		else
			builder.append(" name=\"\" ");
		builder.append(" table=\"").append(getName()).append("\" />\n");
		for (ColumnDef cdef : getColumns())
			builder.append(cdef.toJdoXml()).append("\n");
		if (getPrimaryKeyMetadata()!=null)
			builder.append(getPrimaryKeyMetadata().toJdoXml()).append("\n");
		if (getForeignKeys()!=null)
			for (ForeignKeyDef fk : getForeignKeys())
				builder.append(fk.toJdoXml()).append("\n");
		if (getUniques()!=null)
			for (UniqueIndexDef idx : getUniques())
				builder.append(idx.toJdoXml()).append("\n");
		if (getIndices()!=null)
			for (NonUniqueIndexDef idx : getIndices())
				builder.append(idx.toJdoXml()).append("\n");
		builder.append("    </class>");
		return builder.toString();
	}
}
