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

/**
 * Implementation of a table definition
 * @author Sergio Montoro Ten
 */
public class TableDef extends ViewDef {

	private static final long serialVersionUID = 10000l;

	private String creationTimestampColumnName;

	public TableDef(String name) {
		super(name);
		creationTimestampColumnName = null;
	}

	public TableDef(String name, ColumnDef... columnDefs) {
		super(name, columnDefs);
		creationTimestampColumnName = null;
		autoSetPrimaryKey();
	}

	public TableDef(String name, Collection<ColumnDef> columnDefs) {
		super(name, columnDefs);
		creationTimestampColumnName = null;
		autoSetPrimaryKey();
	}

	public TableDef(TableDef source) {
		super(source);
		creationTimestampColumnName = source.creationTimestampColumnName;
	}

	@Override
	public TableDef clone() {
		return new TableDef(this);
	}
	
	public void addPrimaryKeyColumn(String columnFamilyName, String columnName, int columnType, int maxLength) {
		addColumnMetadata (columnFamilyName, columnName, columnType, maxLength, 0, false, null, null, null, true);
	}

	public void addPrimaryKeyColumn(String columnFamilyName, String columnName, int columnType) {
		addColumnMetadata (columnFamilyName, columnName, columnType, ColumnDef.getDefaultPrecision(columnType), 0, false, null, null, null, true);
	}

	public void autoSetPrimaryKey() {
		if (getPrimaryKeyMetadata()!=null) {
			getPrimaryKeyMetadata().clear();
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
			}
		}
	}

	public String getCreationTimestampColumnName() {
		return creationTimestampColumnName;
	}

	public void setCreationTimestampColumnName(String columnName) {
		creationTimestampColumnName = columnName;
	}
	
	/**
	 * Get representation of this TableDef as a JDO XML class
	 * @return String of the form &lt;class name="<i>class_name</i>" table="<i>table_name</i>" /&gt; &hellip; &lt;/class&gt;
	 */
	public String toJdoXml() {
		StringBuilder builder = new StringBuilder();
		builder.append("    <class");
		if (getRecordClassName()!=null)
			builder.append(" name=\"").append(getRecordClassName()).append("\" ");
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
