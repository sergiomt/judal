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

import java.util.Arrays;

import javax.jdo.annotations.ForeignKeyAction;
import javax.jdo.metadata.ColumnMetadata;
import javax.jdo.metadata.FieldMetadata;
import javax.jdo.metadata.ForeignKeyMetadata;
import javax.jdo.metadata.MemberMetadata;
import javax.jdo.metadata.PropertyMetadata;

/**
 * JDO ForeignKeyMetadata interface implementation
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class ForeignKeyDef extends ExtendableDef implements ForeignKeyMetadata {

	private boolean unique;
	private boolean deferred;
	private String name, table;
	private ForeignKeyAction delAction;
	private ForeignKeyAction updAction;
	private ColumnMetadata[] columns;

	public ForeignKeyDef() {
		unique = false;
		updAction = ForeignKeyAction.DEFAULT;
		delAction = ForeignKeyAction.DEFAULT;
		clear();
	}

	public void clear() {
		columns = new ColumnMetadata[0];
	}
	
	@Override
	public ColumnMetadata[] getColumns() {
		return columns;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public int getNumberOfColumns() {
		return columns.length;
	}

	@Override
	public Boolean getDeferred() {
		return deferred;
	}

	@Override
	public ForeignKeyAction getDeleteAction() {
		return delAction;
	}

	@Override
	public String getTable() {
		return table;
	}

	@Override
	public Boolean getUnique() {
		return unique;
	}

	@Override
	public ForeignKeyAction getUpdateAction() {
		return updAction;
	}

	/**
	 * Create a new ColumnMetadata instance and add it to the list of columns of this object
	 * @return ColumnDef
	 */
	@Override
	public ColumnDef newColumnMetadata() {
		ColumnDef colDef = new ColumnDef();
		if (columns.length==0) {
			columns = new ColumnMetadata[]{colDef};
		} else {
			int pos = columns.length;
			columns = Arrays.copyOf(columns, pos+1);
			columns[pos] = colDef;
		}
		return colDef;
	}

	/**
	 * Add an existing ColumnMetadata instance to the column list of this object
	 * @param colDef ColumnMetadata
	 * @return New instance of ColumnDef
	 */
	public ColumnDef addColumn(ColumnMetadata colDef) {
		ColumnDef newCol = newColumnMetadata();
		newCol.setName(colDef.getName());
		newCol.setJDBCType(colDef.getJDBCType());
		newCol.setSQLType(colDef.getSQLType());
		newCol.setLength(colDef.getLength());
		newCol.setScale(colDef.getScale());
		newCol.setAllowsNull(colDef.getAllowsNull());
		newCol.setPosition(colDef.getPosition());
		if (colDef instanceof ColumnDef) {
			ColumnDef columnDef = (ColumnDef) colDef;
			newCol.setFamily(columnDef.getFamily());
			newCol.setType(columnDef.getType());
			newCol.setAutoIncrement(columnDef.getAutoIncrement());
		}
		return newCol;
	}

	/**
	 * @throws JDOUnsupportedOptionException
	 */
	@Override
	public FieldMetadata newFieldMetadata(String name) {
		throw new javax.jdo.JDOUnsupportedOptionException("ForeignKeyMetadata.newFieldMetadata() is not implemented");
	}

	/**
	 * @throws JDOUnsupportedOptionException
	 */
	@Override
	public PropertyMetadata newPropertyMetadata(String arg0) {
		throw new javax.jdo.JDOUnsupportedOptionException("ForeignKeyMetadata.newPropertyMetadata() is not implemented");
	}

	@Override
	public ForeignKeyMetadata setDeferred(boolean defered) {
		deferred = defered;
		return this;
	}

	@Override
	public ForeignKeyMetadata setDeleteAction(ForeignKeyAction action) {
		delAction = action;
		return this;
	}

	@Override
	public ForeignKeyMetadata setName(String foreignKeyName) {
		name = foreignKeyName;
		return this;
	}

	@Override
	public ForeignKeyMetadata setTable(String tableName) {
		table = tableName;
		return this;
	}

	@Override
	public ForeignKeyMetadata setUnique(boolean isUnique) {
		unique = isUnique;
		return this;
	}

	@Override
	public ForeignKeyMetadata setUpdateAction(ForeignKeyAction action) {
		updAction = action;
		return this;
	}

	@Override
	public MemberMetadata[] getMembers() {
		return null;
	}

	@Override
	public int getNumberOfMembers() {
		return 0;
	}

	public String toJdoXml() {
		StringBuilder builder = new StringBuilder();
		builder.append("      <foreign-key");
		builder.append(" table=\""+getTable()+"\"");
		builder.append(" deferred=\""+(getDeferred() ? "true" : "false")+"\"");
		if (getDeleteAction()!=null)
			builder.append(" delete-action=\""+getDeleteAction().toString()+"\"");
		if (getUpdateAction()!=null)
			builder.append(" update-action=\""+getUpdateAction().toString()+"\"");
		builder.append(" unique=\""+(getUnique() ? "true" : "false")+"\"");
		if (getName()!=null && getName().length()>0)
			builder.append(" name=\""+getName()+"\"");
		builder.append(">\n");
		for (ColumnMetadata cdef : getColumns())
			builder.append("        <column name=\""+cdef.getName()+"\" />\n");
		builder.append("      </foreign-key>");
		return builder.toString();
	}
	
}
