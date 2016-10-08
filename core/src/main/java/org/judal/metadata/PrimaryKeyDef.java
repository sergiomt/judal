package org.judal.metadata;

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

import java.util.Arrays;

import javax.jdo.JDOException;
import javax.jdo.metadata.ColumnMetadata;
import javax.jdo.metadata.PrimaryKeyMetadata;

/**
 * JDO PrimaryKeyMetadata interface implementation
 * @author Sergio Montoro Ten
 *
 */
public class PrimaryKeyDef extends ExtendableDef implements PrimaryKeyMetadata {

	private String name;
	private ColumnMetadata[] columns;

	public PrimaryKeyDef() {
		clear();
	}

	/**
	 * Set name to <b>null</b> and remove all columns from this primary key metadata
	 */
	public void clear() {
		name = null;
		columns = new ColumnMetadata[0];
	}

	/**
	 * @return String Name of the first column of this primary key, or <b>null</b> if this primary key has no columns.
	 */
	@Override
	public String getColumn() {
		if (columns.length>0)
			return columns[0].getName();
		else
			return null;
	}

	/**
	 * @return ColumnMetadata[] Array of columns of this primary key.
	 */
	@Override
	public ColumnMetadata[] getColumns() {
		return columns;
	}

	/**
	 * @return String Primary key name.
	 */
	@Override
	public String getName() {
		return name;
	}

	/**
	 * @return int Number of columns.
	 */
	@Override
	public int getNumberOfColumns() {
		return columns.length;
	}

	/**
	 * Create a new ColumnMetadata instance and add it as next column of this primary key.
	 * @return ColumnDef Added ColumnMetadata
	 */
	@Override
	public ColumnDef newColumnMetadata() {
		ColumnDef colDef = new ColumnDef();
		colDef.setPrimaryKey(true);
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
	 * Add a copy of an existing ColumnMetadata as next column of this primary key.
	 * @param colDef ColumnMetadata
	 * @return New ColumnDef instance with the same data as colDef
	 */
	public ColumnDef addColumn(ColumnMetadata colDef) throws JDOException {
		for (ColumnMetadata cmeta : columns)
			if (cmeta.getName().equalsIgnoreCase(colDef.getName()))
				throw new JDOException("Primary key "+getName()+" already contains column "+colDef.getName());
		ColumnDef newCol = newColumnMetadata();
		newCol.setName(colDef.getName());
		newCol.setJDBCType(colDef.getJDBCType());
		newCol.setSQLType(colDef.getSQLType());
		newCol.setLength(colDef.getLength());
		newCol.setScale(colDef.getScale());
		newCol.setAllowsNull(colDef.getAllowsNull());
		newCol.setPosition(colDef.getPosition());
		if (colDef instanceof ColumnDef) {
			ColumnDef cdef = (ColumnDef) colDef;
			newCol.setFamily(cdef.getFamily());
			newCol.setType(cdef.getType());
			newCol.setAutoIncrement(cdef.getAutoIncrement());			
		}
		return newCol;
	}
	
	/**
	 * <p>Add a new column to this primary key.</p>
	 * If given column name is already in this primary key, nothing is done
	 * @param columnName String
	 * @return PrimaryKeyMetadata <b>this</b> object
	 */
	@Override
	public PrimaryKeyMetadata setColumn(String columnName) {
		boolean found = false;
		for (ColumnMetadata colDef : columns) {
			if (colDef.getName().equalsIgnoreCase(columnName)) {
				found = true;
				break;
			}
		}
		if (!found) {
			ColumnMetadata newCol = newColumnMetadata();
			newCol.setName(columnName);
		}
		return this;
	}

	/**
	 * Set name of this primary key
	 * @param pkName String
	 */
	@Override
	public PrimaryKeyMetadata setName(String pkName) {
		name = pkName;
		return this;
	}

	/**
	 * 
	 * @return String of the form &lt;primary-key name="<i>primarykey_name<i>" column="<i>column_name<i>" /&gt; if this primary key has one column
	 * or &lt;primary-key name="<i>primarykey_name<i>"&gt;&lt;column name="<i>column1_name<i>"/&gt;&lt;column name="<i>column2_name<i>"/&gt;&lt;/primary-key&gt;
	 * if this primary key has more or less than one column
	 */
	public String toJdoXml() {
		StringBuilder builder = new StringBuilder();
		builder.append("      <primary-key");
		if (getName()!=null && getName().length()>0)
			builder.append(" name=\""+getName()+"\"");
		if (getNumberOfColumns()==1) {
			builder.append(" column=\""+getColumn()+"\" />");
		} else {
			builder.append(">\n");
			for (ColumnMetadata cdef : getColumns())
				builder.append("        <column name=\""+cdef.getName()+"\" />\n");
			builder.append("      </primary-key>");
		}
		return builder.toString();
	}
}
