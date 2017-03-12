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

import java.util.LinkedList;
import java.util.List;

import javax.jdo.metadata.ColumnMetadata;
import javax.jdo.metadata.FieldMetadata;
import javax.jdo.metadata.IndexMetadata;
import javax.jdo.metadata.MemberMetadata;
import javax.jdo.metadata.PropertyMetadata;

/**
 * JDO IndexMetadata interface implementation for an index on a table or view
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public abstract class IndexDef extends ExtendableDef implements IndexMetadata {

	private static final long serialVersionUID = 10000l;

	public enum Type {
		ONE_TO_ONE,
		MANY_TO_ONE,
		ONE_TO_MANY,
		MANY_TO_MANY;
		;		
	}

	public enum Using {
		BITMAP,
		BTREE,
		CLUSTERED,
		GIST,
		;		
	}

	private Type eType;
	private Using eUsing;
	private String sName;
	private String sTable;
	private LinkedList<ColumnMetadata> oColumns;

	public IndexDef (String sTableName, String sIndexName, List<ColumnMetadata> oIndexColumns, Type eIndexType) {
		sTable = sTableName;
		sName = sIndexName;
		eType = eIndexType;
		eUsing = null;
		oColumns = new LinkedList<ColumnMetadata>(oIndexColumns);
	}

	public IndexDef (String sTableName, String sIndexName, ColumnMetadata column, Type eIndexType) {
		sTable = sTableName;
		sName = sIndexName;
		eType = eIndexType;
		eUsing = null;
		oColumns = new LinkedList<ColumnMetadata>();
		oColumns.add(column);
	}

	public IndexDef (String sTableName, String sIndexName, ColumnMetadata column, Type eIndexType, Using eUsingType) {
		sTable = sTableName;
		sName = sIndexName;
		eType = eIndexType;
		eUsing = eUsingType;
		oColumns = new LinkedList<ColumnMetadata>();
		oColumns.add(column);
	}

	public IndexDef (String sTableName, String sIndexName, ColumnMetadata[] aIndexColumns, Type eIndexType) {
		sTable = sTableName;
		sName = sIndexName;
		eType = eIndexType;
		eUsing = null;
		oColumns = new LinkedList<ColumnMetadata>();
		if (null!=aIndexColumns) {
			for (int c=0; c<aIndexColumns.length; c++)
				oColumns.add(aIndexColumns[c]);
		} // fi
	}

	public IndexDef (String sTableName, String sIndexName, ColumnMetadata[] aIndexColumns, Type eIndexType, Using eIndexUsing) {
		sTable = sTableName;
		sName = sIndexName;
		eType = eIndexType;
		eUsing = eIndexUsing;
		oColumns = new LinkedList<ColumnMetadata>();
		if (null!=aIndexColumns) {
			for (int c=0; c<aIndexColumns.length; c++)
				oColumns.add(aIndexColumns[c]);
		} // fi
	}

	@Override
	public String getName() {
		return sName;
	}

	public Type getType() {
		return eType;
	}

	public void setType(Type indexType) {
		eType = indexType;
	}

	public Using getUsing() {
		return eUsing;
	}

	public void setUsing(Using indexUsing) {
		eUsing = indexUsing;
	}

	@Override
	public ColumnMetadata[] getColumns() {
		return oColumns.toArray(new ColumnMetadata[oColumns.size()]);
	}

	@Override
	public boolean getUnique() {
		return eType.equals(Type.ONE_TO_ONE);
	}

	@Override
	public MemberMetadata[] getMembers() {
		return null;
	}

	@Override
	public int getNumberOfMembers() {
		return 0;
	}
	
	@Override
	public int getNumberOfColumns() {
		return oColumns.size();
	}

	@Override
	public String getTable() {
		return sTable;
	}

	@Override
	public ColumnMetadata newColumn() {
		ColumnDef colDef = new ColumnDef();
		oColumns.add(colDef);
		return colDef;
	}

	@Override
	public FieldMetadata newFieldMetadata(String arg0) {
		throw new UnsupportedOperationException("IndexDef does not implement newFieldMetadata()");
	}

	@Override
	public PropertyMetadata newPropertyMetadata(String arg0) {
		return null;
	}

	@Override
	public IndexDef setName(String name) {
		sName = name;
		return this;
	}

	@Override
	public IndexDef setTable(String tableName) {
		sTable = tableName;
		return this;
	}
	
	@Override
	public IndexDef setUnique(boolean unique) {
		if (unique)
			eType = Type.ONE_TO_ONE;
		else
			eType = Type.ONE_TO_MANY;
		return this;
	}

	public abstract String toJdoXml();
	
}
