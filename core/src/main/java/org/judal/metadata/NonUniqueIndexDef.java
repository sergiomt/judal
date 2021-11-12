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

import java.util.List;
import java.util.LinkedList;

import javax.jdo.metadata.ColumnMetadata;

import org.judal.metadata.IndexDef;

/**
 * Metadata of a non-unique index
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class NonUniqueIndexDef extends IndexDef {

	private static final long serialVersionUID = 10000L;

	public NonUniqueIndexDef () {
		super(null, null, new LinkedList<ColumnMetadata>(), IndexDef.Type.ONE_TO_MANY);
	}

	public NonUniqueIndexDef (String sTableName, String sIndexName, List<ColumnMetadata> oIndexColumns, Type eIndexType) {
		super(sTableName, sIndexName, oIndexColumns, eIndexType);
	}

	public NonUniqueIndexDef (String sTableName, String sIndexName, ColumnMetadata column, Type eIndexType) {
		super(sTableName, sIndexName, column, eIndexType);
	}

	public NonUniqueIndexDef (String sTableName, String sIndexName, ColumnMetadata column, Type eIndexType, Using eUsingType) {
		super(sTableName, sIndexName, column, eIndexType, eUsingType);
	}

	public NonUniqueIndexDef (String sTableName, String sIndexName, ColumnMetadata[] aIndexColumns, Type eIndexType) {
		super(sTableName, sIndexName, aIndexColumns, eIndexType);
	}

	public NonUniqueIndexDef (String sTableName, String sIndexName, ColumnMetadata[] aIndexColumns, Type eIndexType, Using eIndexUsing) {
		super(sTableName, sIndexName, aIndexColumns, eIndexType, eIndexUsing);
	}

	/**
	 * @param name String
	 * @return NonUniqueIndexDef <b>this</b>
	 */
	@Override
	public NonUniqueIndexDef setName(String name) {
		super.setName(name);
		return this;
	}

	/**
	 * @param tableName String
	 * @return NonUniqueIndexDef <b>this</b>
	 */
	@Override
	public NonUniqueIndexDef setTable(String tableName) {
		super.setTable(tableName);
		return this;
	}
	
	/**
	 * @param unique boolean
	 */
	public NonUniqueIndexDef setUnique(boolean unique) {
		super.setUnique(unique);
		return this;
	}

	/**
	 * <p>get JDO XML representation of this NonUniqueIndexDef, like:</p>
	 * &lt;index name="<i>indexName</i>" table="<i>tableName</i>" unique="false"&gt;<br/>
	 * &lt;column name="<i>column1</i>"/&gt;<br/>
	 * &lt;column name="<i>column2</i>"/&gt;<br/>
	 * &lt;index/&gt;
	 * @return String
	 */
	@Override
	public String toJdoXml() {
		StringBuilder builder = new StringBuilder();
		builder.append("      <index");
		if (getName()!=null)
			builder.append(" name=\""+getName()+"\"");
		if (getTable()!=null)
			builder.append(" table=\""+getTable()+"\"");
		builder.append(" unique=\""+(getUnique() ? "true" : "false")+"\">\n");
		for (ColumnMetadata cdef : getColumns())
			builder.append("        <column name=\""+cdef.getName()+"\" />\n");
		builder.append("      </index>");
		return builder.toString();		
	}

}
