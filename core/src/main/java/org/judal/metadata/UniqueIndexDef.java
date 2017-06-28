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

import java.util.List;

import javax.jdo.metadata.ColumnMetadata;
import javax.jdo.metadata.UniqueMetadata;

/**
 * Implementation of JDO UniqueMetadata interface
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class UniqueIndexDef extends NonUniqueIndexDef implements UniqueMetadata {

	private static final long serialVersionUID = 10000l;
	private boolean deferred;
	
	public UniqueIndexDef () {
		super(null, null, new LinkedList<ColumnMetadata>(), IndexDef.Type.ONE_TO_ONE);
		super.setUnique(false);
	}
	
	public UniqueIndexDef (String sTableName, String sIndexName, List<ColumnMetadata> oIndexColumns, Type eIndexType) {
		super(sTableName, sIndexName, oIndexColumns, Type.ONE_TO_ONE);
		deferred = false;
	}

	public UniqueIndexDef (String sTableName, String sIndexName, ColumnMetadata column, Type eIndexType) {
		super(sTableName, sIndexName, column, Type.ONE_TO_ONE);
		deferred = false;
	}

	public UniqueIndexDef (String sTableName, String sIndexName, ColumnMetadata column, Using eUsingType) {
		super(sTableName, sIndexName, column, Type.ONE_TO_ONE, eUsingType);
		deferred = false;
	}

	public UniqueIndexDef (String sTableName, String sIndexName, ColumnMetadata[] aIndexColumns, Type eIndexType) {
		super(sTableName, sIndexName, aIndexColumns, Type.ONE_TO_ONE);
		deferred = false;
	}

	public UniqueIndexDef (String sTableName, String sIndexName, ColumnMetadata[] aIndexColumns, Type eIndexType, Using eIndexUsing) {
		super(sTableName, sIndexName, aIndexColumns, Type.ONE_TO_ONE, eIndexUsing);
		deferred = false;
	}

	@Override
	public UniqueIndexDef setUnique(boolean unique) {
		if (!unique)
			throw new IllegalArgumentException("Unique constraint cannot be set to non-unique");
		super.setUnique(true);
		return this;
	}

	@Override
	public UniqueIndexDef setName(String name) {
		super.setName(name);
		return this;
	}

	@Override
	public UniqueIndexDef setTable(String tableName) {
		super.setTable(tableName);
		return this;
	}
	
	@Override
	public Boolean getDeferred() {
		return deferred;
	}

	@Override
	public ColumnMetadata newColumnMetadata() {
		return super.newColumn();
	}

	@Override
	public UniqueIndexDef setDeferred(boolean deferred) {
		this.deferred = deferred;
		return this;
	}

	/**
	 * <p>Get JDO XML representation of this unique index like:</p>
	 * &lt;unique name="<i>indexName</i>" table="<i>tableName</i>"&gt;
	 * &lt;column name="<i>column1</i>"/&gt;
	 * &lt;column name="<i>column2</i>"/&gt;
	 * &lt;/unique&gt;
	 * @return String
	 */
	@Override
	public String toJdoXml() {
		StringBuilder builder = new StringBuilder();
		builder.append("      <unique");
		if (getName()!=null)
			builder.append(" name=\""+getName()+"\"");
		if (getTable()!=null)
			builder.append(" table=\""+getTable()+"\"");
		for (ColumnMetadata cdef : getColumns())
			builder.append("        <column name=\""+cdef.getName()+"\" />\n");
		builder.append("      </unique>");
		return builder.toString();		
	}
	
}
