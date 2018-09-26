package org.judal.mongodb;

/**
 * © Copyright 2018 the original author.
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

import org.judal.metadata.IndexDef;

public class MongoIndex extends IndexDef {

	private static final long serialVersionUID = 1L;

	public MongoIndex(String collectionName, String indexName, ColumnMetadata indexColumn, Type indexType) {
		super(collectionName, indexName, indexColumn, indexType);
	}

	public MongoIndex(String collectionName, String indexName, List<ColumnMetadata> indexColumns, Type indexType) {
		super(collectionName, indexName, indexColumns, indexType);
	}

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
