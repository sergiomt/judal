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

import java.sql.Types;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.jdo.JDOException;
import javax.jdo.metadata.ColumnMetadata;
import javax.transaction.TransactionManager;

import org.judal.metadata.ColumnDef;
import org.judal.metadata.IndexDef;
import org.judal.metadata.IndexDef.Type;
import org.judal.metadata.IndexDef.Using;
import org.judal.metadata.JoinType;
import org.judal.metadata.NameAlias;
import org.judal.metadata.SchemaMetaData;
import org.judal.metadata.TableDef;
import org.judal.metadata.ViewDef;

import org.judal.storage.FieldHelper;
import org.judal.storage.table.IndexableView;
import org.judal.storage.table.Record;
import org.judal.storage.table.TableDataSource;

import com.knowgate.tuples.Pair;
import com.mongodb.MongoException;

public class MongoTableDataSource extends MongoDataSource implements TableDataSource {

	private SchemaMetaData metadata;

	public MongoTableDataSource(Map<String, String> properties, TransactionManager transactManager, SchemaMetaData metaData) {
		super(properties, transactManager);
		this.metadata = metaData;
	}

	@Override
	public TableDef getTableDef(String tableName) throws JDOException {
		return metadata.getTable(tableName);
	}

	@Override
	public FieldHelper getFieldHelper() throws JDOException {
		return null;
	}

	@Override
	public ViewDef getViewDef(String viewName) throws JDOException {
		return metadata.getTable(viewName);
	}

	@Override
	public ViewDef getTableOrViewDef(String objectName) throws JDOException {
		return metadata.getTable(objectName);
	}

	@Override
	public SchemaMetaData getMetaData() throws JDOException {
		return metadata;
	}

	@Override
	public void setMetaData(SchemaMetaData smd) throws JDOException {
		metadata = smd;
	}

	@Override
	public ColumnDef createColumnDef(String columnName, int position, short colType, Map<String, Object> options) {
		return new ColumnDef(columnName, position, colType);
	}

	@Override
	public TableDef createTableDef(String tableName, Map<String, Object> options) throws JDOException {
		return new TableDef(tableName);
	}

	@Override
	public IndexDef createIndexDef(String indexName, String tableName, Iterable<String> columns, Type indexType, Using using) throws JDOException {
		List<ColumnMetadata> indexColumns = new LinkedList<ColumnMetadata>();
		int columnPosition = 0;
		if (metadata.containsTable(tableName)) {
			TableDef tdef = metadata.getTable(tableName);
			for (String column : columns)
				indexColumns.add(new ColumnDef(column, tdef.getColumnByName(column).getType(), ++columnPosition));			
		} else {
			for (String column : columns)
				indexColumns.add(new ColumnDef(column, Types.NULL, ++columnPosition));
		}
		return new MongoIndex(tableName, indexName, indexColumns, indexType);
	}

	@Override
	public void createTable(TableDef tableDef, Map<String, Object> options) throws JDOException {
		createBucket(tableDef.getName(), options);
	}

	@Override
	public void dropTable(String tableName, boolean cascade) throws JDOException {
		dropBucket(tableName);
	}

	@Override
	public void truncateTable(String tableName, boolean cascade) throws JDOException {
		truncateBucket(tableName);
	}

	@Override
	public MongoTable openTable(Record recordInstance) throws JDOException {
		try {
			final String collectionName = recordInstance.getTableName();
			return new MongoTable(getCluster(), getDatabaseName(), metadata.getTable(collectionName), getCollection(collectionName), recordInstance.getClass());
		} catch (MongoException e) {
			throw new JDOException(e.getMessage(), e);
		}
	}

	@Override
	public MongoTable openIndexedTable(Record recordInstance) throws JDOException {
		return openTable(recordInstance);
	}

	@Override
	public MongoView openView(Record recordInstance) throws JDOException {
		return openTable(recordInstance);
	}

	@Override
	public MongoView openIndexedView(Record recordInstance) throws JDOException {
		return openTable(recordInstance);
	}

	@SuppressWarnings("unchecked")
	@Override
	public IndexableView openJoinView(JoinType joinType, Record result, NameAlias baseTable, NameAlias joinedTable,
			Pair<String, String>... onColumns) throws JDOException {
		throw new UnsupportedOperationException("MongoDB does not support JOIN operations");
	}


}
