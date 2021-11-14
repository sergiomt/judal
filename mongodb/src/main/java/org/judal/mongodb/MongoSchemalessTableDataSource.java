package org.judal.mongodb;

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

import org.judal.storage.Pair;
import org.judal.storage.table.Record;
import org.judal.storage.table.SchemalessIndexableTable;
import org.judal.storage.table.SchemalessIndexableView;
import org.judal.storage.table.SchemalessTableDataSource;
import org.judal.storage.table.SchemalessView;

import com.mongodb.MongoException;

public class MongoSchemalessTableDataSource extends MongoDataSource implements SchemalessTableDataSource {

	public MongoSchemalessTableDataSource(Map<String, String> properties, TransactionManager transactManager) {
		super(properties, transactManager);
	}

	@Override
	public IndexDef createIndexDef(String indexName, String tableName, Iterable<String> columns, Type indexType, Using using) throws JDOException {
		List<ColumnMetadata> indexColumns = new LinkedList<ColumnMetadata>();
		int columnPosition = 0;
		for (String column : columns)
			indexColumns.add(new ColumnDef(column, Types.NULL, ++columnPosition));
		return new MongoIndex(tableName, indexName, indexColumns, indexType);
	}

	@Override
	public void createTable(String tableName, Map<String, Object> options) throws JDOException {
		createBucket(tableName, options);
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
			return new MongoTable(getCluster(), getDatabaseName(), collectionName, getCollection(collectionName), recordInstance.getClass());
		} catch (MongoException e) {
			throw new JDOException(e.getMessage(), e);
		}
	}

	@Override
	public SchemalessIndexableTable openIndexedTable(Record recordInstance) throws JDOException {
		return openTable(recordInstance);
	}

	@Override
	public SchemalessView openView(Record recordInstance) throws JDOException {
		return openTable(recordInstance);
	}

	@Override
	public SchemalessIndexableView openIndexedView(Record recordInstance) throws JDOException {
		return openTable(recordInstance);
	}

	@Override
	public SchemalessIndexableView openJoinView(JoinType joinType, Record result, NameAlias baseTable,
			NameAlias joinedTable, Pair<String, String>... onColumns) throws JDOException {
		throw new UnsupportedOperationException("MongoDB does not support JOIN operations");
	}

}
