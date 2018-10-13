package org.judal.mongodb;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.jdo.JDOException;
import javax.jdo.metadata.ColumnMetadata;
import javax.transaction.TransactionManager;

import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.judal.metadata.ColumnDef;
import org.judal.metadata.IndexDef;
import org.judal.metadata.JoinType;
import org.judal.metadata.NameAlias;
import org.judal.metadata.SchemaMetaData;
import org.judal.metadata.TableDef;
import org.judal.metadata.IndexDef.Type;
import org.judal.metadata.IndexDef.Using;
import org.judal.storage.relational.RelationalDataSource;
import org.judal.storage.relational.RelationalView;
import org.judal.storage.table.Record;

import com.knowgate.tuples.Pair;
import com.mongodb.MongoException;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;

public class MongoRelationalDataSource extends MongoTableDataSource implements RelationalDataSource {

	public MongoRelationalDataSource(Map<String, String> properties, TransactionManager transactManager, SchemaMetaData metaData) {
		super(properties, transactManager, metaData);
	}

	
	@Override
	public MongoIndex createIndexDef(String indexName, String tableName, String[] columns, boolean unique) {
		List<ColumnMetadata> indexColumns = new ArrayList<>(columns.length);
		int pos = 1;
		for (String columnName : columns)
			indexColumns.add(new ColumnDef(columnName, Types.NULL, pos++));
		return new MongoIndex(tableName, indexName, indexColumns, unique ? Type.ONE_TO_ONE : Type.ONE_TO_MANY);
	}

	@Override
	public void createIndex(IndexDef indexDef) throws JDOException {
		IndexOptions opts = new IndexOptions().unique(indexDef.getUnique()).name(indexDef.getName());
		ColumnMetadata[] columnsMeta = indexDef.getColumns();
		String columns[] = new String[columnsMeta.length];
		for (int c=0; c<columnsMeta.length; c++)
			columns[c] = columnsMeta[c].getName();
		try {
			final Using indexUsing = indexDef.getUsing();
			if (indexUsing==null || Using.ASCENDING.equals(indexUsing))
				getCollection(indexDef.getTable()).createIndex(Indexes.ascending(columns), opts);
			else if (Using.DESCENDING.equals(indexUsing))
				getCollection(indexDef.getTable()).createIndex(Indexes.descending(columns), opts);
			else if (Using.HASH.equals(indexUsing))
				if (columns.length==1)
					getCollection(indexDef.getTable()).createIndex(Indexes.hashed(columns[0]), opts);
				else
					throw new IllegalArgumentException("Mongo hash indexes can only be created over one field");
			else if (Using.TEXT.equals(indexUsing))
				if (columns.length==1)
					getCollection(indexDef.getTable()).createIndex(Indexes.text(columns[0]), opts);
				else
					throw new IllegalArgumentException("Mongo text indexes can only be created over one field");
			else if (Using.GEO2D.equals(indexUsing))
				if (columns.length==1)
					getCollection(indexDef.getTable()).createIndex(Indexes.geo2d(columns[0]), opts);
				else
					throw new IllegalArgumentException("Mongo geo2d indexes can only be created over one field");
			else if (Using.GEO2DSPHERE.equals(indexUsing))
				getCollection(indexDef.getTable()).createIndex(Indexes.geo2dsphere(columns), opts);
			else
				throw new IllegalArgumentException("Unsupported index type " + indexUsing);
		} catch (MongoException e) {
			throw new JDOException(e.getClass().getName()+" " + e.getMessage(), e);
		}

	}

	@Override
	public void dropIndex(String indexName, String tableName) throws JDOException {
		getCollection(tableName).dropIndex(indexName);
	}

	@Override
	public MongoTable openRelationalTable(Record recordInstance) throws JDOException {
		return super.openTable(recordInstance);
	}

	@Override
	public MongoView openRelationalView(Record recordInstance) throws JDOException {
		return super.openView(recordInstance);
	}

	@Override
	public MongoView openRelationalView(Record recordInstance, String alias) throws JDOException {
		return super.openView(recordInstance);
	}

	@Override
	public MongoView openJoinView(JoinType joinType, Record result, NameAlias baseTable, NameAlias joinedTable, Pair<String, String>... onColumns) throws JDOException {
		if (onColumns.length!=1)
			throw new JDOException("Mongo DB only supports joins by one column");
		if (joinType!=null && !joinType.equals(JoinType.OUTER))
			throw new JDOException("Mongo DB only supports left outer joins");

		BsonDocument unwind = new BsonDocument();
		BsonDocument unpath = new BsonDocument();
		// unwind.append("$unwind", new BsonString("$"+joinedTable.getName()));
		unpath.append("path", new BsonString("$"+joinedTable.getName()));
		unpath.append("preserveNullAndEmptyArrays", new BsonBoolean(true));
		unwind.append("$unwind", unpath);

		BsonDocument from = new BsonDocument();
		from.append("from", new BsonString(joinedTable.getName()));
		from.append("localField", new BsonString(onColumns[0].$1()));
		from.append("foreignField", new BsonString(onColumns[0].$2()));
		from.append("as", new BsonString(joinedTable.getName()));
		BsonDocument lookup = new BsonDocument();
		lookup.put("$lookup", from);
		TableDef tdef = getTableDef(baseTable.getName()).clone();
		for (ColumnDef clonedCol : tdef.getColumns()) {
			clonedCol.setFamily(tdef.getTable());
		}
		for (ColumnDef col : getTableDef(joinedTable.getName()).getColumns()) {
			try {
				tdef.getColumnByName(col.getName());
			} catch (ArrayIndexOutOfBoundsException nocolumnfound) {
				tdef.addColumnMetadata(joinedTable.getName(), col.getName(), col.getType(), false);
			}
		}
		try {
			return new MongoView(getCluster(), getDatabaseName(), tdef, getCollection(baseTable.getName()), result.getClass(), Arrays.asList(lookup,unwind));
		} catch (MongoException e) {
			throw new JDOException(e.getMessage(), e);
		}
	}

	@Override
	public int getRdbmsId() {
		return 0;
	}

}
