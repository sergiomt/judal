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

import javax.jdo.JDOException;
import javax.jdo.metadata.PrimaryKeyMetadata;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.judal.metadata.IndexDef.Using;
import org.judal.metadata.TableDef;
import org.judal.storage.Param;
import org.judal.storage.keyvalue.Stored;
import org.judal.storage.query.AbstractQuery;
import org.judal.storage.query.bson.BSONQuery;
import org.judal.storage.relational.RelationalTable;
import org.judal.storage.table.Record;
import org.judal.storage.table.SchemalessIndexableTable;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.connection.Cluster;

public class MongoTable extends MongoView implements RelationalTable, SchemalessIndexableTable  {

	private static final UpdateOptions upsert = new UpdateOptions().upsert(true);
	
	private String timestampColumnName;

	public MongoTable(Cluster cluster, String databaseName, String tableName, MongoCollection<Document> collection, Class<? extends Record> recClass) throws JDOException {
		super(cluster, databaseName, tableName, collection, recClass);
	}

	public MongoTable(Cluster cluster, String databaseName, TableDef tableDef, MongoCollection<Document> collection, Class<? extends Record> recClass) throws JDOException {
		super(cluster, databaseName, tableDef, collection, recClass);
	}

	@Override
	public String getTimestampColumnName() {
		return timestampColumnName;
	}

	@Override
	public void setTimestampColumnName(String columnName) throws IllegalArgumentException {
		throw new UnsupportedOperationException("MongoDB does not support timestamp non-updatable field");
	}

	@Override
	public PrimaryKeyMetadata getPrimaryKey() {
		return tableDef.getPrimaryKeyMetadata();
	}

	@Override
	public void store(Stored source) throws JDOException {
		Bson filter = Filters.eq("_id", source.getKey());
		Bson update =  new Document("$set", ((MongoDocument) source).getDocument());
		try {
			getCollection().updateOne(filter, update, upsert);
		} catch (MongoException e) {
			throw new JDOException(e.getMessage(), e);
		}
	}
	
	@Override
	public void insert(Param... params) throws JDOException {
		Document doc = new Document();
		for (Param p : params)
			doc.put(p.getName(), p.getValue());
		try {
			getCollection().insertOne(doc);
		} catch (MongoException e) {
			throw new JDOException(e.getMessage(), e);
		}
	}


	@Override
	public int update(Param[] values, Param[] where) throws JDOException {
		Document filter = new Document();
		for (Param w : where)
			filter.append(w.getName(), w.getValue());
		Document fields = new Document();
		for (Param v : values)
			fields.append(v.getName(), v.getValue());
		int updated = 0;
		try {
			UpdateResult r = getCollection().updateMany(filter, fields);
			updated = (int) r.getModifiedCount();
		} catch (MongoException e) {
			throw new JDOException(e.getClass().getName()+" " + e.getMessage(), e);
		}
		return updated;
	}

	@Override
	public int delete(Param[] where) throws JDOException {
		Document filter = new Document();
		for (Param w : where)
			filter.append(w.getName(), w.getValue());
		int deleted = 0;
		try {
			DeleteResult r = getCollection().deleteMany(filter);
			deleted = (int) r.getDeletedCount();
		} catch (MongoException e) {
			throw new JDOException(e.getClass().getName()+" " + e.getMessage(), e);
		}
		return deleted;
	}

	@Override
	public void createIndex(String indexName, boolean unique, Using indexUsing, String... columns) throws JDOException,IllegalArgumentException {
		IndexOptions opts = new IndexOptions().unique(unique).name(indexName);
		try {
			if (indexUsing==null || Using.ASCENDING.equals(indexUsing))
				getCollection().createIndex(Indexes.ascending(columns), opts);
			else if (Using.DESCENDING.equals(indexUsing))
				getCollection().createIndex(Indexes.descending(columns), opts);
			else if (Using.HASH.equals(indexUsing))
				if (columns.length==1)
					getCollection().createIndex(Indexes.hashed(columns[0]), opts);
				else
					throw new IllegalArgumentException("Mongo hash indexes can only be created over one field");
			else if (Using.TEXT.equals(indexUsing))
				if (columns.length==1)
					getCollection().createIndex(Indexes.text(columns[0]), opts);
				else
					throw new IllegalArgumentException("Mongo text indexes can only be created over one field");
			else if (Using.GEO2D.equals(indexUsing))
				if (columns.length==1)
					getCollection().createIndex(Indexes.geo2d(columns[0]), opts);
				else
					throw new IllegalArgumentException("Mongo geo2d indexes can only be created over one field");
			else if (Using.GEO2DSPHERE.equals(indexUsing))
				getCollection().createIndex(Indexes.geo2dsphere(columns), opts);
			else
				throw new IllegalArgumentException("Unsupported index type " + indexUsing);
		} catch (MongoException e) {
			throw new JDOException(e.getClass().getName()+" " + e.getMessage(), e);
		}
	}

	@Override
	public void dropIndex(String indexName) throws JDOException {
		getCollection().dropIndex(indexName);
	}

	@Override
	public int update(Param[] values, AbstractQuery filter) throws JDOException {
		Bson where = ((BSONQuery) filter).source();
		Document doc =new Document();
		for (Param p : values)
			doc.put(p.getName(), p.getValue());
		UpdateResult result = getCollection().updateMany(where, doc);
		return (int) result.getModifiedCount();
	}

	@Override
	public int delete(AbstractQuery filter) throws JDOException {
		Bson where = ((BSONQuery) filter).source();
		DeleteResult result = getCollection().deleteMany(where);
		return (int) result.getDeletedCount();
	}

}
