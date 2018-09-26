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

import java.lang.reflect.Constructor;

import javax.jdo.FetchGroup;
import javax.jdo.JDOException;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.judal.metadata.ColumnDef;
import org.judal.metadata.TableDef;
import org.judal.storage.Param;
import org.judal.storage.StorageObjectFactory;
import org.judal.storage.keyvalue.Stored;
import org.judal.storage.table.IndexableView;
import org.judal.storage.table.Record;
import org.judal.storage.table.RecordSet;

import com.mongodb.Block;
import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.connection.Cluster;

public class MongoView extends MongoBucket implements IndexableView {

	protected TableDef tableDef;
	protected Class<? extends Stored> candidateClass;
	private Class<? extends Record> recordClass;
	private Constructor<? extends Record> recordConstructor;

	public MongoView(Cluster cluster, String databaseName, TableDef tableDef, MongoCollection<Document> collection, Class<? extends Record> recClass) throws JDOException {
		super(cluster, databaseName, tableDef.getName(), collection);
		this.tableDef = tableDef;
		this.candidateClass = this.recordClass = recClass;
		this.recordConstructor = null;
	}

	@Override
	public ColumnDef[] columns() {
		return tableDef.getColumns();
	}

	@Override
	public int columnsCount() {
		return tableDef.getNumberOfColumns();
	}

	@Override
	public ColumnDef getColumnByName(String columnName) {
		return tableDef.getColumnByName(columnName);
	}

	@Override
	public int getColumnIndex(String columnName) {
		int position = 1;
		for (ColumnDef c :columns())
			if (c.getName().equalsIgnoreCase(columnName))
				return position;
			else
				position++;
		return -1;
	}

	@Override
	public boolean exists(Param... keys) throws JDOException {
		Bson[] filters = new Bson[keys.length];
		int n = 0;
		for (Param p : keys)
			filters[n++] = Filters.eq(p.getName(), p.getValue());
		try {
			return getCollection().find(Filters.and(filters)).limit(1).first()!=null;
		} catch (MongoException e) {
			throw new JDOException(e.getClass().getName()+" " + e.getMessage(), e);
		}
	}

	@Override
	public long count(String indexColumnName, Object valueSearched) throws JDOException {
		Bson filter = Filters.eq(indexColumnName, valueSearched);
		try {
			return getCollection().countDocuments(filter);
		} catch (MongoException e) {
			throw new JDOException(e.getClass().getName()+" " + e.getMessage(), e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <R extends Record> RecordSet<R> fetch(FetchGroup fetchGroup, String indexColumnName, Object valueSearched) throws JDOException {
		Bson filter = Filters.eq(indexColumnName, valueSearched);
		Document fields = new Document();
		for (Object fieldName : fetchGroup.getMembers())
			fields.append((String) fieldName, true);
		try {
			final RecordSet<R> recordSet = (RecordSet<R>) StorageObjectFactory.newRecordSetOf(recordClass, (int) getCollection().countDocuments(filter));
			getCollection().find(filter).projection(fields).forEach((Block<Document>) doc -> recordSet.add((R) newRecord(doc)));
			return recordSet;
		} catch (MongoException e) {
			throw new JDOException(e.getClass().getName()+" " + e.getMessage(), e);
		} catch (NoSuchMethodException e) {
			throw new JDOException("NoSuchMethodException " + e.getMessage(), e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <R extends Record> RecordSet<R> fetch(FetchGroup fetchGroup, String indexColumnName, Object valueSearched, int maxrows, int offset) throws JDOException {
		Bson filter = Filters.eq(indexColumnName, valueSearched);
		Document fields = new Document();
		for (Object fieldName : fetchGroup.getMembers())
			fields.append((String) fieldName, true);
		try {
			int count = (int) getCollection().countDocuments(filter) - offset;
			if (count<0)
				count = 1;
			if (maxrows>0 && count>maxrows)
				count = maxrows;
			final RecordSet<R> recordSet = (RecordSet<R>) StorageObjectFactory.newRecordSetOf(recordClass, count);
			getCollection().find(filter).limit(maxrows).skip(offset).projection(fields).forEach((Block<Document>) doc -> recordSet.add((R) newRecord(doc)));
			return recordSet;
		} catch (MongoException e) {
			throw new JDOException(e.getClass().getName()+" " + e.getMessage(), e);
		} catch (NoSuchMethodException e) {
			throw new JDOException("NoSuchMethodException " + e.getMessage(), e);
		}	
	}

	@SuppressWarnings("unchecked")
	@Override
	public <R extends Record> RecordSet<R> fetch(FetchGroup fetchGroup, String indexColumnName, Object valueFrom, Object valueTo) throws JDOException, IllegalArgumentException {
		Bson from = Filters.gte(indexColumnName, valueFrom);
		Bson to = Filters.lte(indexColumnName, valueTo);
		Bson range = Filters.and(from,to);
		Document fields = new Document();
		for (Object fieldName : fetchGroup.getMembers())
			fields.append((String) fieldName, true);
		try {
			final RecordSet<R> recordSet = (RecordSet<R>) StorageObjectFactory.newRecordSetOf(recordClass, (int) getCollection().countDocuments(range));
			getCollection().find(range).projection(fields).forEach((Block<Document>) doc -> recordSet.add((R) newRecord(doc)));
			return recordSet;
		} catch (MongoException e) {
			throw new JDOException(e.getClass().getName()+" " + e.getMessage(), e);
		} catch (NoSuchMethodException e) {
			throw new JDOException("NoSuchMethodException " + e.getMessage(), e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <R extends Record> RecordSet<R> fetch(FetchGroup fetchGroup, String indexColumnName, Object valueFrom,
			Object valueTo, int maxrows, int offset) throws JDOException, IllegalArgumentException {
		Bson from = Filters.gte(indexColumnName, valueFrom);
		Bson to = Filters.lte(indexColumnName, valueTo);
		Bson range = Filters.and(from,to);
		Document fields = new Document();
		for (Object fieldName : fetchGroup.getMembers())
			fields.append((String) fieldName, true);
		int count = (int) getCollection().countDocuments(range) - offset;
		if (count<0)
			count = 1;
		if (maxrows>0 && count>maxrows)
			count = maxrows;
		try {
			final RecordSet<R> recordSet = (RecordSet<R>) StorageObjectFactory.newRecordSetOf(recordClass, count);
			getCollection().find(range).limit(maxrows).skip(offset).projection(fields).forEach((Block<Document>) doc -> recordSet.add((R) newRecord(doc)));
			return recordSet;
		} catch (MongoException e) {
			throw new JDOException(e.getClass().getName()+" " + e.getMessage(), e);
		} catch (NoSuchMethodException e) {
			throw new JDOException("NoSuchMethodException " + e.getMessage(), e);
		}
	}

	@Override
	public Class<? extends Record> getResultClass() {
		return recordClass;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <R extends Record> RecordSet<R> fetch(FetchGroup fetchGroup, int maxrows, int offset, Param... params) {
		Bson[] filters = new Bson[params.length];
		int f = 0;
		for (Param p : params)
			filters[f++] = Filters.eq(p.getName(), p.getValue());
		Bson filter = Filters.and(filters);
		Document fields = new Document();
		for (Object fieldName : fetchGroup.getMembers())
			fields.append((String) fieldName, true);
		int count = (int) getCollection().countDocuments(filter) - offset;
		if (count < 0)
			count = 1;
		if (maxrows > 0 && count > maxrows)
			count = maxrows;
		try {
			final RecordSet<R> recordSet = (RecordSet<R>) StorageObjectFactory.newRecordSetOf(recordClass, count);
			getCollection().find(filter).limit(maxrows).skip(offset).projection(fields).forEach((Block<Document>) doc -> recordSet.add((R) newRecord(doc)));
			return recordSet;
		} catch (MongoException e) {
			throw new JDOException(e.getClass().getName()+" " + e.getMessage(), e);
		} catch (NoSuchMethodException e) {
			throw new JDOException("NoSuchMethodException " + e.getMessage(), e);
		}
	}

	@SuppressWarnings("unchecked")
	public Record newRecord(Document bsonDoc) {
		Object[] constructorParameters;
		if (null==recordConstructor) {
			recordConstructor = (Constructor<? extends Record>) StorageObjectFactory.getConstructor(getResultClass(), new Class<?>[]{TableDef.class, Document.class});			
		}
		try {
			constructorParameters = StorageObjectFactory.filterParameters(recordConstructor.getParameters(), new Object[]{tableDef, bsonDoc});
		} catch (InstantiationException e) {
			throw new JDOException(e.getMessage() + " getting constructor for MongoDocument subclass");
		}
		return StorageObjectFactory.newRecord(recordConstructor, constructorParameters);
	}

}
