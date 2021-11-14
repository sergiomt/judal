package org.judal.mongodb;

import java.io.Serializable;

/*
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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.jdo.FetchPlan;
import javax.jdo.JDOException;
import javax.jdo.PersistenceManager;

import org.bson.Document;
import org.bson.codecs.Decoder;
import org.bson.codecs.DocumentCodec;
import org.bson.conversions.Bson;

import org.judal.metadata.TableDef;
import org.judal.storage.keyvalue.Bucket;
import org.judal.storage.keyvalue.Stored;
import org.judal.storage.table.Record;

import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.binding.ClusterBinding;
import com.mongodb.binding.ReadWriteBinding;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.connection.Cluster;
import com.mongodb.operation.BatchCursor;
import com.mongodb.operation.FindOperation;

public class MongoBucket implements Bucket {

	private static final UpdateOptions upsert = new UpdateOptions().upsert(true);

	private final Set<MongoIterator> iterators;
	private final Cluster cluster;
	private final TableDef tdef;
	private final String database;
	private final MongoCollection<Document> collection;

	public MongoBucket(Cluster cluster, String databaseName, String collectionName, final MongoCollection<Document> collection) throws JDOException {
		this.cluster = cluster;
		this.database = databaseName;
		this.collection = collection;
		this.tdef = new TableDef(collectionName);
		this.iterators = new HashSet<MongoIterator>();
	}

	@Override
	public String name() {
		return tdef.getName();
	}

	@Override
	public boolean exists(Object key) throws JDOException {
		Bson filter;
		if (key instanceof String)
			filter = Filters.eq("_id", (String) key);
		else
			filter = Filters.eq("_id", key.toString());
		return collection.find(filter).limit(1).first()!=null;
	}

	@Override
	public boolean load(Object key, Stored target) throws JDOException {
		Bson filter;
		if (key instanceof String)
			filter = Filters.eq("_id", (String) key);
		else
			filter = Filters.eq("_id", key.toString());
		Document doc = collection.find(filter).limit(1).first();
		if (target instanceof MongoDocument) {
			((MongoDocument) target).setDocument(doc);
		} else if (target instanceof Record) {
			Record rec = (Record) target;
			rec.clear();
			if (doc!=null) {
				for (Map.Entry<String, Object> e : doc.entrySet()) {
					rec.put(e.getKey(), e.getValue());
				}
			}
		} else {
			target.setKey(key);
			target.setValue(doc!=null ? (Serializable) doc.get("value") : null);
		}

		return doc!=null;
	}

	@Override
	public void close() throws JDOException {
	}

	@Override
	public void setClass(Class<? extends Stored> candidateClass) {
	}

	@Override
	public void close(@SuppressWarnings("rawtypes") Iterator iter) {
		((MongoIterator) iter).close();
		if (iterators.contains(iter))
			iterators.remove(iter);
	}

	@Override
	public void closeAll() {
		for (MongoIterator iter : iterators)
			iter.close();
		iterators.clear();
	}

	@Override
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Class getCandidateClass() {
		return MongoDocument.class;
	}

	@Override
	public FetchPlan getFetchPlan() {
		return null;
	}

	@Override
	public PersistenceManager getPersistenceManager() {
		return null;
	}

	@Override
	public boolean hasSubclasses() {
		return false;
	}

	public BatchCursor<Document> getCursor() {
		ReadPreference readPref = ReadPreference.primary();
		ReadConcern concern = ReadConcern.DEFAULT;
		MongoNamespace ns = new MongoNamespace(database,tdef.getName());
		Decoder<Document> codec = new DocumentCodec();
		FindOperation<Document> fop = new FindOperation<Document>(ns,codec);
		ReadWriteBinding readBinding = new ClusterBinding(cluster, readPref, concern);
		return fop.execute(readBinding);
	}

	@Override
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Iterator iterator() {
		return new MongoIterator(tdef, getCursor());
	}

	@Override
	public void store(Stored source) throws JDOException {
		Bson filter = Filters.eq("_id", source.getKey());
		Document update = null;
		if (source instanceof MongoDocument) {
			update = new Document("$set", ((MongoDocument) source).getDocument());
		} else {
			update = new Document();
			if (source instanceof Record)  {
				for (Map.Entry<String,Object> e : ((Record) source).asEntries()) {
					update.put(e.getKey(), e.getValue());
				}
			} else {
			update.put("value", source.getValue());
			}
		}
		try {
			getCollection().updateOne(filter, update, upsert);
		} catch (MongoException e) {
			throw new JDOException(e.getMessage(), e);
		}
	}

	@Override
	public void delete(Object key) throws JDOException {
		Bson filter = Filters.eq("_id", key);
		try {
			collection.deleteOne(filter);
		} catch (MongoException e) {
			throw new JDOException(e.getMessage(), e);
		}
	}

	public Cluster getCluster() {
		return cluster;
	}

	public String getDatabase() {
		return database;
	}

	public MongoCollection<Document> getCollection() {
		return collection;
	}

}
