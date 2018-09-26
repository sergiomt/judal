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

import java.lang.reflect.Field;

import java.util.Map;

import javax.jdo.JDOException;
import javax.jdo.JDOUnsupportedOptionException;

import javax.transaction.TransactionManager;

import com.mongodb.MongoException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.binding.ClusterBinding;
import com.mongodb.binding.ReadWriteBinding;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.connection.Cluster;

import org.bson.Document;

import org.judal.mongodb.MongoSequence;

import org.judal.storage.Param;
import org.judal.storage.keyvalue.BucketDataSource;

public class MongoDataSource implements BucketDataSource {

	private MongoClient mongoClient;
	private MongoDatabase database;
	private String databaseName;
	private TransactionManager trnMngr;

	private Map<String, String> properties;

	public MongoDataSource(Map<String, String> properties) {
		this.properties = properties;
		this.mongoClient = MongoClients.create();
		this.databaseName = properties.getOrDefault(URI, "localhost");
		this.database = mongoClient.getDatabase(databaseName);
		this.trnMngr = null;
	}

	public MongoDataSource(Map<String, String> properties, TransactionManager transactManager) {
		this.properties = properties;
		this.mongoClient = MongoClients.create();
		this.databaseName = properties.getOrDefault(URI, "localhost");
		this.database = mongoClient.getDatabase(databaseName);
		this.trnMngr = transactManager;
	}

	@Override
	public boolean exists(String objectName, String objectType) throws JDOException {
		if ("U".equals(objectType)) {
			for (String collectionName : database.listCollectionNames())
				if (collectionName.equals(objectName))
					return true;
			return false;
		} else {
			return false;
		}
	}

	protected MongoCollection<Document> getCollection(final String collectionName) {
		return database.getCollection(collectionName);
	}

	public String getDatabaseName() {
		return databaseName;
	}

	@Override
	public Map<String, String> getProperties() {
		return properties;
	}

	@Override
	public TransactionManager getTransactionManager() {
		return trnMngr;
	}

	@Override
	public MongoConnection getJdoConnection() throws JDOException {
    	ReadPreference readPref = ReadPreference.primary();
    	ReadConcern concern = ReadConcern.DEFAULT;
		ReadWriteBinding readBinding = new ClusterBinding(getCluster(), readPref, concern);
		return new MongoConnection(readBinding.getReadConnectionSource().getConnection());
	}

	public MongoSequence createSequence(String name, long initial) throws JDOException {
		MongoSequence seq;
		try {
			database.createCollection(name);
			MongoCollection<Document> cll = database.getCollection(name);
			Document doc = new Document();
			doc.put("_id",name);
			doc.put("seq",initial);
			cll.insertOne(doc);
			seq = new MongoSequence(name, cll);
		} catch (MongoException e) {
			throw new JDOException(e.getMessage(), e);
		}
		return seq;
	}

	public void dropSequence(String name) throws JDOException {
		dropBucket(name);
	}

	@Override
	public MongoSequence getSequence(String name) throws JDOException {
		MongoSequence seq;
		try {
			MongoCollection<Document> cll = database.getCollection(name);
			seq = new MongoSequence(name, cll);
		} catch (MongoException e) {
			throw new JDOException(e.getMessage(), e);
		}
		return seq;
	}

	@Override
	public Object call(String statement, Param... parameters) throws JDOException {
		throw new JDOUnsupportedOptionException("MongoDataSource does not support callable statements");
	}

	@Override
	public boolean inTransaction() throws JDOException {
		return false;
	}

	@Override
	public void close() throws JDOException {
		try {
			mongoClient.close();
		} catch (MongoException e) {
			throw new JDOException(e.getMessage(), e);
		}
	}

	@Override
	public void createBucket(String bucketName, Map<String, Object> options) throws JDOException {
		try {
			database.createCollection(bucketName);
		} catch (MongoException e) {
			throw new JDOException(e.getMessage(), e);
		}
	}

	@Override
	public MongoBucket openBucket(String bucketName) throws JDOException {
		try {
			return new MongoBucket(getCluster(), databaseName, bucketName, getCollection(bucketName));
		} catch (MongoException e) {
			throw new JDOException(e.getMessage(), e);
		}
	}

	@Override
	public void dropBucket(String bucketName) throws JDOException {
		try {
			getCollection(bucketName).drop();
		} catch (MongoException e) {
			throw new JDOException(e.getMessage(), e);
		}
	}

	@Override
	public void truncateBucket(String bucketName) throws JDOException {
		try {
			getCollection(bucketName).deleteMany(new Document());
		} catch (MongoException e) {
			throw new JDOException(e.getMessage(), e);
		}
	}

	public Cluster getCluster() throws JDOException {
		Field cluster, delegate;
		Cluster mongoCluster = null;
		try {
			delegate = mongoClient.getClass().getDeclaredField("delegate");
			delegate.setAccessible(true);
			Object clientDelegate = delegate.get(mongoClient);
			cluster = clientDelegate.getClass().getDeclaredField("cluster");
			cluster.setAccessible(true);
			mongoCluster = (Cluster) cluster.get(clientDelegate);
		} catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
			throw new JDOException(e.getMessage(), e);
		}
		return mongoCluster;
	}

}
