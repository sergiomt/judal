package org.judal.inmemory;

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

import java.util.HashMap;
import java.util.Map;

import javax.transaction.TransactionManager;

import javax.jdo.JDOException;
import javax.jdo.JDOUnsupportedOptionException;
import javax.jdo.datastore.JDOConnection;

import org.judal.metadata.SchemaMetaData;
import org.judal.storage.Param;
import org.judal.storage.keyvalue.Bucket;
import org.judal.storage.keyvalue.BucketDataSource;

/**
 * In-memory implementation of BucketDataSource forAmazon S3
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class InMemoryDataSource implements BucketDataSource {

	private SchemaMetaData smd;
	private Map<String,String> props;
	private Map<String,InMemoryBucket> dataStore;
	private Map<String,InMemorySequence> sequences;

	/**
	 * 
	 * @param properties
	 */
	public InMemoryDataSource(Map<String,String> properties) {
		smd = new SchemaMetaData();
		props = new HashMap<String,String>(17);
		props.putAll(properties);
		dataStore = new HashMap<String,InMemoryBucket>();
		sequences  = new HashMap<String,InMemorySequence>();
	}

	public InMemoryDataSource(Map<String, String> properties, SchemaMetaData metaData) throws JDOException {
		smd = metaData;
		props = new HashMap<String,String>(17);
		props.putAll(properties);
		dataStore = new HashMap<String,InMemoryBucket>();
	}

	/**
	 * Check whether a Bucket with the given name exists
	 * @param objectName String Bucket Name
	 * @param objectType String Must have value "U"
	 * @return boolean
	 */
	@Override
	public boolean exists(String objectName, String objectType) throws JDOException {
		boolean retval;
		if (objectType.equals("U")) {
			retval = dataStore.containsKey(objectName);
		} else {
			retval = false;
		}
		return retval;
	}

	/**
	 * return Map&lt;String,String&gt;
	 */
	@Override
	public Map<String, String> getProperties() {
		return props;
	}

	/**
	 * S3 does not support transactions. This method will always raise JDOUnsupportedOptionException
	 * @throws JDOUnsupportedOptionException
	 */
	@Override
	public TransactionManager getTransactionManager() {
		throw new JDOUnsupportedOptionException("InMemoryDataSource does not support transactions");
	}

	/**
	 * S3 does not support transactions. This method will always raise JDOUnsupportedOptionException
	 * @throws JDOUnsupportedOptionException
	 */
	@Override
	public boolean inTransaction() throws JDOException {
		throw new JDOUnsupportedOptionException("InMemoryDataSource does not support transactions");
	}

	@Override
	public void close() throws JDOException {
	}

	public SchemaMetaData getMetaData() throws JDOException, UnsupportedOperationException {
		return smd;
	}

	public void setMetaData(SchemaMetaData oSmd) throws JDOException,UnsupportedOperationException {
		this.smd = oSmd;
	}

	/**
	 * Create a Bucket.
	 * If a Bucket with the given name already exists then nothing is done.
	 * @param bucketName String
	 * @param options Map&lt;String,Object&gt; Unused
	 */
	@Override
	public void createBucket(String bucketName, Map<String,Object> options) throws JDOException {
		if (!dataStore.containsKey(bucketName))
			dataStore.put(bucketName, new InMemoryBucket(this, bucketName));
	}

	/**
	 * @param bucketName String
	 * @return InMemoryBucket
	 */
	@Override
	public Bucket openBucket(String bucketName) throws JDOException {
		if (dataStore.containsKey(bucketName))
			return dataStore.get(bucketName);
		else
			throw new JDOException("Bucket " + bucketName + " does not exist");
	}

	/**
	 * @param bucketName String
	 */
	@Override
	public void dropBucket(String bucketName) throws JDOException {
		if (dataStore.containsKey(bucketName))
			dataStore.remove(bucketName);
	}

	/**
	 * Delete all objects at this Bucket including all their versions.
	 * @param bucketName String
	 * @throws JDOException
	 */
	@Override
	public void truncateBucket(String bucketName) throws JDOException {
		if (dataStore.containsKey(bucketName))
			dataStore.get(bucketName).truncate();
		else
			throw new JDOException("Bucket " + bucketName + " does not exist");
	}

	/**
	 * InMemoryDataSource does not use connections. This method will always raise JDOUnsupportedOptionException
	 * @throws JDOUnsupportedOptionException
	 */
	@Override
	public JDOConnection getJdoConnection() throws JDOException {
		throw new JDOUnsupportedOptionException("InMemoryDataSource does not use connections");
	}

	public InMemorySequence createSequence(String name, long initial) throws JDOException {
		if (sequences.containsKey(name.toLowerCase()))
			throw new JDOException("Sequence " + name + " already exists");
		InMemorySequence seq = new InMemorySequence(name, initial);
		sequences.put(name.toLowerCase(), seq);
		return seq;
	}

	public void dropSequence(String name, long initial) throws JDOException {
		if (!sequences.containsKey(name.toLowerCase()))
			throw new JDOException("Sequence " + name + "does not exist");
		sequences.remove(name.toLowerCase());
	}

	/**
	 * @return InMemorySequence
	 */
	@Override
	public InMemorySequence getSequence(String name) throws JDOException {
		return sequences.get(name.toLowerCase());
	}

	/**
	 * InMemoryDataSource does not provide callable statements. This method will always raise JDOUnsupportedOptionException
	 * @throws JDOUnsupportedOptionException
	 */
	@Override
	public Object call(String statement, Param... parameters) throws JDOException {
		throw new JDOUnsupportedOptionException("InMemoryDataSource does not support callable statements");
	}

}
