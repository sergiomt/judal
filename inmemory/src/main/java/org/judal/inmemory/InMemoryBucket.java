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

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import java.util.concurrent.ConcurrentHashMap;

import org.judal.storage.Param;

import javax.jdo.FetchPlan;
import javax.jdo.JDOException;
import javax.jdo.PersistenceManager;

import org.judal.storage.keyvalue.Bucket;
import org.judal.storage.keyvalue.Stored;

/**
 * In-Memory implementation of Bucket interface
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class InMemoryBucket implements Bucket {

	private String bucketName;
	protected InMemoryDataSource dts;
	protected Map<Object,InMemoryRecord> bucketData;
	protected Class<? extends Stored> candidateClass;
	protected Collection<InMemoryIterator> iterators;

	public InMemoryBucket(InMemoryDataSource oDts, String sBucketName) {
		bucketName = sBucketName;
		dts = oDts;
		bucketData = new ConcurrentHashMap<Object,InMemoryRecord>();
	}

	/**
	 * @return String Bucket Name
	 */
	@Override
	public String name() {
		return bucketName;
	}

	@Override
	public void close() throws JDOException { }

	/**
	 * Check whether an object with the given key is at this Bucket
	 * @param key Param or String value
	 * @return boolean
	 * @throws NullPointerException if key is <b>null</b>
	 * @throws IllegalArgumentException
	 * @throws JDOException
	 */
	@Override
	public boolean exists(Object key) throws NullPointerException, IllegalArgumentException, JDOException {
		if (null==key) throw new NullPointerException("InMemoryBucket.exists() key value cannot be null");
		return bucketData.containsKey(key instanceof Param ? ((Param) key).getValue() : key);
	}

	/**
	 * <p>Load an object and its metadata (if present) from Bucket.</p>
	 * @param key String
	 * @param target InMemoryRecord
	 * @return boolean <b>true</b>if an object with the given key was loaded <b>false</b> otherwise
	 */
	@Override
	public boolean load(Object key, Stored target) throws NullPointerException, JDOException {
		if (null==key) throw new NullPointerException("InMemoryBucket.load() key value cannot be null");

		String value;
		if (key instanceof Param)
			value = ((Param) key).getValue().toString();
		else
			value = key.toString();

		if (bucketData.containsKey(key)) {
			((InMemoryRecord) target).reference(bucketData.get(value));
			return true;
		} else {
			return false;
		}
	}

	/**
	 * @param record Stored
	 */
	@Override
	public void store(Stored record) throws JDOException {
		bucketData.remove(record.getKey());
		bucketData.put(record.getKey(), (InMemoryRecord) record);
	}

	/**
	 * @param key Param or String
	 */
	@Override
	public void delete(Object key) throws NullPointerException, IllegalArgumentException, JDOException {
		String keyval;
		if (key instanceof Param)
			keyval = ((Param) key).getValue().toString();
		else if (key instanceof String)
			keyval = (String) key;
		else 
			keyval = key.toString();
		bucketData.remove(keyval);
	}

	/**
	 * @param candidateClass Class&lt;Stored&gt;
	 */
	@Override
	public void setClass(Class<? extends Stored> candidateClass) {
		this.candidateClass = candidateClass;
	}

	@Override
	public void close(Iterator<Stored> notused) { }

	@Override
	public void closeAll() { }

	@Override
	public Class<Stored> getCandidateClass() {
		return (Class<Stored>) candidateClass;
	}

	/**
	 * @return This method always returns <b>null</b>
	 */
	@Override
	public FetchPlan getFetchPlan() {
		return null;
	}

	/**
	 * @return This method always returns <b>null</b>
	 */
	@Override
	public PersistenceManager getPersistenceManager() {
		return null;
	}

	/**
	 * @return This method always returns <b>false</b>
	 */
	@Override
	public boolean hasSubclasses() {
		return false;
	}

	/**
	 * @return InMemoryIterator Over all the values stored at this Bucket
	 */
	@Override
	public Iterator<Stored> iterator() {
		Iterator<Stored> retval;
		if (null==iterators)
			iterators = new LinkedList<InMemoryIterator>();
		try {
			retval = new InMemoryIterator(InMemoryBucket.class, this, null);
		} catch (NoSuchMethodException | SecurityException xcpt) {
			throw new JDOException(xcpt.getMessage(), xcpt);
		}
		return retval;
	}

	public void truncate() {
		bucketData.clear();
	}

}
