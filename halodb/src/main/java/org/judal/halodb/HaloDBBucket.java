package org.judal.halodb;

/*
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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;

import java.util.Iterator;

import javax.jdo.FetchPlan;
import javax.jdo.JDOException;
import javax.jdo.PersistenceManager;

import com.oath.halodb.HaloDBException;

import org.judal.storage.keyvalue.Bucket;
import org.judal.storage.keyvalue.Stored;

import static org.judal.serialization.BytesConverter.toBytes;

/**
 * In-Memory implementation of Bucket interface
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class HaloDBBucket implements Bucket {

	private final String bucketName;
	private final HaloDBDataSource dts;
	private Class<? extends Stored> candidateClass;

	public HaloDBBucket(HaloDBDataSource dts, String bucketName) {
		this.bucketName = bucketName;
		this.dts = dts;
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
	 * @return boolean <b>true</b>if an object with the given key exists and is instance of Stored and its bucket name matches
	 * @throws NullPointerException if key is <b>null</b>
	 * @throws JDOException wrapper around ClassNotFoundException | IOException | HaloDBException
	 */
	@Override
	public boolean exists(Object key) throws NullPointerException, JDOException {
		boolean found = false;
		if (null==key) throw new NullPointerException("HaloDBBucket.exists() key value cannot be null");
		try {
			final byte[] value = dts.getDatabase().get(toBytes(key));
			if (null!=value) {
				try (ByteArrayInputStream bin = new ByteArrayInputStream(value)) {
					try (ObjectInputStream oin = new ObjectInputStream(bin)) {
						final Object obj = oin.readObject();
						found = obj instanceof Stored && bucketName.equalsIgnoreCase(((Stored) obj).getBucketName());
					}
				}
			}
		}  catch (ClassNotFoundException | IOException | HaloDBException e) {
			throw new JDOException(e.getMessage(), e);
		}
		return found;
	}

	/**
	 * <p>Load an object and its metadata (if present) from Bucket.</p>
	 * @param key String
	 * @param target Stored
	 * @return boolean <b>true</b>if an object with the given key was loaded <b>false</b> otherwise
	 * @throws NullPointerException if key is <b>null</b>
	 * @throws JDOException wrapper around ClassNotFoundException | IOException | HaloDBException
	 */
	@Override
	public boolean load(Object key, Stored target) throws NullPointerException, JDOException {
		boolean loaded = false;
		if (null==key) throw new NullPointerException("HaloDBBucket.load() key value cannot be null");
		try {
			final byte[] value = dts.getDatabase().get(toBytes(key));
			if (null!=value) {
				try (ByteArrayInputStream bin = new ByteArrayInputStream(value)) {
					try (ObjectInputStream oin = new ObjectInputStream(bin)) {
						final Object obj = oin.readObject();
						if (obj instanceof Stored) {
							final Stored strd = (Stored) obj;
							if (bucketName.equalsIgnoreCase(strd.getBucketName())) {
								target.setKey(strd.getKey());
								target.setValue((Serializable) strd.getValue());
								loaded = true;
							}
						}
					}
				}
			}
		}  catch (ClassNotFoundException | IOException | HaloDBException e) {
			throw new JDOException(e.getMessage(), e);
		}
		return loaded;
	}

	/**
	 * @param record Stored
	 * @throws JDOException wrapper around HaloDBException
	 */
	@Override
	public void store(Stored record) throws NullPointerException, JDOException {
		if (null==record) throw new NullPointerException("HaloDBBucket.store() record value cannot be null");
		try {
			dts.getDatabase().put(toBytes(record.getKey()), toBytes(record));
			assert (dts.getDatabase().get(toBytes(record.getKey()))!=null);
		} catch (HaloDBException e) {
			throw new JDOException(e.getMessage(), e);
		}
	}

	/**
	 * @param key Param or String
	 * @throws NullPointerException if key is <b>null</b>
	 * @throws JDOException wrapper around HaloDBException
	 */
	@Override
	public void delete(Object key) throws NullPointerException, JDOException {
		if (null==key) throw new NullPointerException("HaloDBBucket.delete() key value cannot be null");
		try {
			dts.getDatabase().delete(toBytes(key));
		} catch (HaloDBException e) {
			throw new JDOException(e.getMessage(), e);
		}
	}

	/**
	 * @param candidateClass Class&lt;Stored&gt;
	 */
	@Override
	public void setClass(Class<? extends Stored> candidateClass) {
		this.candidateClass = candidateClass;
	}

	/**
	 * This method does nothing
	 * @param notused
	 */
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
	 * @return HaloDBIterator Over all the values stored at this Bucket
	 */
	@Override
	public Iterator<Stored> iterator() {
		return new HaloDBIterator(this);
	}

	public HaloDBDataSource getDataSource() {
		return dts;
	}
}
