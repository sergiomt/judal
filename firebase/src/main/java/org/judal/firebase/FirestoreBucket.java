package org.judal.firebase;

/**
 * © Copyright 2019 the original author.
 * This file is licensed under the Apache License version 2.0.
 * You may not use this file except in compliance with the license.
 * You may obtain a copy of the License at:
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.
 */

import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

import java.io.Serializable;

import javax.jdo.FetchPlan;
import javax.jdo.JDOException;
import javax.jdo.PersistenceManager;

import org.judal.storage.keyvalue.Bucket;
import org.judal.storage.keyvalue.Stored;
import org.judal.storage.table.Record;

import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;

public class FirestoreBucket implements Bucket {

	private final String collectionName;
	private final FirestoreDataSource dts;
	private Class<? extends Stored> candidateClass;
	protected final CollectionReference collection;
	
	public FirestoreBucket(FirestoreDataSource dataSource, String collectionName, Class<? extends Stored> candidateClass) {
		this.dts = dataSource;
		this.collectionName = collectionName;
		this.candidateClass = candidateClass;
		this.collection = getDb().collection(collectionName);
	}

	@Override
	public String name() {
		return collectionName;
	}

	@Override
	public boolean exists(Object key) throws JDOException {
		try {
			return collection.document((String) key).get().get().exists();
		} catch (InterruptedException | ExecutionException e) {
			throw new JDOException (e.getMessage(), e);
		}
	}

	@Override
	public boolean load(Object key, Stored target) throws JDOException {
		boolean retval;
		try {
			final DocumentSnapshot snapshot = collection.document(key.toString()).get().get();
			retval = snapshot.exists();
			if (retval) {
				if (target instanceof Record) {
					Record rec = (Record) target;
					for (Map.Entry<String,Object> e : snapshot.getData().entrySet()) {
						rec.put(e.getKey(), e.getValue());
					}
				} else {
					target.setKey(key);
					target.setValue((Serializable) snapshot.get("value"));
				}
			}
		} catch (InterruptedException | ExecutionException e) {
			throw new JDOException (e.getMessage(), e);
		}
		return retval;
	}

	@Override
	public void close() throws JDOException {
	}

	@Override
	public void setClass(Class<? extends Stored> candidateClass) {
		this.candidateClass =  candidateClass;
	}

	@Override
	public Iterator<Stored> iterator() {
		return new FirestoreBucketIterator(getDb().collection(name()), getCandidateClass());
	}

	@Override
	public boolean hasSubclasses() {
		return false;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Class<Stored> getCandidateClass() {
		return (Class<Stored>) candidateClass;
	}

	@Override
	public PersistenceManager getPersistenceManager() {
		return null;
	}

	@Override
	public void closeAll() {
	}

	@Override
	public void close(Iterator<Stored> it) {
	}

	@Override
	public FetchPlan getFetchPlan() {
		return null;
	}

	@Override
	public void store(Stored target) throws JDOException {
		DocumentReference docRef = getDb().collection(name()).document(target.getKey().toString());
		Map<String,Object> data;
		if (target instanceof Record) {
			data = ((Record) target).asMap();
		} else {
			data = new HashMap<>();
			data.put("value", target.getValue());
		}
		docRef.set(data);
	}

	@Override
	public void delete(Object key) throws JDOException {
		DocumentReference docRef = getDb().collection(name()).document(key.toString());
		docRef.delete();
	}

	public FirestoreDataSource getDataSource() {
		return dts;
	}

	protected Firestore getDb() {
		return dts.getDb();
	}
}
