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

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import javax.jdo.FetchGroup;
import javax.jdo.JDOException;

import org.judal.storage.Param;
import org.judal.storage.StorageObjectFactory;
import org.judal.storage.keyvalue.Stored;
import org.judal.storage.table.Record;
import org.judal.storage.table.RecordSet;
import org.judal.storage.table.SchemalessIndexableView;

import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.Query;
import com.google.cloud.firestore.QueryDocumentSnapshot;

public class FirestoreSchemalessView extends FirestoreBucket implements SchemalessIndexableView {

	protected static final int DEFAULT_INITIAL_RECORDSET_SIZE = 100;

	private final Class<? extends Record> recClass;

	private Constructor<? extends Record> recordConstructor;

	public FirestoreSchemalessView(FirestoreSchemalessDataSource dataSource, String collectionName, Class<? extends Record> recordClass) {
		super(dataSource, collectionName, recordClass);
		this.recClass =  recordClass;
	}

	@Override
	public boolean load(Object key, Stored target) throws JDOException {
		if (target instanceof Record)
			return super.load(key, target);
		else
			throw new IllegalArgumentException("Expected target implementing org.judal.storage.table.Record but got " + (null==target ? "null" : target.getClass().getName()));
	}

	public boolean load(Object key, Record target) throws JDOException {
		return super.load(key, target);
	}

	@Override
	public void store(Stored target) throws JDOException {
		if (target instanceof Record)
			super.store(target);
		else
			throw new IllegalArgumentException("Expected target implementing org.judal.storage.table.Record but got " + (null==target ? "null" : target.getClass().getName()));
	}

	public void store(Record target) throws JDOException {
		super.store(target);
	}

	@Override
	public boolean exists(Param... keys) throws JDOException {
		boolean retval;
		if (keys==null)
			throw new JDOException("FirestoreCollection.exists keys cannot be null");
		if (keys.length==0)
			throw new JDOException("FirestoreCollection.exists keys cannot be empty");
		Query qry = collection.select(keys[0].getName());
		for (Param p : keys) {
			qry.whereEqualTo(p.getName(), p.getValue());
		}
		try {
			retval = !qry.get().get().isEmpty();
		} catch (InterruptedException | ExecutionException e) {
			throw new JDOException(e.getMessage(), e);
		}
		return retval;
	}

	@Override
	public long count(String indexColumnName, Object valueSearched) throws JDOException {
		long retval;
		Query qry = collection.select(indexColumnName).whereEqualTo(indexColumnName, valueSearched);
		try {
			retval = qry.get().get().size();
		} catch (InterruptedException | ExecutionException e) {
			throw new JDOException(e.getMessage(), e);
		}
		return retval;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <R extends Record> RecordSet<R> fetch(FetchGroup fetchGroup, String indexColumnName, Object valueSearched)
			throws JDOException {
		RecordSet<R> recordSet;
		try {
			recordSet = (RecordSet<R>) StorageObjectFactory.newRecordSetOf(recClass, DEFAULT_INITIAL_RECORDSET_SIZE);
		} catch (NoSuchMethodException nsme) {
			throw new JDOException(nsme.getMessage(), nsme);
		}
		final String[] fieldPaths = new String[fetchGroup.getMembers().size()];
		int f = 0;
		for (Object m : fetchGroup.getMembers())
			fieldPaths[f++] = (String) m;
		Query qry = collection.select(fieldPaths).whereEqualTo(indexColumnName, valueSearched);
		try {
			qry.get().get().forEach(d -> recordSet.add((R) newRecord(d)));
		} catch (InterruptedException | ExecutionException iee) {
			throw new JDOException(iee.getMessage(), iee);
		}
		return recordSet;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <R extends Record> RecordSet<R> fetch(FetchGroup fetchGroup, String indexColumnName, Object valueSearched,
			int maxrows, int offset) throws JDOException {
		RecordSet<R> recordSet;
		try {
			recordSet = (RecordSet<R>) StorageObjectFactory.newRecordSetOf(recClass, DEFAULT_INITIAL_RECORDSET_SIZE);
		} catch (NoSuchMethodException nsme) {
			throw new JDOException(nsme.getMessage(), nsme);
		}
		final String[] fieldPaths = new String[fetchGroup.getMembers().size()];
		int f = 0;
		for (Object m : fetchGroup.getMembers())
			fieldPaths[f++] = (String) m;
		Query qry = collection.select(fieldPaths)
				.whereEqualTo(indexColumnName, valueSearched)
				.limit(maxrows)
				.offset(offset);
		try {
			qry.get().get().forEach(d -> recordSet.add((R) newRecord(d)));
		} catch (InterruptedException | ExecutionException iee) {
			throw new JDOException(iee.getMessage(), iee);
		}
		return recordSet;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <R extends Record> RecordSet<R> fetch(FetchGroup fetchGroup, String indexColumnName, Comparable<?> valueFrom,
			Comparable<?> valueTo) throws JDOException, IllegalArgumentException {
		RecordSet<R> recordSet;
		try {
			recordSet = (RecordSet<R>) StorageObjectFactory.newRecordSetOf(recClass, DEFAULT_INITIAL_RECORDSET_SIZE);
		} catch (NoSuchMethodException nsme) {
			throw new JDOException(nsme.getMessage(), nsme);
		}
		final String[] fieldPaths = new String[fetchGroup.getMembers().size()];
		int f = 0;
		for (Object m : fetchGroup.getMembers())
			fieldPaths[f++] = (String) m;
		Query qry = collection.select(fieldPaths)
				.whereGreaterThanOrEqualTo(indexColumnName, valueFrom)
				.whereLessThanOrEqualTo(indexColumnName, valueTo);
		try {
			qry.get().get().forEach(d -> recordSet.add((R) newRecord(d)));
		} catch (InterruptedException | ExecutionException iee) {
			throw new JDOException(iee.getMessage(), iee);
		}
		return recordSet;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <R extends Record> RecordSet<R> fetch(FetchGroup fetchGroup, String indexColumnName, Comparable<?> valueFrom,
			Comparable<?> valueTo, int maxrows, int offset) throws JDOException, IllegalArgumentException {
		RecordSet<R> recordSet;
		try {
			recordSet = (RecordSet<R>) StorageObjectFactory.newRecordSetOf(recClass, DEFAULT_INITIAL_RECORDSET_SIZE);
		} catch (NoSuchMethodException nsme) {
			throw new JDOException(nsme.getMessage(), nsme);
		}
		final String[] fieldPaths = new String[fetchGroup.getMembers().size()];
		int f = 0;
		for (Object m : fetchGroup.getMembers())
			fieldPaths[f++] = (String) m;
		Query qry = collection.select(fieldPaths)
				.whereGreaterThanOrEqualTo(indexColumnName, valueFrom)
				.whereLessThanOrEqualTo(indexColumnName, valueTo)
				.limit(maxrows)
				.offset(offset);
		try {
			qry.get().get().forEach(d -> recordSet.add((R) newRecord(d)));
		} catch (InterruptedException | ExecutionException iee) {
			throw new JDOException(iee.getMessage(), iee);
		}
		return recordSet;
	}

	@Override
	public Class<? extends Record> getResultClass() {
		return recClass;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Iterator iterator() {
		return new FirestoreTableIterator(getDb().collection(name()), getResultClass());
	}

	@SuppressWarnings("unchecked")
	@Override
	public <R extends Record> RecordSet<R> fetch(FetchGroup fetchGroup, int maxrows, int offset, Param... params)
			throws JDOException {
		RecordSet<R> recordSet;
		try {
			recordSet = (RecordSet<R>) StorageObjectFactory.newRecordSetOf(recClass, DEFAULT_INITIAL_RECORDSET_SIZE);
		} catch (NoSuchMethodException nsme) {
			throw new JDOException(nsme.getMessage(), nsme);
		}
		final String[] fieldPaths = new String[fetchGroup.getMembers().size()];
		int f = 0;
		for (Object m : fetchGroup.getMembers())
			fieldPaths[f++] = (String) m;
		Query qry = collection.select(fieldPaths)
				.limit(maxrows)
				.offset(offset);
		for (Param p : params) {
			qry.whereEqualTo(p.getName(), p.getValue());
		}
		try {
			qry.get().get().forEach(d -> recordSet.add((R) newRecord(d)));
		} catch (InterruptedException | ExecutionException iee) {
			throw new JDOException(iee.getMessage(), iee);
		}
		return recordSet;
	}

	@SuppressWarnings("unchecked")
	protected Record newRecord(QueryDocumentSnapshot doc) {
		Object[] constructorParameters;
		if (null==recordConstructor) {
			recordConstructor = (Constructor<? extends Record>) StorageObjectFactory.getConstructor(getResultClass(), new Class<?>[]{QueryDocumentSnapshot.class});
		}
		try {
			constructorParameters = StorageObjectFactory.filterParameters(recordConstructor.getParameters(), new Object[]{doc});
		} catch (InstantiationException e) {
			throw new JDOException(e.getMessage() + " getting constructor for MongoDocument subclass");
		}
		return StorageObjectFactory.newRecord(recordConstructor, constructorParameters);
	}
}
