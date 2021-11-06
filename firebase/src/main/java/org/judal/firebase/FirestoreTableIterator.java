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

import java.lang.reflect.Constructor;
import java.util.Iterator;

import javax.jdo.JDOException;

import org.judal.storage.StorageObjectFactory;
import org.judal.storage.table.Record;

import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.DocumentReference;

public class FirestoreTableIterator implements Iterator<Record> {

	private final Iterator<DocumentReference> iterable;

	private Class<? extends Record> recClass;

	private Constructor<? extends Record> recordConstructor;

	public FirestoreTableIterator(CollectionReference collection, Class<? extends Record> recordClass) {
		iterable = collection.listDocuments().iterator();
		recClass = recordClass;
	}

	@Override
	public boolean hasNext() {
		return iterable.hasNext();
	}

	@Override
	public Record next() {
		return newRecord(iterable.next());
	}

	@SuppressWarnings("unchecked")
	private Record newRecord(DocumentReference doc) {
		Object[] constructorParameters;
		if (null==recordConstructor) {
			recordConstructor = (Constructor<? extends Record>) StorageObjectFactory.getConstructor(recClass, new Class<?>[]{DocumentReference.class});
		}
		try {
			constructorParameters = StorageObjectFactory.filterParameters(recordConstructor.getParameters(), new Object[]{doc});
		} catch (InstantiationException e) {
			throw new JDOException(e.getMessage() + " getting constructor for FirestoreDocument subclass");
		}
		return StorageObjectFactory.newRecord(recordConstructor, constructorParameters);
	}	
}
