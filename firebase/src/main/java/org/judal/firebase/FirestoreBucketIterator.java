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
import org.judal.storage.keyvalue.Stored;

import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.DocumentReference;

public class FirestoreBucketIterator implements Iterator<Stored> {

	private final Iterator<DocumentReference> iterable;

	private Class<? extends Stored> storedClass;

	private Constructor<? extends Stored> storedConstructor;

	public FirestoreBucketIterator(CollectionReference collection, Class<? extends Stored> storedClass) {
		this.iterable = collection.listDocuments().iterator();
		this.storedClass = storedClass;
	}

	@Override
	public boolean hasNext() {
		return iterable.hasNext();
	}

	@Override
	public Stored next() {
		return newStored(iterable.next());
	}

	@SuppressWarnings("unchecked")
	private Stored newStored(DocumentReference doc) {
		Object[] constructorParameters;
		if (null==storedConstructor) {
			storedConstructor = (Constructor<? extends Stored>) StorageObjectFactory.getConstructor(storedClass, new Class<?>[]{DocumentReference.class});
		}
		try {
			constructorParameters = StorageObjectFactory.filterParameters(storedConstructor.getParameters(), new Object[]{doc});
		} catch (InstantiationException e) {
			throw new JDOException(e.getMessage() + " getting constructor for FirestoreDocument subclass");
		}
		return StorageObjectFactory.newStored(storedConstructor, constructorParameters);
	}	

}
