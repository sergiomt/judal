package org.judal.s3;

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

import java.util.Iterator;

import java.lang.reflect.Constructor;

import org.judal.storage.StorageObjectFactory;
import org.judal.storage.keyvalue.Stored;
import org.judal.storage.table.Record;

import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * 
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class S3Iterator implements Iterator<Stored> {

	private Iterator<S3ObjectSummary> iterator;
	private S3Bucket bucket;
	private S3TableDef tableDef;
	private Class<? extends Record> resultClass;
	protected Constructor<? extends Record> recordConstructor;
	
	public S3Iterator(Class resultClass, S3Bucket bucket, S3TableDef tableDef)
		throws NoSuchMethodException, SecurityException {
		this.bucket = bucket;
		this.tableDef = tableDef;
		this.resultClass = resultClass;
		this.recordConstructor = this.resultClass.getConstructor(tableDef.getClass());
		this.iterator = bucket.getClient().listObjects(bucket.name()).getObjectSummaries().iterator();
	}

	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	@Override
	public Stored next() {
		S3ObjectSummary s3obj = iterator.next();
		String key = s3obj.getKey();
		Stored obj;
		try {
			obj = StorageObjectFactory.newRecord(resultClass, tableDef);
		} catch (NoSuchMethodException neverthrown) { obj = null; }
		bucket.load(key, obj);
		return obj;
	}
	
}
