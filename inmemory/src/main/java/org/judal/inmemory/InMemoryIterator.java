package org.judal.inmemory;

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

import java.util.Iterator;

import java.lang.reflect.Constructor;

import org.judal.storage.keyvalue.Stored;
import org.judal.storage.table.Record;

/**
 * 
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class InMemoryIterator implements Iterator<Stored> {

	private Iterator<InMemoryRecord> iterator;
	private InMemoryBucket bucket;
	private InMemoryTableDef tableDef;
	private Class<? extends Record> resultClass;
	protected Constructor<? extends Record> recordConstructor;
	
	public InMemoryIterator(Class resultClass, InMemoryBucket bucket, InMemoryTableDef tableDef)
		throws NoSuchMethodException, SecurityException {
		this.bucket = bucket;
		this.tableDef = tableDef;
		this.resultClass = resultClass;
		this.recordConstructor = this.resultClass.getConstructor(tableDef.getClass());
		this.iterator = bucket.bucketData.values().iterator();
	}

	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	@Override
	public Stored next() {
		return iterator.next();
	}

}
