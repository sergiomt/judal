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

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.bson.Document;
import org.judal.metadata.ViewDef;

import com.mongodb.operation.BatchCursor;

public class MongoIterator implements Iterator<MongoDocument>, AutoCloseable {

	private final BatchCursor<Document> cursor;
	private final ViewDef tdef;
	private List<Document> curBatch;
	private int curPos;

	public MongoIterator(ViewDef tdef, final BatchCursor<Document> cursor) {
		this.tdef = tdef;
		this.cursor = cursor;
	}

	@Override
	public void close() {
		cursor.close();
	}

	@Override
	public boolean hasNext() {
		return curBatch != null || cursor.hasNext();
	}

	@Override
	public MongoDocument next() {
		if (!hasNext()) {
			throw new NoSuchElementException();
		}

		if (curBatch == null) {
			curBatch = cursor.next();
		}

		return getNextInBatch();
	}

	public MongoDocument tryNext() {
		if (curBatch == null) {
			curBatch = cursor.tryNext();
		}
		return curBatch == null ? null : getNextInBatch();
	}

	private MongoDocument getNextInBatch() {
		MongoDocument nextInBatch = new MongoDocument(tdef, curBatch.get(curPos));
		if (curPos < curBatch.size() - 1) {
			curPos++;
		} else {
			curBatch = null;
			curPos = 0;
		}
		return nextInBatch;
	}

}
