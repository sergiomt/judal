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
import java.util.Iterator;

import com.oath.halodb.HaloDBException;
import com.oath.halodb.Record;
import org.judal.storage.keyvalue.Stored;

/**
 * Iterator records of an HaloDB database belonging to the same bucket
 * This class is not thread safe
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class HaloDBIterator implements Iterator<Stored> {

	private final HaloDBBucket bucket;
	private final com.oath.halodb.HaloDBIterator iterator;
	private Stored nextOne;

	public HaloDBIterator(HaloDBBucket bucket) {
		this.bucket = bucket;
		this.nextOne = null;
		try {
			iterator = bucket.getDataSource().getDatabase().newIterator();
			seekNextInBucket();
		} catch (HaloDBException e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}

	@Override
	public boolean hasNext() {
		return nextOne!=null;
	}

	@Override
	public Stored next() {
		Stored theNext = nextOne;
		if (theNext!=null)
			seekNextInBucket();
		return theNext;
	}

	private void seekNextInBucket() {
		nextOne = null;
		while (iterator.hasNext()) {
			Record nextRecord = iterator.next();
			try (ByteArrayInputStream bin = new ByteArrayInputStream(nextRecord.getValue())) {
				try (ObjectInputStream oin = new ObjectInputStream(bin)) {
					final Object obj = oin.readObject();
					if (obj instanceof Stored) {
						final Stored strd = (Stored) obj;
						if (bucket.name().equalsIgnoreCase(strd.getBucketName())) {
							nextOne = strd;
							break;
						}
					}
				}
			} catch (ClassNotFoundException | IOException e) {
				nextOne = null;
			}
		}
	}
}
