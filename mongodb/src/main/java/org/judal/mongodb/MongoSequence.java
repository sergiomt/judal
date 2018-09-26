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

import javax.jdo.datastore.Sequence;

import org.bson.Document;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;

public class MongoSequence implements Sequence {

	private String sequenceName;
	private MongoCollection<Document> collection;

	public MongoSequence(String name, MongoCollection<Document> mongoCollection) {
		collection = mongoCollection;
		sequenceName = name;
	}

	@Override
	public void allocate(int unused) {
	}

	@Override
	public Long current() {
		return collection.find(Filters.eq("_id", sequenceName)).first().getLong("seq");
	}

	@Override
	public long currentValue() {
		return current();
	}

	@Override
	public String getName() {
		return sequenceName;
	}

	@Override
	public Long next() {
	    Document inc = new Document();
	    inc.put("$inc", new Document("seq", 1));
		collection.findOneAndUpdate(Filters.eq("_id", sequenceName), inc);
		return current();
	}

	@Override
	public long nextValue() {
		return next();
	}

}
