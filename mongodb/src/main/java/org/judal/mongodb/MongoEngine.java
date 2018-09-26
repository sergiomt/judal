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

import java.util.Map;

import javax.jdo.JDOException;
import javax.jdo.JDOUnsupportedOptionException;
import javax.transaction.TransactionManager;

import org.judal.storage.Engine;

import static org.judal.storage.EngineFactory.NAME_MONGODB;

public class MongoEngine implements Engine<MongoDataSource> {

	@Override
	public MongoDataSource getDataSource(Map<String, String> properties) throws JDOException {
		return new MongoDataSource(properties);
	}

	@Override
	public MongoDataSource getDataSource(Map<String, String> properties, TransactionManager transactManager) {
		if (transactManager!=null)
			throw new JDOUnsupportedOptionException("MongoDataSource does not support transactions");
		return new MongoDataSource(properties);
	}

	@Override
	public TransactionManager getTransactionManager() throws JDOException {
		return null;
	}

	@Override
	public String name() {
		return NAME_MONGODB;
	}

}
