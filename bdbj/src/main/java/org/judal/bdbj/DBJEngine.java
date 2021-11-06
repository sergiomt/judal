package org.judal.bdbj;

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

import java.util.Map;

import org.judal.metadata.SchemaMetaData;

import org.judal.storage.Engine;
import org.judal.storage.EngineFactory;

import javax.jdo.JDOException;
import javax.transaction.TransactionManager;

import org.judal.transaction.DataSourceTransactionManager;

public class DBJEngine implements Engine<DBJDataSource> {

	@Override
	public DBJDataSource getDataSource(Map<String, String> properties, TransactionManager transactManager) throws JDOException {
		SchemaMetaData metadata;
		try {
			return new DBJBucketDataSource(properties, transactManager);
		} catch (Exception xcpt) {
			throw new JDOException(xcpt.getMessage(), xcpt);
		}
	}

	@Override
	public DBJDataSource getDataSource(Map<String, String> properties) throws JDOException {
		return getDataSource(properties, getTransactionManager());
	}

	@Override
	public DataSourceTransactionManager getTransactionManager() throws JDOException {
		return DataSourceTransactionManager.Transact;
	}

	@Override
	public String name() {
		return EngineFactory.NAME_BERKELEYDB_JAVA;
	}

}