package org.judal.jdbc;

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

import java.util.Map;

import org.judal.storage.Engine;
import org.judal.storage.EngineFactory;

import javax.jdo.JDOException;
import javax.transaction.TransactionManager;

import org.judal.transaction.DataSourceTransactionManager;

/**
 * JDBC Engine to create relational data sources.
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class JDBCEngine implements Engine<JDBCRelationalDataSource> {

	/**
	 * Get new JDBCRelationalDataSource
	 * @param properties Map Valid property names are listed at DataSource.PropertyNames
	 * @return JDBCRelationalDataSource
	 * @throws JDOException
	 */
	@Override
	public JDBCRelationalDataSource getDataSource(Map<String, String> properties) throws JDOException {
		try {
			return new JDBCRelationalDataSource(properties, getTransactionManager());
		} catch (Exception xcpt) {
			throw new JDOException(xcpt.getMessage(), xcpt);
		}
	}

	/**
	 * Get new JDBCRelationalDataSource using the given TransactionManager
	 * @param properties Map Valid property names are listed at DataSource.PropertyNames
	 * @param transactManager TransactionManager
	 * @return JDBCRelationalDataSource
	 * @throws JDOException
	 */
	@Override
	public JDBCRelationalDataSource getDataSource(Map<String, String> properties, TransactionManager transactManager) throws JDOException {
		try {
			return new JDBCRelationalDataSource(properties, transactManager);
		} catch (Exception xcpt) {
			throw new JDOException(xcpt.getMessage(), xcpt);
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public DataSourceTransactionManager getTransactionManager() throws JDOException {
		return DataSourceTransactionManager.Transact;
	}

	/**
	 * @return String EngineFactory.NAME_JDBC
	 */
	@Override
	public String name() {
		return EngineFactory.NAME_JDBC;
	}

}