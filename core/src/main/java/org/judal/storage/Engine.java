package org.judal.storage;

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

import javax.jdo.JDOException;
import javax.transaction.TransactionManager;

/**
 * Interface for Engine instances.
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public interface Engine<GenericDataSource extends DataSource> {

	/**
	 * Get new DataSource
	 * @param properties Map Valid property names are listed at DataSource.PropertyNames
	 * @return GenericDataSource
	 * @throws JDOException
	 */
	GenericDataSource getDataSource (Map<String,String> properties) throws JDOException;

	/**
	 * Get new DataSource using the given TransactionManager
	 * @param properties Map Valid property names are listed at DataSource.PropertyNames
	 * @param transactManager TransactionManager
	 * @return GenericDataSource
	 * @throws JDOException
	 */
	GenericDataSource getDataSource (Map<String,String> properties, TransactionManager transactManager) throws JDOException;
	
	/**
	 * @return TransactionManager
	 * @throws JDOException
	 */
	TransactionManager getTransactionManager () throws JDOException;

	/**
	 * Get Engine name
	 * @return String
	 */
	String name();
}
