package org.judal.halodb;

import java.io.File;
import java.io.FileInputStream;

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

import com.oath.halodb.HaloDBException;

import org.judal.storage.Engine;
import org.judal.storage.EngineFactory;

import javax.jdo.JDOException;
import javax.jdo.JDOUnsupportedOptionException;
import javax.transaction.TransactionManager;

/**
 * HaloDB implementation of Engine interface
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class HaloDBEngine implements Engine<HaloDBDataSource> {

	public HaloDBEngine() { }

	/**
	 * @param properties Map&lt;String,String&gt; Containing: region, uri, user and password key->value pairs
	 * @throws JDOException wrapper for HaloDBException
	 */
	@Override
	public HaloDBDataSource getDataSource(Map<String, String> properties) throws JDOException {
		try {
			return new HaloDBDataSource(properties);
		} catch (HaloDBException e) {
			throw new JDOException(e.getMessage(), e);
		}
	}

	/**
	 * @param properties Map&lt;String,String&gt; Containing: region, uri, user and password key->value pairs
	 * @param transactManager TransactionManager Must be <b>null</b>
	 * @throws JDOUnsupportedOptionException If transactManager is not <b>null</b>
	 */
	@Override
	public HaloDBDataSource getDataSource(Map<String, String> properties, TransactionManager transactManager)
			throws JDOException {
		if (transactManager==null)
			return getDataSource(properties);
		else
			throw new JDOUnsupportedOptionException("HaloDBDataSource does not support transactions");			
	}

	/**
	 * This method will always raise JDOUnsupportedOptionException
	 * @throws JDOUnsupportedOptionException HaloDBDataSource does not support transactions
	 */
	@Override
	public TransactionManager getTransactionManager() throws JDOException {
		throw new JDOUnsupportedOptionException("HaloDBDataSource does not support transactions");
	}

	/**
	 * @return String Engine.NAME_HALODB
	 */
	@Override
	public String name() {
		return EngineFactory.NAME_HALODB;
	}

}