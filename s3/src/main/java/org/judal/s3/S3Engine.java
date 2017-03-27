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

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.judal.storage.DataSource;
import org.judal.storage.Engine;
import org.judal.storage.EngineFactory;

import javax.jdo.JDOException;
import javax.jdo.JDOUnsupportedOptionException;
import javax.transaction.TransactionManager;

import com.knowgate.debug.DebugFile;
import com.knowgate.debug.StackTraceUtil;

/**
 * Implementation of Engine interface for Amazon S3
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class S3Engine implements Engine<S3DataSource> {

	/**
	 * @param properties Map&lt;String,String&gt; Containing: region, uri, user and password key->value pairs
	 * @throws JDOException
	 */
	@Override
	public S3DataSource getDataSource(Map<String, String> properties) throws JDOException {
		try {
			return new S3DataSource(properties);
		} catch (Exception xcpt) {
			if (DebugFile.trace) {
				try {
					DebugFile.writeln(StackTraceUtil.getStackTrace(xcpt));
				} catch (IOException ignore) { }
				if (properties!=null) {
					Iterator<String> props = properties.keySet().iterator();
					String propName = props.next();
					if (!propName.equalsIgnoreCase(DataSource.PASSWORD) && !propName.equalsIgnoreCase(DataSource.SECRETKEY))
						DebugFile.writeln(propName+"="+properties.get(propName));
				}
			}
			throw new JDOException(xcpt.getMessage(), xcpt);
		}
	}

	/**
	 * @param properties Map&lt;String,String&gt; Containing: region, uri, user and password key->value pairs
	 * @param transactManager TransactionManager  Must be <b>null</b>
	 * @throws JDOUnsupportedOptionException If transactManager is not <b>null</b>
	 */
	@Override
	public S3DataSource getDataSource(Map<String, String> properties, TransactionManager transactManager)
			throws JDOException {
		if (transactManager==null)
			return getDataSource(properties);
		else
			throw new JDOUnsupportedOptionException("Amazon S3 does not support transactions");			
	}

	/**
	 * S3 does not support transactions. This method will always raise JDOUnsupportedOptionException
	 * @throws JDOUnsupportedOptionException
	 */
	@Override
	public TransactionManager getTransactionManager() throws JDOException {
		throw new JDOUnsupportedOptionException("Amazon S3 does not support transactions");
	}

	/**
	 * @return String Engine.NAME_AMAZONS3
	 */
	@Override
	public String name() {
		return EngineFactory.NAME_AMAZONS3;
	}

}