package org.judal.inmemory;

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

import java.io.InputStream;
import java.util.Map;

import org.judal.metadata.SchemaMetaData;
import org.judal.metadata.bind.JdoPackageMetadata;
import org.judal.metadata.bind.JdoXmlMetadata;
import org.judal.storage.DataSource;
import org.judal.storage.Engine;
import org.judal.storage.EngineFactory;
import org.judal.storage.Env;

import javax.jdo.JDOException;
import javax.jdo.JDOUnsupportedOptionException;
import javax.transaction.TransactionManager;

/**
 * In-Memory implementation of Engine interface
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class InMemoryEngine implements Engine<InMemoryDataSource> {

	/**
	 * @param properties Map&lt;String,String&gt; Containing: region, uri, user and password key->value pairs
	 * @throws JDOException
	 */
	@Override
	public InMemoryDataSource getDataSource(Map<String, String> properties) throws JDOException {
		SchemaMetaData metadata;
		try {
			String metadataFilePath = Env.getString(properties, DataSource.METADATA, "");
			String metadataPackage = Env.getString(properties, DataSource.PACKAGE, "");
			if (metadataFilePath.length()==0 && metadataPackage.length()==0) {
				return new InMemoryDataSource(properties);
			}
			else if (metadataPackage.length()==0) {
				FileInputStream fin = new FileInputStream(new File(metadataFilePath));
				JdoXmlMetadata xmlMeta = new JdoXmlMetadata(null);
				metadata = xmlMeta.readMetadata(fin);
				fin.close();
				return new InMemoryDataSource(properties, metadata);
			} else if (metadataPackage.length()>0) {
				InMemoryDataSource retval = new InMemoryDataSource(properties);
				JdoPackageMetadata packMeta = new JdoPackageMetadata(retval, metadataPackage, metadataFilePath);
				InputStream instrm = packMeta.openStream();
				if (instrm!=null) {
					metadata = packMeta.readMetadata(instrm);
					retval = new InMemoryDataSource(properties, metadata);
					instrm.close();
					return retval;
				} else {
					return new InMemoryDataSource(properties);
				}
			} else {
				return new InMemoryDataSource(properties);
			}
		} catch (Exception xcpt) {
			throw new JDOException(xcpt.getMessage(), xcpt);
		}
	}

	/**
	 * @param properties Map&lt;String,String&gt; Containing: region, uri, user and password key->value pairs
	 * @param transactManager TransactionManager  Must be <b>null</b>
	 * @throws JDOUnsupportedOptionException If transactManager is not <b>null</b>
	 */
	@Override
	public InMemoryDataSource getDataSource(Map<String, String> properties, TransactionManager transactManager)
			throws JDOException {
		if (transactManager==null)
			return getDataSource(properties);
		else
			throw new JDOUnsupportedOptionException("InMemoryDataSource does not support transactions");			
	}

	/**
	 * This method will always raise JDOUnsupportedOptionException
	 * @throws JDOUnsupportedOptionException InMemoryDataSource does not support transactions
	 */
	@Override
	public TransactionManager getTransactionManager() throws JDOException {
		throw new JDOUnsupportedOptionException("InMemoryDataSource does not support transactions");
	}

	/**
	 * @return String Engine.NAME_INMEMORY
	 */
	@Override
	public String name() {
		return EngineFactory.NAME_INMEMORY;
	}

}