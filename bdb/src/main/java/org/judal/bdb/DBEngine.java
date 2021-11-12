package org.judal.bdb;

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

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Map;

import org.judal.metadata.SchemaMetaData;
import org.judal.metadata.bind.JdoXmlMetadata;
import org.judal.metadata.bind.JdoPackageMetadata;

import org.judal.storage.Env;
import org.judal.storage.DataSource;
import org.judal.storage.Engine;
import org.judal.storage.EngineFactory;

import javax.jdo.JDOException;
import javax.transaction.TransactionManager;

import org.judal.transaction.DataSourceTransactionManager;

public class DBEngine implements Engine<DBDataSource> {

	@Override
	public DBDataSource getDataSource(Map<String, String> properties, TransactionManager transactManager) throws JDOException {
		SchemaMetaData metadata;
		try {
			String metadataFilePath = Env.getString(properties, DataSource.METADATA, DataSource.DEFAULT_METADATA);
			String metadataPackage = Env.getString(properties, DataSource.PACKAGE, "");
			if (metadataPackage.length()==0) {
				FileInputStream fin = new FileInputStream(new File(metadataFilePath));
				JdoXmlMetadata xmlMeta = new JdoXmlMetadata(null);
				metadata = xmlMeta.readMetadata(fin);
				fin.close();
				return new DBTableDataSource(properties, transactManager, metadata);
			} else if (metadataPackage.length()>0) {
				DBDataSource retval = new DBTableDataSource(properties, transactManager, null);
				JdoPackageMetadata packMeta = new JdoPackageMetadata(retval, metadataPackage, metadataFilePath);
				InputStream instrm = packMeta.openStream();
				if (instrm!=null) {
					metadata = packMeta.readMetadata(instrm);
					retval = new DBTableDataSource(properties, transactManager, metadata);
					instrm.close();
					return retval;
				} else {
					return new DBBucketDataSource(properties, transactManager);					
				}
			} else {
				return new DBBucketDataSource(properties, transactManager);
			}
		} catch (Exception xcpt) {
			throw new JDOException(xcpt.getMessage(), xcpt);
		}
	}

	@Override
	public DBDataSource getDataSource(Map<String, String> properties) throws JDOException {
		return getDataSource(properties, getTransactionManager());
	}

	@Override
	public DataSourceTransactionManager getTransactionManager() throws JDOException {
		return DataSourceTransactionManager.Transact;
	}

	@Override
	public String name() {
		return EngineFactory.NAME_BERKELEYDB;
	}

}