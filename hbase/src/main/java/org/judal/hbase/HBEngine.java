package org.judal.hbase;

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
import java.util.Map;

import javax.jdo.JDOException;
import javax.jdo.JDOUnsupportedOptionException;
import javax.transaction.TransactionManager;

import org.judal.storage.DataSource;
import org.judal.storage.Engine;
import org.judal.storage.EngineFactory;
import org.judal.storage.Env;
import org.judal.storage.table.TableDataSource;
import org.judal.metadata.SchemaMetaData;
import org.judal.metadata.bind.JdoPackageMetadata;
import org.judal.metadata.bind.JdoXmlMetadata;

/**
 * Implementation of Engine interface for HBase
 * @author Sergio Montoro Ten
 *
 */
public class HBEngine implements Engine<TableDataSource> {

	@Override
	public HBTableDataSource getDataSource(Map<String, String> properties) throws JDOException {
		SchemaMetaData metadata;
		try {
			String metadataFilePath = Env.getString(properties, DataSource.METADATA, DataSource.DEFAULT_METADATA);
			String metadataPackage = Env.getString(properties, DataSource.PACKAGE, "");
			if (metadataPackage.length()>0) {
				JdoPackageMetadata packMeta = new JdoPackageMetadata(null, metadataPackage, metadataFilePath);
				metadata = packMeta.readMetadata(packMeta.openStream());
				return new HBTableDataSource(properties.get(DataSource.CONFIG), metadata);
			} else if (metadataFilePath.length()>0) {
				FileInputStream fin = new FileInputStream(new File(metadataFilePath));
				JdoXmlMetadata xmlMeta = new JdoXmlMetadata(null);
				metadata = xmlMeta.readMetadata(fin);
				fin.close();
				return new HBTableDataSource(properties.get(DataSource.CONFIG), metadata);
			} else {
				throw new JDOException("Either "+DataSource.PACKAGE+" or "+DataSource.METADATA+" property is required");
			}
		} catch (Exception xcpt) {
			throw new JDOException(xcpt.getMessage(), xcpt);
		}
	}
	
	@Override
	public HBTableDataSource getDataSource(Map<String, String> properties, TransactionManager transactManager) throws JDOException {
		if (transactManager != null)
			throw new JDOUnsupportedOptionException("HBase does not support transactions");
		else
			return getDataSource(properties);
	}

	@Override
	public TransactionManager getTransactionManager() throws JDOException {
		return null;
	}

	@Override
	public String name() {
		return EngineFactory.NAME_HBASE;
	}

}
