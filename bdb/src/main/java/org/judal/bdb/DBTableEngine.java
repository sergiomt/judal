package org.judal.bdb;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Map;

import javax.jdo.JDOException;
import javax.jdo.JDOUserException;
import javax.transaction.TransactionManager;

import org.judal.metadata.SchemaMetaData;
import org.judal.metadata.bind.JdoPackageMetadata;
import org.judal.metadata.bind.JdoXmlMetadata;
import org.judal.storage.DataSource;
import org.judal.storage.Engine;
import org.judal.storage.EngineFactory;
import org.judal.storage.Env;
import org.judal.transaction.DataSourceTransactionManager;

public class DBTableEngine implements Engine<DBTableDataSource>  {

	@Override
	public DBTableDataSource getDataSource(Map<String, String> properties, TransactionManager transactManager) throws JDOException {
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
				JdoPackageMetadata packMeta = new JdoPackageMetadata(null, metadataPackage, metadataFilePath);
				InputStream instrm = packMeta.openStream();
				if (instrm!=null) {
					metadata = packMeta.readMetadata(instrm);
					DBTableDataSource retval = new DBTableDataSource(properties, transactManager, metadata);
					instrm.close();
					return retval;
				} else {
					throw new JDOUserException("Could not load metadata for package " + metadataPackage + " file " + metadataFilePath);					
				}
			} else {
				throw new JDOUserException("Missing metadata package and no schema file specified");					
			}
		} catch (Exception xcpt) {
			throw new JDOException(xcpt.getMessage(), xcpt);
		}
	}

	@Override
	public DBTableDataSource getDataSource(Map<String, String> properties) throws JDOException {
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
