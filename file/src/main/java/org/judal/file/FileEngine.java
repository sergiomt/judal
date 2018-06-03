package org.judal.file;

import java.util.Map;

import javax.jdo.JDOException;
import javax.jdo.JDOUnsupportedOptionException;
import javax.transaction.TransactionManager;

import org.judal.storage.Engine;

import static org.judal.storage.EngineFactory.NAME_FILE;

public class FileEngine implements Engine<FileDataSource> {

	@Override
	public FileDataSource getDataSource(Map<String, String> properties) throws JDOException {
		return new FileDataSource(properties);
	}

	@Override
	public FileDataSource getDataSource(Map<String, String> properties, TransactionManager transactManager) {
		if (transactManager!=null)
			throw new JDOUnsupportedOptionException("FileDataSource does not support transactions");
		return new FileDataSource(properties);
	}

	@Override
	public TransactionManager getTransactionManager() throws JDOException {
		return null;
	}

	@Override
	public String name() {
		return NAME_FILE;
	}

}
