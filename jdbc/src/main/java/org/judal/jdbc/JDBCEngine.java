package org.judal.jdbc;

import java.util.Map;

import org.judal.storage.Engine;
import org.judal.storage.EngineFactory;

import javax.jdo.JDOException;
import javax.transaction.TransactionManager;

import org.judal.transaction.DataSourceTransactionManager;

public class JDBCEngine implements Engine<JDBCTableDataSource> {

	@Override
	public JDBCRelationalDataSource getDataSource(Map<String, String> properties) throws JDOException {
		try {
			return new JDBCRelationalDataSource(properties, getTransactionManager());
		} catch (Exception xcpt) {
			throw new JDOException(xcpt.getMessage(), xcpt);
		}
	}

	@Override
	public JDBCRelationalDataSource getDataSource(Map<String, String> properties, TransactionManager transactManager) throws JDOException {
		try {
			return new JDBCRelationalDataSource(properties, transactManager);
		} catch (Exception xcpt) {
			throw new JDOException(xcpt.getMessage(), xcpt);
		}
	}
	
	@Override
	public DataSourceTransactionManager getTransactionManager() throws JDOException {
		return DataSourceTransactionManager.Transact;
	}

	@Override
	public String name() {
		return EngineFactory.NAME_JDBC;
	}

}
