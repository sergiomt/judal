package org.judal.examples.java.jdbc;

import java.util.Map;
import java.util.HashMap;

import org.junit.Test;

import org.judal.storage.Engine;
import org.judal.storage.EngineFactory;
import org.judal.storage.StorageContext;
import org.judal.storage.relational.RelationalDataSource;

import static org.judal.storage.DataSource.*;

import org.judal.jdbc.JDBCEngine;
import org.judal.jdbc.JDBCRelationalDataSource;

import static org.judal.transaction.DataSourceTransactionManager.Transact;

/**
 * Example of how to create a JDBC data source and set it as default relational DataSource in the StorageContext
 *
 */
public class E01_CreateDefaultRelationalDataSource {

	public static JDBCRelationalDataSource create() throws Exception {
		
		// Properties for am HSQL DB in memory data source
		// Can be read from external properties files of servlet config
		// by using org.judal.storage.Env.getDataSourceProperties()
		Map<String, String> properties = new HashMap<>();
		properties.put(DRIVER, "org.hsqldb.jdbc.JDBCDriver");
		properties.put(URI, "jdbc:hsqldb:mem:test");
		properties.put(USER, "sa");
		properties.put(PASSWORD, "");
		properties.put(POOLSIZE, DEFAULT_MAXPOOLSIZE);
		properties.put(MAXPOOLSIZE, DEFAULT_MAXCONNECTIONS);
		properties.put(SCHEMA, "PUBLIC");
		properties.put(USE_DATABASE_METADATA, DEFAULT_USE_DATABASE_METADATA);

		// Create a JDBC Engine
		Engine<JDBCRelationalDataSource> jdbc = new JDBCEngine();
		
		// Register the JDBC Engine (this is optional)
		EngineFactory.registerEngine(jdbc.name(), jdbc.getClass().getName());

		// Use the Engine to create an instance of a RelationalDataSource
		JDBCRelationalDataSource dataSource = jdbc.getDataSource(properties, Transact);
		
		// Set this as the default data source from the current Thread with default transaction manager		
		EngineFactory.DefaultThreadDataSource.set(dataSource);

		return dataSource;
	}

	public static void close() throws Exception {
		EngineFactory.DefaultThreadDataSource.get().close();
	}
	
	@Test
	public void demo() throws Exception {
		create();
		close();
	}
	
}
