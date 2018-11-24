package org.judal.examples.java.jdbc;

import java.util.Map;
import java.util.HashMap;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.judal.storage.Engine;
import org.judal.storage.EngineFactory;

import static org.judal.storage.DataSource.*;

import org.judal.jdbc.JDBCEngine;
import org.judal.jdbc.JDBCRelationalDataSource;

import static org.judal.transaction.DataSourceTransactionManager.Transact;

/**
 * Example of how to create a JDBC data source and set it as default
 * relational DataSource for the current Thread
 *
 */
public class E01_CreateDefaultRelationalDataSource {

	private static Engine<JDBCRelationalDataSource> jdbc;

	public static JDBCRelationalDataSource create() throws Exception {
		
		// Create a JDBC Engine
		jdbc = new JDBCEngine();
		
		// Register the JDBC Engine (this is optional)
		EngineFactory.registerEngine(jdbc.name(), jdbc.getClass().getName());

		assertNotNull(EngineFactory.getEngine(jdbc.name()));
		
		// Use the Engine to create an instance of a RelationalDataSource
		JDBCRelationalDataSource dataSource = jdbc.getDataSource(dataSourceProperties(), Transact);
		
		assertNotNull(dataSource);
		assertEquals("jdbc:hsqldb:mem:test", dataSource.getProperty(URI));
		
		// Set this as the default data source for the current Thread with default transaction manager		
		EngineFactory.DefaultThreadDataSource.set(dataSource);

		return dataSource;
	}

	public static void close() throws Exception {
		assertNotNull(EngineFactory.getEngine(jdbc.name()));
		EngineFactory.DefaultThreadDataSource.get().close();
		EngineFactory.DefaultThreadDataSource.remove();
		EngineFactory.deregisterEngine(jdbc.name());
	}

	public static Map<String, String> dataSourceProperties() {
		
		// Properties for am HSQL DB in memory data source
		// Can be read from external properties files of servlet config
		// by using org.judal.storage.Env.getDataSourceProperties()
		Map<String, String> properties = new HashMap<>();
		properties.put(DRIVER, "org.hsqldb.jdbc.JDBCDriver");
		properties.put(URI, "jdbc:hsqldb:mem:test");
		properties.put(USER, "sa");
		properties.put(PASSWORD, "");
		properties.put(POOLSIZE, DEFAULT_POOLSIZE);
		properties.put(MAXPOOLSIZE, DEFAULT_MAXPOOLSIZE);
		properties.put(SCHEMA, "PUBLIC");
		properties.put(USE_DATABASE_METADATA, DEFAULT_USE_DATABASE_METADATA);
		properties.put(AUTOCOMMIT, "true");

		return properties;
	}
	
	@Test
	public void demo() throws Exception {
		create();
		close();
	}

}
