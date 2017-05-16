package org.judal.examples.java.jdbc;

import java.util.Map;

import org.junit.Test;

import org.judal.storage.Engine;
import org.judal.storage.relational.RelationalDataSource;

import static org.judal.storage.Env.getDataSourceProperties;

import org.judal.jdbc.JDBCEngine;
import org.judal.jdbc.JDBCRelationalDataSource;

import static org.judal.examples.Resources.getResourceAsStream;

import static org.judal.transaction.DataSourceTransactionManager.Transact;

/**
 * Example of how to create a JDBC data source maybe to a second or third database other than the main one used by your application
 */
public class E02_CreateAdditionalDataSource {

	@Test
	public void demo() throws Exception {
		JDBCRelationalDataSource relationalDataSource = create();
		close(relationalDataSource);
	}
	
	public static JDBCRelationalDataSource create() throws Exception {
		
		// Properties for am HSQL DB in memory data source read from a properties file		
		Map<String, String> properties = getDataSourceProperties(getResourceAsStream("hsql.properties"), "example");

		// Create a JDBC Engine
		Engine<JDBCRelationalDataSource> jdbc = new JDBCEngine();

		return jdbc.getDataSource(properties, Transact);

	}

	public static void close(RelationalDataSource relationalDataSource) throws Exception {
		relationalDataSource.close();
	}
	
}