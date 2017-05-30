package org.judal.examples.java.jdbc;

import java.io.InputStream;
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
 * Example of how to create a JDBC data source 
 * for a second or third database other than
 * the main one used by your application.
 */
public class E02_CreateAdditionalDataSource {

	static final String PROPERTIES_NAMESPACE = "example";

	@Test
	public void demo() throws Exception {
		JDBCRelationalDataSource relationalDataSource = create();
		close(relationalDataSource);
	}
	
	public static JDBCRelationalDataSource create() throws Exception {
		
		try (InputStream props = getResourceAsStream("hsql.properties")) {
			// Properties for am HSQL DB in memory data source read from a properties file
			Map<String, String> properties = getDataSourceProperties(props, PROPERTIES_NAMESPACE);
			
			// Create a JDBC Engine
			Engine<JDBCRelationalDataSource> jdbc = new JDBCEngine();

			JDBCRelationalDataSource dataSource = jdbc.getDataSource(properties, Transact);

			return dataSource;
		}
	}

	public static void close(RelationalDataSource relationalDataSource) throws Exception {
		relationalDataSource.close();
	}
	
}