package org.judal.examples.java.jdbc;

import java.util.Map.Entry;

import org.junit.Test;
import org.judal.metadata.SchemaMetaData;
import org.judal.metadata.TableDef;
import org.judal.jdbc.JDBCRelationalDataSource;

/**
 * Example of how to read the metadata of tables
 * already present in a datamodel.
 */
public class E07_ScanTablesFromExistingDatamodel {

	@Test
	public void demo() throws Exception {

		// Simulate a data model that already exists
		JDBCRelationalDataSource dataSource = setUp();
		
		// Walk tables
		for (Entry<String,TableDef> entry : dataSource.getJDCTablesMap().entrySet())
			System.out.println("Scanned table " + entry.getValue().getName());

		// Walk schema objects
		SchemaMetaData schemaMetadata = dataSource.getMetaData();
		for (TableDef tableDef : schemaMetadata.tables())
			System.out.println("Scanned table " + tableDef.getName());
		
		tearDown (dataSource);
	}

	public static JDBCRelationalDataSource setUp() throws Exception {
		JDBCRelationalDataSource dataSource = E02_CreateAdditionalDataSource.create();
		E06_CreateTablesFromDDLXML.createSchemaObjects(dataSource);
		return dataSource;
	}

	public static void tearDown(JDBCRelationalDataSource dataSource) throws Exception {
		E06_CreateTablesFromDDLXML.dropSchemaObjects(dataSource);
		E02_CreateAdditionalDataSource.close(dataSource);
	}
	
}
