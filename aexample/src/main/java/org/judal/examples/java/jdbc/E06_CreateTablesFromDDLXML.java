package org.judal.examples.java.jdbc;

import org.junit.Test;
import org.judal.metadata.IndexDef;
import org.judal.metadata.SchemaMetaData;
import org.judal.metadata.TableDef;
import org.judal.storage.DataSource;
import org.judal.jdbc.metadata.SQLXmlMetadata;
import org.judal.jdbc.JDBCRelationalDataSource;

import static org.judal.examples.Resources.getResourceAsStream;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import javax.jdo.JDOException;

/**
 * Example of how to create tables read from a DDL XML file
 * @see https://db.apache.org/ddlutils/schema/
 */
public class E06_CreateTablesFromDDLXML {

	@Test
	public void demo() throws Exception {

		JDBCRelationalDataSource dataSource = E02_CreateAdditionalDataSource.create();
				
		createSchemaObjects(dataSource);
		
		dropSchemaObjects(dataSource);
		
		E02_CreateAdditionalDataSource.close(dataSource);
	}
	
	public static void createSchemaObjects(JDBCRelationalDataSource dataSource) throws JDOException, IOException {
		
		SQLXmlMetadata xmlMeta = new SQLXmlMetadata (dataSource);

		// Load table definitions from a DDL XML file
		SchemaMetaData metadata = xmlMeta.readMetadata(getResourceAsStream("ddlmetadata.xml"));

		Map<String,Object> options = new HashMap<>();
		options.put(DataSource.CATALOG, dataSource.getProperties().getOrDefault(DataSource.CATALOG, ""));
		options.put(DataSource.SCHEMA, dataSource.getProperties().getOrDefault(DataSource.CATALOG, "PUBLIC"));
		
		// Create tables in the database
		for (TableDef tdef : metadata.tables())
			dataSource.createTable(tdef, options);

		// Create indexes in the database
		for (IndexDef idef : metadata.indexes())
			dataSource.createIndex(idef);		
	}

	public static void dropSchemaObjects(JDBCRelationalDataSource dataSource) throws JDOException, IOException {
		
		SQLXmlMetadata xmlMeta = new SQLXmlMetadata (dataSource);
		
		SchemaMetaData metadata = xmlMeta.readMetadata(getResourceAsStream("ddlmetadata.xml"));
		
		// Reverse the table list
		LinkedList<String> tableNames = new LinkedList<>();
		for (TableDef tdef : metadata.tables())
			tableNames.addFirst(tdef.getName());
		
		// Drop tables in the database
		for (String tableName: tableNames)
			dataSource.dropTable(tableName, false);
	}
	
}
