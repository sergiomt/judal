package org.judal.examples.java.jdbc;

import java.util.Map;

import javax.jdo.JDOException;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;

import org.junit.Test;
import static org.junit.Assert.assertTrue;

import org.judal.storage.DataSource;
import org.judal.storage.relational.RelationalDataSource;
import org.judal.metadata.TableDef;
import org.judal.jdbc.JDBCRelationalDataSource;
import org.judal.metadata.SchemaMetaData;
import org.judal.metadata.bind.JdoXmlMetadata;

import static org.judal.examples.Resources.getResourceAsStream;

/**
 * Example of how to create tables read from a JDO XML file
 */
public class E04_CreateTablesFromJDOXML {

	@Test
	public void demo() throws Exception {
		JDBCRelationalDataSource dataSource = E02_CreateAdditionalDataSource.create();
		
		createSchemaObjects(dataSource);

		dropSchemaObjects(dataSource);
		
		E02_CreateAdditionalDataSource.close(dataSource);
	}	

	public static void createSchemaObjects(JDBCRelationalDataSource dataSource) throws JDOException, IOException {

		JdoXmlMetadata xmlMeta = new JdoXmlMetadata (dataSource);
		
		// Load table definitions from a JDO XML file
		SchemaMetaData metadata = xmlMeta.readMetadata(getResourceAsStream("jdometadata.xml"));		

		Map<String,Object> options = new HashMap<>();
		options.put(DataSource.CATALOG, "PUBLIC");
		options.put(DataSource.SCHEMA, "PUBLIC");
		
		// Create tables in the database
		for (TableDef tdef : metadata.tables())
			dataSource.createTable(tdef, options);
		
		assertTrue(dataSource.exists("student", "U"));
		assertTrue(dataSource.exists("course", "U"));
		assertTrue(dataSource.exists("student_x_course", "U"));
	}

	public static void dropSchemaObjects(RelationalDataSource dataSource) throws JDOException, IOException {
		
		JdoXmlMetadata xmlMeta = new JdoXmlMetadata (dataSource);
		
		// Load table definitions from a JDO XML file
		SchemaMetaData metadata = xmlMeta.readMetadata(getResourceAsStream("jdometadata.xml"));		
		
		// Reverse the table list
		LinkedList<String> tableNames = new LinkedList<>();
		for (TableDef tdef : metadata.tables())
			tableNames.addFirst(tdef.getName());
		
		// Drop tables in the database
		for (String tableName: tableNames)
			dataSource.dropTable(tableName, false);
	}
	
}
