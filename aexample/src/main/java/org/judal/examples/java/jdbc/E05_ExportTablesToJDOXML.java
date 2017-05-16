package org.judal.examples.java.jdbc;

import org.junit.Test;

import org.judal.metadata.SchemaMetaData;
import org.judal.metadata.bind.JdoXmlMetadata;

import org.judal.jdbc.JDBCRelationalDataSource;

import org.judal.examples.StudentCourseSchema;

/**
 * Programmatically create schema meta data and output it as JDO XML
 */
public class E05_ExportTablesToJDOXML {

	@Test
	public void demo() throws Exception {

		JDBCRelationalDataSource dataSource = E02_CreateAdditionalDataSource.create();
		
		SchemaMetaData schemaMetaData = StudentCourseSchema.generateSchemaMetaData(dataSource);
		
		JdoXmlMetadata xmlMeta = new JdoXmlMetadata (dataSource);

		xmlMeta.writeMetadata(schemaMetaData, System.out);

		E02_CreateAdditionalDataSource.close(dataSource);
	}

}
