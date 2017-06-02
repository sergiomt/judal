package org.judal.examples.scala.jdbc

import org.junit.Test

import org.scalatest.Suite

import org.judal.metadata.bind.JdoXmlMetadata

import org.judal.examples.StudentCourseSchema

/**
 * Example of how to programmatically create schema meta data and output it as JDO XML
 */
class E05_ExportTablesToJDOXML extends Suite {

	@Test def demo() = {

		val dataSource = E02_CreateAdditionalDataSource.create()
		
		val schemaMetaData = StudentCourseSchema.generateSchemaMetaData(dataSource)
		
		val xmlMeta = new JdoXmlMetadata (dataSource)

		xmlMeta.writeMetadata(schemaMetaData, System.out)

		E02_CreateAdditionalDataSource.close(dataSource)
	}

}
