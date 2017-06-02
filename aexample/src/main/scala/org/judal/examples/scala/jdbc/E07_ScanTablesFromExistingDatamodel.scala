package org.judal.examples.scala.jdbc

import org.junit.Test

import org.scalatest.Suite

import org.judal.jdbc.JDBCRelationalDataSource

import scala.collection.JavaConversions._

/**
 * Example of how to read the metadata of tables
 * already present in a datamodel.
 */
class E07_ScanTablesFromExistingDatamodel extends Suite {

	@Test def demo() {

		// Simulate a data model that already exists
		val dataSource = E07_ScanTablesFromExistingDatamodel.setUp()
		
		// Walk tables
		dataSource.getJDCTablesMap.entrySet.foreach { entry => println("Scanned table " + entry.getValue.getName) }

		// Walk schema objects
		dataSource.getMetaData.tables.foreach { tdef => println("Scanned table " + tdef.getName) }
		
		E07_ScanTablesFromExistingDatamodel.tearDown (dataSource)
	}
	
}

object E07_ScanTablesFromExistingDatamodel {

	def setUp() = {
		val dataSource = E02_CreateAdditionalDataSource.create()
		E06_CreateTablesFromDDLXML.createSchemaObjects(dataSource)
		dataSource
	}

	def tearDown(dataSource: JDBCRelationalDataSource) = {
		E06_CreateTablesFromDDLXML.dropSchemaObjects(dataSource)
		E02_CreateAdditionalDataSource.close(dataSource)
	}
  
}