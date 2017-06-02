package org.judal.examples.scala.jdbc

import org.junit.Test
import org.scalatest.Suite

import org.judal.storage.DataSource

import org.judal.jdbc.metadata.SQLXmlMetadata
import org.judal.jdbc.JDBCRelationalDataSource

import org.judal.examples.Resources.getResourceAsStream

import collection.JavaConverters._

import scala.collection.JavaConversions._

/**
 * Example of how to create tables read from a DDL XML file
 * @see https://db.apache.org/ddlutils/schema/
 */
class E06_CreateTablesFromDDLXML extends Suite {

	@Test def demo() = {

		val dataSource = E02_CreateAdditionalDataSource.create()
				
		E06_CreateTablesFromDDLXML.createSchemaObjects(dataSource)
		
		E06_CreateTablesFromDDLXML.dropSchemaObjects(dataSource)
		
		E02_CreateAdditionalDataSource.close(dataSource)
	}
		
}

object E06_CreateTablesFromDDLXML {

	def createSchemaObjects(dataSource: JDBCRelationalDataSource) = {		
		val xmlMeta = new SQLXmlMetadata (dataSource)

		// Load table definitions from a DDL XML file
		val metadata = xmlMeta.readMetadata(getResourceAsStream("ddlmetadata.xml"))

		val options = Map[String,Object](
		    DataSource.CATALOG -> "PUBLIC",
		    DataSource.SCHEMA -> "PUBLIC" ).asJava
		
		metadata.tables.foreach { tdef => dataSource.createTable(tdef, options) }
		    			
		metadata.indexes.foreach { idef => dataSource.createIndex(idef) }
	}

	def dropSchemaObjects(dataSource: JDBCRelationalDataSource) = {
		
		val metadata = new SQLXmlMetadata (dataSource).readMetadata(getResourceAsStream("ddlmetadata.xml"))

		metadata.tables.toList.reverse.foreach { tdef => dataSource.dropTable(tdef.getName, false) }
		
	}
  
}
