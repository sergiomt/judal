package org.judal.examples.scala.jdbc

import org.junit.Test

import org.scalatest.Suite

import org.judal.storage.DataSource
import org.judal.storage.relational.RelationalDataSource

import org.judal.metadata.TableDef
import org.judal.metadata.SchemaMetaData
import org.judal.metadata.bind.JdoXmlMetadata

import org.judal.jdbc.JDBCRelationalDataSource

import org.judal.examples.Resources.getResourceAsStream

import collection.JavaConverters._

import scala.collection.mutable.MutableList
import scala.collection.JavaConversions._

/**
 * Example of how to create tables read from a JDO XML file
 */
class E04_CreateTablesFromJDOXML extends Suite {

	@Test def demo() = {
		val dataSource = E02_CreateAdditionalDataSource.create()
		
		E04_CreateTablesFromJDOXML.createSchemaObjects(dataSource)

		E04_CreateTablesFromJDOXML.dropSchemaObjects(dataSource)
		
		E02_CreateAdditionalDataSource.close(dataSource)
	}	
	
}

object E04_CreateTablesFromJDOXML {

	def createSchemaObjects(dataSource: JDBCRelationalDataSource) = {

		val xmlMeta = new JdoXmlMetadata (dataSource)
		
		// Load table definitions from a JDO XML file
		val metadata = xmlMeta.readMetadata(getResourceAsStream("jdometadata.xml"))		

		val options = Map[String,Object](
		    DataSource.CATALOG -> "PUBLIC",
		    DataSource.SCHEMA -> "PUBLIC" ).asJava

		// Create tables in the database
		metadata.tables.foreach { tdef => dataSource.createTable(tdef, options) }
	}

	def dropSchemaObjects(dataSource: RelationalDataSource) = {
		
		val xmlMeta = new JdoXmlMetadata (dataSource)
		
		// Load table definitions from a JDO XML file
		val metadata = xmlMeta.readMetadata(getResourceAsStream("jdometadata.xml"))		
		
		// Drop tables in the database in reverse order than they were created
		metadata.tables.toList.reverse.foreach { tdef => dataSource.dropTable(tdef.getName, false) }

	}
  
}