package org.judal.examples.scala.jdbc

import org.junit.Test

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull

import org.judal.storage.Engine
import org.judal.storage.EngineFactory

import org.judal.storage.DataSource._

import org.judal.jdbc.JDBCEngine
import org.judal.jdbc.JDBCRelationalDataSource

import org.judal.transaction.DataSourceTransactionManager.Transact

import collection.JavaConverters._

/**
 * Example of how to create a JDBC data source and set it as default
 * relational DataSource for the current Thread
 *
 */
object E01_CreateDefaultRelationalDataSource {

	def create() = {
		
		// Properties for am HSQL DB in memory data source
		// Can be read from external properties files of servlet config
		// by using org.judal.storage.Env.getDataSourceProperties()
		val properties = Map(DRIVER -> "org.hsqldb.jdbc.JDBCDriver",
                         URI -> "jdbc:hsqldb:mem:test",
                         USER -> "sa",
                         PASSWORD -> "",
                         POOLSIZE -> DEFAULT_POOLSIZE,
                         MAXPOOLSIZE -> DEFAULT_MAXPOOLSIZE,
                         SCHEMA -> "PUBLIC",
                         USE_DATABASE_METADATA -> DEFAULT_USE_DATABASE_METADATA).asJava

		// Create a JDBC Engine
		val jdbc : Engine[JDBCRelationalDataSource] = new JDBCEngine
		
		// Register the JDBC Engine (this is optional)
		EngineFactory.registerEngine (jdbc.name, jdbc.getClass.getName)

		// Use the Engine to create an instance of a RelationalDataSource
		val dataSource = jdbc.getDataSource(properties, Transact)
		
		assertNotNull(dataSource)
		assertEquals("jdbc:hsqldb:mem:test", dataSource.getProperty(URI))
		
		// Set this as the default data source for the current Thread with default transaction manager		
		EngineFactory.DefaultThreadDataSource.set (dataSource)

		dataSource
	}

	def close() = EngineFactory.DefaultThreadDataSource.get.close

}

class E01_CreateDefaultRelationalDataSource {

  @Test
	def demo() = {
		E01_CreateDefaultRelationalDataSource.create()
		E01_CreateDefaultRelationalDataSource.close ()
	}
  
}
