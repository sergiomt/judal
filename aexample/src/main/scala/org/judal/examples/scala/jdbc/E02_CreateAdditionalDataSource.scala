package org.judal.examples.scala.jdbc

import org.junit.Test

import org.scalatest.Suite

import java.io.InputStream

import org.judal.Using._
import org.judal.storage.Engine
import org.judal.storage.relational.RelationalDataSource

import org.judal.storage.Env.getDataSourceProperties

import org.judal.jdbc.JDBCEngine
import org.judal.jdbc.JDBCRelationalDataSource

import org.judal.examples.Resources.getResourceAsStream

import org.judal.transaction.DataSourceTransactionManager.Transact

/**
 * Example of how to create a JDBC data source 
 * for a second or third database other than
 * the main one used by your application.
 */

class E02_CreateAdditionalDataSource extends Suite {

	@Test def demo() = {
		val relationalDataSource = E02_CreateAdditionalDataSource.create()
		E02_CreateAdditionalDataSource.close(relationalDataSource)
	}

}

object E02_CreateAdditionalDataSource {

	val PROPERTIES_NAMESPACE = "example"
	
	def create() = {

	  var props = getResourceAsStream("hsql.properties")
	  var dataSource : JDBCRelationalDataSource = null
	  
		using (props) {
			// Properties for am HSQL DB in memory data source read from a properties file
			val properties = getDataSourceProperties(props, PROPERTIES_NAMESPACE)

			dataSource = new JDBCEngine().getDataSource(properties, Transact)
		}
		dataSource
	}

	def close(relationalDataSource: RelationalDataSource) = relationalDataSource.close
	
}