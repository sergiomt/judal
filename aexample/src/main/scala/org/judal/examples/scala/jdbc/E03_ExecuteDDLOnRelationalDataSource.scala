package org.judal.examples.scala.jdbc

import org.junit.Test
import org.junit.Assert.assertTrue

import org.scalatest.Suite

import org.judal.jdbc.JDBCRelationalDataSource

/**
 * Example of how to execute any SQL statement over a JDBC data source
 */
class E03_ExecuteDDLOnRelationalDataSource extends Suite {

	@Test def demo() = {
		val dataSource = E02_CreateAdditionalDataSource.create
		
		dataSource.execute ("CREATE TABLE test_table (column1 INTEGER, column2 VARCHAR(100))")
		
		// Model changes won't be visible until the data sources is restarted
		dataSource.restart
		
		assertTrue (dataSource.exists("test_table", "U"))
		
		dataSource.execute ("DROP TABLE test_table")
		
		E02_CreateAdditionalDataSource.close (dataSource)
	}
	
}
