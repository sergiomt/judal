package org.judal.examples.scala.jdbc

import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement

import org.junit.Test

import org.judal.storage.EngineFactory
import org.judal.jdbc.JDBCRelationalDataSource

class E30_GetNativeConnection {

	@Test
	def demo() : Unit = {
		
		E30_GetNativeConnection.setUp()

		val dts = EngineFactory.getDefaultRelationalDataSource.asInstanceOf[JDBCRelationalDataSource]

		val conn = dts.getConnection
		val stmt = conn.createStatement
		val rset = stmt.executeQuery("SELECT * FROM student")

		// ...

		rset.close
		stmt.close
		conn.close

		E30_GetNativeConnection.tearDown()
	}

}


object E30_GetNativeConnection {
  
	def setUp() = E10_WriteCSVDataIntoTheDatabase.setUp()

	def tearDown() = E10_WriteCSVDataIntoTheDatabase.tearDown()

}