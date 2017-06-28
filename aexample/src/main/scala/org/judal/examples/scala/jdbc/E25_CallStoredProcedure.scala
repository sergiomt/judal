package org.judal.examples.scala.jdbc

import java.sql.Types

import org.junit.Test
import org.junit.Before
import org.junit.runner.RunWith

import org.junit.Assert.assertEquals

import org.mockito.junit.MockitoJUnitRunner
import org.mockito.Mockito.when
import org.mockito.Mockito.mock

import org.judal.storage.Param
import org.judal.jdbc.JDBCRelationalDataSource

import scala.collection.JavaConverters._

/**
 * Example of how to call a stored procedure or PostgreSQL function and get its return value
 */
@RunWith(classOf[MockitoJUnitRunner])
class E25_CallStoredProcedure {

	val dts = mock(classOf[JDBCRelationalDataSource])

	val oneParam = new Param("column_name", Types.VARCHAR, 1, "Param Value")
	
	val procReturnValue = "Hello!"
	
	@Test
	def demo() : Unit = {
		
		val retval = dts.call("your_procedure_name", oneParam)		
		assertEquals(procReturnValue, retval)

		val resultset = dts.call("return_resultset", oneParam).asInstanceOf[java.util.List[java.util.Map[String,_]]]
		assertEquals(2, resultset.size)

	}

	@Before
	def setUp() : Unit = {
		val row1 = Map("column_name1" -> "column_value1", "column_name2" -> "column_value2", "column_name3" -> "column_value3").asJava
		val row2 = Map("column_name1" -> "column_value4", "column_name2" -> "column_value5", "column_name3" -> "column_value6").asJava
		val rows = List(row1, row2).asJava

		// HSQL does not support stored procedures, so mock it
		when(dts.call("your_procedure_name", oneParam)).thenReturn(procReturnValue, null)
		when(dts.call("return_resultset", oneParam)).thenReturn(rows, null)

	}
}
