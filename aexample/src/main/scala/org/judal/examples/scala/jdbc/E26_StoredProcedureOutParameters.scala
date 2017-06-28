package org.judal.examples.scala.jdbc

import java.sql.Types

import org.junit.Test
import org.junit.Before
import org.junit.runner.RunWith

import org.junit.Assert.assertEquals

import org.mockito.invocation.InvocationOnMock
import org.mockito.junit.MockitoJUnitRunner
import org.mockito.stubbing.Answer

import org.mockito.Mockito.when
import org.mockito.Mockito.mock

import org.judal.storage.Param
import org.judal.storage.Param.Direction
import org.judal.jdbc.JDBCRelationalDataSource

/**
 * Example of how to call a stored procedure with output parameters
 */
@RunWith(classOf[MockitoJUnitRunner])
class E26_StoredProcedureOutParameters {

	val dts = mock(classOf[JDBCRelationalDataSource])

	val paramValue = "Hello!"

	// One input and one output parameter
	val twoParams = Array[Param](
			new Param("column1_name", Types.VARCHAR, 1, Direction.IN, paramValue),
			new Param("column2_name", Types.VARCHAR, 2, Direction.OUT) )
	
	@Test
	def demo() : Unit = {

		val retval = dts.call("your_procedure_name", twoParams:_*)

		assertEquals(paramValue.length, retval)
		assertEquals(paramValue, twoParams(1).getValue)

	}

	@Before
	def setUp() : Unit = {

		// HSQL does not support stored procedures, so mock it
		when(dts.call("your_procedure_name", twoParams:_*)).thenAnswer(new Answer[Int]() {
			def answer(invocation: InvocationOnMock) : Int = {
				val in = invocation.getArgument(1).asInstanceOf[Param]
				invocation.getArgument(2).asInstanceOf[Param].setValue(in.getValue)
				paramValue.length
			}
		})
		
	}
}
