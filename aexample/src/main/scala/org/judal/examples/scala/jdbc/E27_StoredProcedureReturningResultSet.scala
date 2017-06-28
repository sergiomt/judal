package org.judal.examples.scala.jdbc

import java.sql.Types

import org.junit.Test
import org.junit.After
import org.junit.Before
import org.junit.runner.RunWith

import org.junit.Assert.assertEquals

import org.mockito.junit.MockitoJUnitRunner
import org.mockito.Mockito.when
import org.mockito.Mockito.mock

import org.judal.storage.Param
import org.judal.examples.java.model.map.Student
import org.judal.jdbc.JDBCRelationalDataSource

import scala.collection.JavaConverters._

/**
 * Example of how to call a stored procedure which returns a ResultSet
 */
@RunWith(classOf[MockitoJUnitRunner])
class E27_StoredProcedureReturningResultSet {

	val dts = mock(classOf[JDBCRelationalDataSource])

	val oneParam = Array[Param](new Param("column_name", Types.VARCHAR, 1, "Param Value"))
	
	@Test
	def demo() : Unit = {
		
		// ResultSet is returned as List of Map
		val resultSet = dts.call("your_procedure_name", oneParam:_*).asInstanceOf[java.util.List[java.util.Map[String,_]]].asScala
		
		var r = 0
		for (row <- resultSet) {
			
			val s = new Student()
			s.putAll(row.asInstanceOf[java.util.Map[_ <: String, _]])

			if (r==0)
				assertEquals(1, s.getId)

			if (r==1)
				assertEquals("Wrigth", s.getLastName)

			r += 1
		}
	}

	@Before
	def setUp() : Unit = {

		E10_WriteCSVDataIntoTheDatabase.setUp()
		
		val row1 = Map("id_student" -> 1, "first_name" -> "John", "last_name" -> "Smith").asJava
		val row2 = Map("id_student" -> 2, "first_name" -> "Marcus", "last_name" -> "Wrigth").asJava
		val row3 = Map("id_student" -> 3, "first_name" -> "Lawrence", "last_name" -> "Pyzc").asJava
		
		// HSQL does not support stored procedures, so mock it
		when(dts.call("your_procedure_name", oneParam:_*)).thenReturn(List(row1,row2,row3).asJava, null)

	}

	@After
	def tearDown() : Unit = {
		E10_WriteCSVDataIntoTheDatabase.tearDown()
	}

}