package org.judal.examples.scala.jdbc

import org.junit.Test

import java.util.Iterator

import org.judal.examples.java.model.map.Student
import org.judal.storage.EngineFactory
import org.judal.storage.relational.RelationalDataSource
import org.judal.storage.table.View

import org.judal.Using._

class E29_IterateOpenResultSet {

	@Test
	def demo() : Unit = {
		
		E29_IterateOpenResultSet.setUp()

		val dts = EngineFactory.getDefaultRelationalDataSource
		var tbl : View = null
		using (tbl) {
		  tbl = dts.openView(new Student())
			val cursor = tbl.iterator
			while (cursor.hasNext) {
				val s = cursor.next.asInstanceOf[Student]
			}
			tbl.close(cursor)
		}

		E29_IterateOpenResultSet.tearDown()
	}

}


object E29_IterateOpenResultSet {
  
	def setUp() : Unit = {
		E10_WriteCSVDataIntoTheDatabase.setUp()
		E10_WriteCSVDataIntoTheDatabase.insertCoursesIntoDatabase()
		E10_WriteCSVDataIntoTheDatabase.insertStudentsIntoDatabase()
		E10_WriteCSVDataIntoTheDatabase.assignStudentsToCoursesIntoDatabase()
	}

	def tearDown() : Unit = {
		E10_WriteCSVDataIntoTheDatabase.tearDown()
	}	

}