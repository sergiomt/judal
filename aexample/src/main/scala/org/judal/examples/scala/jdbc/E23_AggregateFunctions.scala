package org.judal.examples.scala.jdbc

import org.junit.Test

import org.judal.storage.EngineFactory
import org.judal.storage.query.Predicate
import org.judal.storage.relational.RelationalDataSource
import org.judal.storage.relational.RelationalView
import org.judal.storage.table.impl.SingleBigDecimalColumnRecord
import org.judal.storage.table.impl.SingleDateColumnRecord
import org.judal.storage.table.impl.SingleLongColumnRecord

import org.judal.storage.query.Operator.ISNOTNULL
import org.judal.storage.query.Operator.LIKE

import java.math.BigDecimal
import java.util.Date

import org.judal.examples.scala.model.Course
import org.judal.examples.scala.model.Student

import org.judal.Using._

class E23_AggregateFunctions {

	@Test
	def demo() : Unit = {
		
		E23_AggregateFunctions.setUp()
		val COLUMN_NAME = "a"
		val dts = EngineFactory.DefaultThreadDataSource.get().asInstanceOf[RelationalDataSource]
		
		val count = new SingleLongColumnRecord(Student.TABLE_NAME, COLUMN_NAME)
		var students : RelationalView = null
		using (students) {
		  students = dts.openRelationalView(count)
			val like_S = students.newPredicate().and("last_name", LIKE, "S%")
			val l = students.count(like_S)
		}

		val youngest = new SingleDateColumnRecord(Student.TABLE_NAME, COLUMN_NAME)
		using (students) {
      students = dts.openRelationalView(youngest)
      val like_S = students.newPredicate().and("last_name", LIKE, "S%")
			val d = students.max("date_of_birth", like_S).asInstanceOf[Date]
		}
		
		val sum = new SingleBigDecimalColumnRecord(Course.TABLE_NAME, COLUMN_NAME)
		var courses : RelationalView = null
		using(courses) {
		  courses = dts.openRelationalView(sum)
			val all = courses.newPredicate().and("price", ISNOTNULL)
			val total = courses.sum("price", all).asInstanceOf[BigDecimal]
		}
		
		E23_AggregateFunctions.tearDown()
	}

}


object E23_AggregateFunctions {
  
	def setUp() = {
		E10_WriteCSVDataIntoTheDatabase.setUp()
		E10_WriteCSVDataIntoTheDatabase.insertStudentsIntoDatabase()
		E10_WriteCSVDataIntoTheDatabase.insertCoursesIntoDatabase()
		E10_WriteCSVDataIntoTheDatabase.assignStudentsToCoursesIntoDatabase()
	}

	def tearDown() = {
		E10_WriteCSVDataIntoTheDatabase.tearDown()
	}

}