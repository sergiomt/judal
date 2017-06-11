package org.judal.examples.scala.jdbc

import org.junit.Test

import java.sql.Date
import java.util.Calendar

import org.judal.storage.table.RecordSet
import org.judal.storage.scala.RelationalQuery
import org.judal.storage.table.ColumnGroup

import org.judal.storage.query.Operator.LIKE
import org.judal.storage.query.Operator.GT

import org.judal.examples.scala.model.Student

import org.judal.Using._

/**
 * Fetch records using a query builder
 */
class E18_FetchByRelationalQuery {

	@Test
	def demo() = {
		
		E18_FetchByRelationalQuery.setUp()
		var qry : RelationalQuery[Student] = null
		using (qry) {
		  qry = new RelationalQuery[Student](classOf[Student])
			qry.and("last_name", LIKE, "S%").and("date_of_birth", GT, new Date(80, 0, 1))
			qry.setResult(new ColumnGroup("id_student","first_name","last_name","date_of_birth"))
			for (s <- qry.fetch) {
				val id = s.getId
				val firstName = s.getFirstName
				val lastName = s.getLastName
				val dob = s.getDateOfBirth
			}
		}

		E18_FetchByRelationalQuery.tearDown()
	}

}


object E18_FetchByRelationalQuery {

	def setUp() = {
		E10_WriteCSVDataIntoTheDatabase.setUp()
		E10_WriteCSVDataIntoTheDatabase.insertCoursesIntoDatabase()
		E10_WriteCSVDataIntoTheDatabase.insertStudentsIntoDatabase()
		E10_WriteCSVDataIntoTheDatabase.assignStudentsToCoursesIntoDatabase()
	}

	def tearDown() = {
		E10_WriteCSVDataIntoTheDatabase.tearDown()
	}

}