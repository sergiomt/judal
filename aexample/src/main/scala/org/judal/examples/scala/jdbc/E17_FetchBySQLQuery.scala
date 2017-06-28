package org.judal.examples.scala.jdbc

import org.junit.Test

import java.util.Calendar

import org.judal.storage.EngineFactory
import org.judal.storage.table.Record
import org.judal.storage.relational.RelationalView

import org.judal.storage.scala.RelationalQuery
import org.judal.storage.table.RecordSet

import org.judal.examples.scala.model.Student

import org.judal.Using._

import scala.collection.JavaConverters._

/**
 * Use a SQL WHERE clause as filter of a relational query
 */
class E17_FetchBySQLQuery {

	@Test
	def demo() = {
		
		E17_FetchBySQLQuery.setUp()
		
		// ---------------------------------------------------------------

		var qry : RelationalQuery[Student] = null
		// Use implicitly the default RelationalDataSource for the thread.
		using (qry) {
			qry = new RelationalQuery[Student](classOf[Student])
			
			// List the columns as in SELECT clause
			qry.setResult("id_student,first_name,last_name,date_of_birth")
			
			// Set the filter as it'd be written in a SQL WHERE clause
			qry.setFilter("last_name='Kol'")
			
			// Set ordering as in SQL ORDER BY clause
			qry.setOrdering("date_of_birth")

			for (s <- qry.fetch) {
				val id = s.getId
				val firstName = s.getFirstName
				val lastName = s.getLastName
				val dob = s.getDateOfBirth
			}
		}

		
		// ---------------------------------------------------------------
		// Another way to do the same

		val dts = EngineFactory.getDefaultRelationalDataSource()
	  var viw : RelationalView = null
		using (viw) {
		  viw = dts.openRelationalView(new Student)
			val aqr = viw.newQuery
			aqr.setResult("*")
			aqr.declareParameters("last_name") // must be called before setFilter()
			aqr.setFilter("last_name=?")
			aqr.setOrdering("date_of_birth")
			val kols : Iterable[_ <: Record] = aqr.execute("Kol").asScala
			for (r <- kols) {
				val s = r.asInstanceOf[Student];
				val id = s.getId
				val firstName = s.getFirstName
				val lastName = s.getLastName
				val dob = s.getDateOfBirth
			}			
		}

		// ---------------------------------------------------------------
		
		E17_FetchBySQLQuery.tearDown()
	}

}


object E17_FetchBySQLQuery {

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
