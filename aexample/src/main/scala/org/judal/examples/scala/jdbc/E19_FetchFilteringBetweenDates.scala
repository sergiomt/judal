package org.judal.examples.scala.jdbc

import java.util.GregorianCalendar

import org.junit.Test

import org.judal.examples.scala.model.Student
import org.judal.storage.scala.RelationalQuery
import org.judal.storage.table.RecordSet

import org.judal.storage.query.Operator.BETWEEN

import org.judal.Using._

/**
 * Fetch students whose date of birth is between 01/01/1980 and 31/12/1989
 */
class E19_FetchFilteringBetweenDates {

	@Test
	def demo() = {
		
		E19_FetchFilteringBetweenDates.setUp()
		
		var qry : RelationalQuery[Student] = null
		
		using (qry) {
			  // Fetch students born between 01/01/1980 and 31/12/1989 ordering results from youngest to oldest
		    qry = new RelationalQuery[Student](classOf[Student])
			  qry.setResult(new Student().fetchGroup.getMembers.asInstanceOf[java.lang.Iterable[String]]);
			  qry.and("date_of_birth", BETWEEN, new GregorianCalendar(1980,0,1), new GregorianCalendar(1989,11,31));
			  qry.setOrdering("date_of_birth DESC")
			  for (s <- qry.fetch) {
				  val id = s.getId
				  val firstName = s.getFirstName
				  val lastName = s.getLastName
			}
		}

		E19_FetchFilteringBetweenDates.tearDown()
	}

}


object E19_FetchFilteringBetweenDates {

  def setUp() = {
		E10_WriteCSVDataIntoTheDatabase.setUp();
		E10_WriteCSVDataIntoTheDatabase.insertCoursesIntoDatabase();
		E10_WriteCSVDataIntoTheDatabase.insertStudentsIntoDatabase();
		E10_WriteCSVDataIntoTheDatabase.assignStudentsToCoursesIntoDatabase();
	}

	def tearDown() = {
		E10_WriteCSVDataIntoTheDatabase.tearDown();
	}

}
