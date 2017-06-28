package org.judal.examples.scala.jdbc

import org.junit.Test

import org.judal.metadata.NameAlias

import org.judal.storage.table.RecordSet
import org.judal.storage.scala.RelationalQuery
import org.judal.storage.query.Expression
import org.judal.storage.query.Predicate

import org.judal.storage.query.Operator.EQ
import org.judal.storage.query.Operator.EXISTS

import org.judal.examples.scala.model.Student
import org.judal.examples.scala.model.StudentCourse

import org.judal.Using._

class E22_ExistsSubqueryUsingPredicates {

	@Test
	def demo() : Unit = {

		E22_ExistsSubqueryUsingPredicates.setUp()

		//  SELECT * FROM student s WHERE EXISTS (SELECT id_student FROM student_x_course x WHERE s.id_student = x.id_student)

		val STUDENT_X_COURSE = new NameAlias(StudentCourse.TABLE_NAME, "x")
		
		var qry : RelationalQuery[Student] = null
		
		using (qry) {
		  qry = new RelationalQuery[Student](classOf[Student], "s")
			qry.setResult("*")
			
			val sid = qry.newPredicate().and("s.id_student", EQ, new Expression("x.id_student"))
			qry.and("id_student", EXISTS, STUDENT_X_COURSE, sid)
			
			for (s <- qry.fetch) {
				val id = s.getId
				val firstName = s.getFirstName
				val lastName = s.getLastName
			}
		}

		E22_ExistsSubqueryUsingPredicates.tearDown()
	}

}

object E22_ExistsSubqueryUsingPredicates {
  
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