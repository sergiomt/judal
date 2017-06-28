package org.judal.examples.scala.jdbc

import org.junit.Test

import java.util.Calendar

import org.judal.examples.scala.model.Student
import org.judal.examples.scala.model.StudentCourse

import org.judal.storage.EngineFactory
import org.judal.storage.table.IndexableView
import org.judal.storage.relational.RelationalDataSource

import org.judal.metadata.JoinType.INNER
import org.judal.metadata.NameAlias.AS

import com.knowgate.tuples.Pair.P$

import org.judal.Using._

import scala.collection.JavaConverters._

class E20_InnerJoinRelationalQuery {

	@Test
	def demo() : Unit = {

		E20_InnerJoinRelationalQuery.setUp()

		val s = new Student()
		val c = new StudentCourse()

		val dts = EngineFactory.getDefaultRelationalDataSource()
		var v : IndexableView = null
		using (v) {
		  v = dts.openJoinView(INNER, s, AS(s,"s"), AS(c,"c"), P$("id_student","id_student"))
			val s80 : Iterable[Student] = v.fetch(s.fetchGroup, c.getTableName+".id_course", new Integer(7)).asScala
		  for(t <- s80) {
				val id = t.getId
				val firstName = t.getFirstName
				val lastName = t.getLastName
				val dob = t.getDateOfBirth
		  }
		}

		E20_InnerJoinRelationalQuery.tearDown()
	}

}


object E20_InnerJoinRelationalQuery {
  
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
