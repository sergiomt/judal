package org.judal.examples.scala.jdbc

import org.junit.Test

import java.util.Date
import java.util.Calendar

import org.judal.examples.scala.model.Student
import org.judal.examples.scala.model.StudentCourse
import org.judal.examples.scala.model.StudentPerCourse

import org.judal.storage.Param;
import org.judal.storage.EngineFactory
import org.judal.storage.table.IndexableView
import org.judal.storage.relational.RelationalDataSource

import org.judal.jdbc.RDBMS
import org.judal.jdbc.metadata.SQLViewDef
import org.judal.jdbc.JDBCRelationalDataSource
import org.judal.jdbc.JDBCMetadataObjectFactory

import org.judal.metadata.JoinType.INNER
import org.judal.metadata.NameAlias.AS
import org.judal.metadata.ColumnDef
import org.judal.metadata.ViewDef

import org.judal.Using._

import org.judal.storage.Pair.P$
import com.knowgate.dateutils.GregorianCalendarLocalisation.addMonths

import scala.collection.JavaConverters._

/**
 * Example of how to create an inner join and query it
 * and how to perform a query on an already existing view
 * with the same join
 */
class E20_InnerJoinRelationalQuery {

	@Test
	def demo() : Unit = {

		E20_InnerJoinRelationalQuery.setUp()

		val s = new Student()
		val c = new StudentCourse()

		val dts = EngineFactory.getDefaultRelationalDataSource()
		var v : IndexableView = null
		
		// Create the inner join on the fly to query students from course 7
		
		using (v) {
		  v = dts.openJoinView(INNER, s, AS(s,"s"), AS(c,"c"), P$("id_student","id_student"))
			val s80 : Iterable[Student] = v.fetch(s.fetchGroup, "c.id_course", new Integer(7)).asScala
		  for(t <- s80) {
				val id = t.getId
				val firstName = t.getFirstName
				val lastName = t.getLastName
				val dob = t.getDateOfBirth
		  }
		}


		// Another way
		// Create a view in the database and query it for courses created less than a month ago
		
		E20_InnerJoinRelationalQuery.createStudentCourseView(dts.asInstanceOf[JDBCRelationalDataSource])
		
		val spc = new StudentPerCourse()
		
		using (v) {
		  v = dts.openIndexedView(spc)
			val sc80 : Iterable[StudentPerCourse] = v.fetch(
			    spc.fetchGroup(), Integer.MAX_VALUE, 0,
					new Param("dt_created",1, addMonths(-1, new Date()))).asScala
			for (t <- sc80) {
				val lastName = t.getLastName()
				val dob = t.getDateOfBirth()
				val courseCode = t.getCourseCode()
				val coursePrice = t.getPrice()
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
		EngineFactory.getDefaultRelationalDataSource.asInstanceOf[JDBCRelationalDataSource].execute("DROP VIEW student_per_course")
		E10_WriteCSVDataIntoTheDatabase.tearDown()
	}

	def createStudentCourseView(dts: JDBCRelationalDataSource ) =  {
		val createViewSql = "CREATE VIEW student_per_course AS SELECT s.id_student,s.first_name,s.last_name,s.date_of_birth,c.id_course,c.code,c.nm_course,c.price FROM student s INNER JOIN student_x_course x ON s.id_student=x.id_student INNER JOIN course c ON x.id_course=c.id_course"
		val vdef : ViewDef = JDBCMetadataObjectFactory.newViewDef(RDBMS.valueOf(dts.getRdbmsId()), "student_per_course", createViewSql);
		for (cdef <- new StudentPerCourse().getTableDef.getColumns)
			vdef.addColumnMetadata(cdef);
		dts.getMetaData().addView(vdef);
		dts.execute(createViewSql);
	}
	
}
