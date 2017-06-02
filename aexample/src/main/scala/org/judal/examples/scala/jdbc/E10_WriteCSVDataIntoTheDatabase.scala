package org.judal.examples.scala.jdbc

import java.math.BigDecimal

import org.junit.Test
import org.junit.Assert.assertEquals

import org.scalatest.Suite

import com.knowgate.io.IOUtils

import org.judal.Using._

import org.judal.storage.EngineFactory
import org.judal.storage.relational.RelationalView
import org.judal.storage.table.impl.SingleLongColumnRecord

import org.judal.jdbc.JDBCRelationalDataSource

import org.judal.examples.Resources
import org.judal.examples.scala.model.Course
import org.judal.examples.scala.model.Student
import org.judal.examples.scala.model.StudentCourse

import collection.JavaConverters._

/**
 * Insert data from a comma delimited file into the database
 * using Default Relational DataSource kept at StorageContext
 */
class E10_WriteCSVDataIntoTheDatabase extends Suite {
	
	@Test def demo() = {
		
		E10_WriteCSVDataIntoTheDatabase.setUp()
		
		E10_WriteCSVDataIntoTheDatabase.insertStudentsIntoDatabase()
		
		E10_WriteCSVDataIntoTheDatabase.insertCoursesIntoDatabase()
		
		E10_WriteCSVDataIntoTheDatabase.assignStudentsToCoursesIntoDatabase()

		E10_WriteCSVDataIntoTheDatabase.tearDown()
	}

}

object E10_WriteCSVDataIntoTheDatabase {

	def insertStudentsIntoDatabase() = {
		val lines = IOUtils.readLines(Resources.getResourceAsStream("students.csv")).asScala

		// Write new students into the database
		for (line <- lines) {
			val fields = line.split(";")
			val s = new Student()
			s.setId(fields(0).toInt)
			s.setFirstName(fields(1))
			s.setLastName(fields(2))
			s.setDateOfBirth(fields(3))
			// This call use EngineFactory.DefaultThreadDataSource under the hood
			s.store()
		}

		assertEquals(lines.size, countRows(Student.TABLE_NAME))
	}

	def insertCoursesIntoDatabase() = {
		val lines = IOUtils.readLines(Resources.getResourceAsStream("courses.csv")).asScala

		// Write new students into the database
		for (line <- lines) {
			val fields = line.split(";")
			val c = new Course()
			c.setId(fields(0).toInt)
			c.setCode(fields(1))
			c.setCourseName(fields(2))
			c.setStartDate(fields(3))
			c.setEndDate(fields(4))
			c.setPrice(new BigDecimal(fields(5)))
			c.setDescription(fields(6))
			// This call use EngineFactory.DefaultThreadDataSource under the hood
			c.store()
		}
		
		assertEquals(lines.size, countRows(Course.TABLE_NAME))
	}
	
	def assignStudentsToCoursesIntoDatabase() = {
		val lines = IOUtils.readLines(Resources.getResourceAsStream("studentcourse.csv")).asScala

		// Write new students into the database
		for (line <- lines) {
			val fields = line.split(",")
			val sc = new StudentCourse()
			sc.setStudentId(fields(0).toInt)
			sc.setCourseId(fields(1).toInt)
			// This call use EngineFactory.DefaultThreadDataSource under the hood
			sc.store()
		}		
		assertEquals(lines.size, countRows(StudentCourse.TABLE_NAME))
	}

	def countRows(TableName: String) = {
		val count = new SingleLongColumnRecord(TableName, "c")
		val dts = EngineFactory.getDefaultRelationalDataSource
		var courses : RelationalView = null
		var retval : Int = 0
		using (courses) {
		  courses = dts.openRelationalView(count)
			retval = courses.count(null).intValue
		}
		retval
	}
	
	def setUp() = {
		val dataSource = E01_CreateDefaultRelationalDataSource.create
		E04_CreateTablesFromJDOXML.createSchemaObjects(dataSource)
		dataSource.execute("CREATE SEQUENCE seq_student AS BIGINT START WITH 1 INCREMENT BY 1")
		dataSource.execute("CREATE SEQUENCE seq_course AS BIGINT START WITH 1 INCREMENT BY 1")
		dataSource.asInstanceOf[JDBCRelationalDataSource]
	}

	def tearDown() = {
		E04_CreateTablesFromJDOXML.dropSchemaObjects(EngineFactory.getDefaultRelationalDataSource)
		E01_CreateDefaultRelationalDataSource.close
	}
  
}