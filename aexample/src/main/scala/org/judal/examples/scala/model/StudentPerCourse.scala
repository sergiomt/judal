package org.judal.examples.scala.model

import org.judal.storage.EngineFactory
import org.judal.storage.scala.MapRecord
import org.judal.storage.relational.RelationalDataSource

class StudentPerCourse(dataSource: RelationalDataSource) extends MapRecord(dataSource, StudentPerCourse.VIEW_NAME, StudentPerCourse.COLUMN_NAMES:_*) {

	def this() = this(EngineFactory.getDefaultRelationalDataSource)

	def getStudentId() = getInt("id_student")

	def getFirstName() = getString("first_name")

	def getLastName() = getString("last_name")

	def getDateOfBirth() = getCalendar("date_of_birth")

	def getCourseId() = getInt("id_course")
	
	def getCourseCode() = getString("code")
	
	def getCourseName() = getString("nm_course")

	def getPrice() = getDecimal("price")

}


object StudentPerCourse {

  val VIEW_NAME = "student_per_course"
	
  val COLUMN_NAMES = Array[String]("id_student","first_name","last_name","date_of_birth","id_course","code","nm_course","price")
}