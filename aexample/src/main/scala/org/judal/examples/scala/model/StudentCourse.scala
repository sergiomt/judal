package org.judal.examples.scala.model

import org.judal.storage.EngineFactory
import org.judal.storage.scala.MapRecord
import org.judal.storage.relational.RelationalDataSource

class StudentCourse(dataSource: RelationalDataSource) extends MapRecord(dataSource, StudentCourse.TABLE_NAME) {
	
	def this() = this(EngineFactory.getDefaultRelationalDataSource)

	def getCourseId() : Int = getInt("id_course")

	def setCourseId(id: Int) = put("id_course", id)

	def getStudentId() = getInt("id_student")

	def setStudentId(id: Int) = put("id_student", id)
	
}


object StudentCourse {
  val TABLE_NAME = "student_x_course"
}
