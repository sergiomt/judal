package org.judal.examples.scala.model

import javax.jdo.JDOException

import org.judal.storage.EngineFactory
import org.judal.storage.scala.MapRecord
import org.judal.storage.relational.RelationalDataSource

class StudentDocument(dataSource: RelationalDataSource) extends MapRecord(dataSource, StudentDocument.TABLE_NAME) {

	def this() = this(EngineFactory.getDefaultRelationalDataSource())

}

object StudentDocument {
  val TABLE_NAME = "student_document"
}