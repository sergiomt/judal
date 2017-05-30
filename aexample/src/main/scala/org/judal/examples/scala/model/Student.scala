package org.judal.examples.scala.model

import java.text.ParseException
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.GregorianCalendar

import javax.jdo.JDOException

import org.judal.storage.DataSource
import org.judal.storage.EngineFactory
import org.judal.storage.scala.MapRecord
import org.judal.storage.relational.RelationalDataSource

/**
 * Extend MapRecord or ArrayRecord in order to create model classes manageable by JUDAL.
 * Add your getters and setters for database fields.
 */
class Student(dataSource: RelationalDataSource) extends MapRecord(dataSource, Student.TABLE_NAME) {
	
	def this() = this(EngineFactory.getDefaultRelationalDataSource)

	@throws(classOf[JDOException])
	override def store(dts: DataSource) {
		// Generate the student Id. from a sequence if it is not provided
		if (isNull("id_student"))
			setId (dts.getSequence("seq_student").nextValue().asInstanceOf[Int])
		super.store(dts)
	}
	
	def getId() : Int = getInt("id_student")

	def setId(id: Int) = put("id_student", id)

	def getFirstName() = getString("first_name")

	def setFirstName(firstName: String) = put("first_name", firstName)

	def getLastName() = getString("last_name")

	def setLastName(lastName: String) = put("last_name", lastName)

	def getDateOfBirth() = getCalendar("date_of_birth")

	def setDateOfBirth(dob: Calendar) = put("date_of_birth", dob)
	
	@throws(classOf[ParseException])
	def setDateOfBirth(yyyyMMdd: String) : Unit = {
		val dobFormat = new SimpleDateFormat("yyyy-MM-dd")
		val cal = new GregorianCalendar()
		cal.setTime(dobFormat.parse(yyyyMMdd))
		setDateOfBirth(cal)
	}

	def getPhoto() = getBytes("photo")

	def setPhoto(photoData: Array[Byte]) = put("photo", photoData)

}

object Student {
  
  val TABLE_NAME = "student"
}
