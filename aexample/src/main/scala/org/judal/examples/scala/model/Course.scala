package org.judal.examples.scala.model

import java.math.BigDecimal

import java.text.ParseException
import java.text.SimpleDateFormat

import java.util.Calendar
import java.util.GregorianCalendar

import javax.jdo.JDOException

import org.judal.storage.EngineFactory
import org.judal.storage.scala.MapRecord
import org.judal.storage.relational.RelationalDataSource

class Course(dataSource: RelationalDataSource) extends MapRecord(dataSource, Course.TABLE_NAME) {

	def this() = this(EngineFactory.getDefaultRelationalDataSource)

	def getId() : Int = getInt("id_course")

	def setId(id: Int) = put("id_course", id)

	def getCode() = getString("code")

	def setCode(courseCode: String) = put("code", courseCode)

	def getCourseName() = getString("nm_course")

	def setCourseName(courseName: String) = put("nm_course", courseName)

	def getStartDate() = getCalendar("dt_start")

	def setStartDate(dtStart: Calendar) = put("dt_start", dtStart)
	
	@throws(classOf[ParseException])
	def setStartDate(yyyyMMdd:String) : Unit = {
		val dtFormat = new SimpleDateFormat("yyyy-MM-dd")
		val cal = new GregorianCalendar()
		cal.setTime(dtFormat.parse(yyyyMMdd))
		setStartDate(cal)
	}

	def getEndDate() = getCalendar("dt_end")

	def setEndDate(dtEnd: Calendar) = put("dt_end", dtEnd)
	
	@throws(classOf[ParseException])
	def setEndDate(yyyyMMdd: String) : Unit = {
		val dtFormat = new SimpleDateFormat("yyyy-MM-dd")
		val cal = new GregorianCalendar()
		cal.setTime(dtFormat.parse(yyyyMMdd))
		setEndDate(cal)
	}

	def getPrice() = getDecimal("price")

	def setPrice(price: BigDecimal) = put("price", price)
	
	def getDescription() = getString("description")

	def setDescription(description: String) =  put("description", description)
	
}


object Course {
  
  val TABLE_NAME = "course"
  
}