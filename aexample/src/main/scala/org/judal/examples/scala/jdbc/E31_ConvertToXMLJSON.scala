package org.judal.examples.scala.jdbc

import org.junit.Test

import java.text.SimpleDateFormat
import java.util.Date

import org.judal.storage.EngineFactory
import org.judal.storage.table.RecordSet
import org.judal.storage.table.IndexableView

import org.judal.examples.scala.model.Course

import org.judal.Using._

import scala.collection.JavaConverters._

/**
 * Example of how to print a RecordSet as XML o JSON
 */
class E31_ConvertToXMLJSON {

	@Test
	def demo() : Unit = {
		
		E31_ConvertToXMLJSON.setUp()

		val c = new Course()
		
		c.load(1)

		val attribs = Map("timestamp" -> new Date().toString).asJava		
		val dateFormat = new SimpleDateFormat("yyyy-MM-DD")
		
		val courseAsXML = c.toXML("  ", attribs, dateFormat, null, null)

		var viw : IndexableView = null
		using (viw) {
		  viw = EngineFactory.getDefaultRelationalDataSource.openIndexedView(c)
						
			val courses : RecordSet[Course] = viw.fetch(c.fetchGroup, "id_course", new Integer(1), new Integer(4))
			
			val coursesAsXML = courses.toXML("  ", dateFormat, null, null)

			val coursesAsJSON = courses.toJSON
		}
		
		E31_ConvertToXMLJSON.tearDown()
	}
		
}


object E31_ConvertToXMLJSON {
  
	def setUp() : Unit = {
		E10_WriteCSVDataIntoTheDatabase.setUp()
		E10_WriteCSVDataIntoTheDatabase.insertCoursesIntoDatabase()
	}

	def tearDown() : Unit = {
		E10_WriteCSVDataIntoTheDatabase.tearDown()
	}	

}