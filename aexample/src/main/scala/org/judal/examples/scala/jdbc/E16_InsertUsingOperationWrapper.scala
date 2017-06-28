package org.judal.examples.scala.jdbc

import org.junit.Test
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue

import java.math.BigDecimal
import java.sql.Date
import java.sql.Types

import org.judal.examples.scala.model.Course
import org.judal.storage.Param
import org.judal.storage.scala.RelationalOperation
import org.judal.storage.scala.IndexableTableOperation

import org.judal.Using._

/**
 * Insert a new row that does not previously exist in the database
 */

class E16_InsertUsingOperationWrapper {

	@Test
	def demo() = {
		
		E16_InsertUsingOperationWrapper.setUp()

		var op : IndexableTableOperation[Course] = null
		using (op) {
		  op = new IndexableTableOperation[Course](new Course())
			// Insert new course
			// Faster than Course.store() because it won't perform an update attempt
			// before inserting and also useful when the user has permissions to write
			// but not to read the database table holding the data.

			val courseId = E16_InsertUsingOperationWrapper.freeCourseId()
			
			assertFalse (op.exists(courseId))
			op.insert(
					new Param("id_course", Types.INTEGER, 1, courseId),
					new Param("code", Types.VARCHAR, 2, "EX21"),
					new Param("dt_start", Types.DATE, 3, new Date(117, 5, 15)),
					new Param("dt_end", Types.DATE, 4, new Date(117, 6, 15)),
					new Param("price", Types.DECIMAL, 5, new BigDecimal("250")),
					new Param("nm_course", Types.VARCHAR, 6, "Extension 21") )
			
			assertTrue (op.exists(courseId))
		}

		E16_InsertUsingOperationWrapper.tearDown
	}
  
}


object E16_InsertUsingOperationWrapper {

  def freeCourseId() = {
		// This is not a safe way of generating ids, it is just for demo purposes
		var nxtId : Int = 0
		var op : RelationalOperation[Course] = null
	  using(op) {
		  op = new RelationalOperation[Course](new Course())
			nxtId = op.maxInt("id_course", null) + 1
		}
		nxtId
	}

	def setUp() = {
		E10_WriteCSVDataIntoTheDatabase.setUp()
		E10_WriteCSVDataIntoTheDatabase.insertCoursesIntoDatabase()

	}

	def tearDown() = {
		E10_WriteCSVDataIntoTheDatabase.tearDown()
	}
  
}