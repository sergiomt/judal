package org.judal.examples.scala.jdbc

import java.math.BigDecimal

import javax.transaction.Status
import javax.transaction.TransactionManager

import org.junit.Test
import org.junit.Assert.assertTrue
import org.junit.Assert.assertFalse
import org.junit.Assert.assertEquals

import org.judal.storage.Param
import org.judal.storage.EngineFactory
import org.judal.storage.table.IndexableView

import org.judal.examples.scala.model.Course
import org.judal.examples.scala.model.Student
import org.judal.jdbc.JDBCRelationalDataSource

import org.judal.Using._

/**
 * Example of how to commit or rollback a transaction
 */
class E32_Transaction1 {

	@Test
	def demo() : Unit =  {
		
		E32_Transaction1.setUp()

		val dts = EngineFactory.getDefaultRelationalDataSource.asInstanceOf[JDBCRelationalDataSource]
		dts.setDefaultAutoCommit(false)

		val txm = dts.getTransactionManager
		
		val c = new Course(dts)
		c.setId(100)
		c.setCode("CU01")
		c.setCourseName("Applied electromechanics")
		c.setPrice(new BigDecimal("2134"))
		c.setStartDate("2017-08-09")
		c.setStartDate("2017-10-11")

		val s = new Student(dts)
		s.setId(100)
		s.setFirstName("Jhon")
		s.setLastName("McMillan")
		s.setDateOfBirth("1971-02-03")

		txm.begin()
		
		assertEquals(Status.STATUS_ACTIVE, txm.getStatus())
		
		c.store(dts)				
		s.store(dts)
		
		txm.rollback()
		
		var v : IndexableView = null;
		using(v) {
		  v = dts.openIndexedView(c)
			assertFalse(v.exists(Seq(new Param("code", 1, "CU01")):_*))
		}
		using(v) {
		  v = dts.openIndexedView(s)
			assertFalse(v.exists(Seq(new Param("last_name", 1, "McMillan")):_*))			
		}
		
		txm.begin()

		assertEquals(Status.STATUS_ACTIVE, txm.getStatus())
		
		c.store(dts)				
		s.store(dts)
		
		txm.commit()
		using(v) {
		  v = dts.openIndexedView(c)
			assertTrue(v.exists(Seq(new Param("code", 1, "CU01")):_*))
		}
		using (v) {
		  v = dts.openIndexedView(s)
			assertTrue(v.exists(Seq(new Param("last_name", 1, "McMillan")):_*))
		}

		E32_Transaction1.tearDown()
	}

}


object E32_Transaction1 {

  def setUp() : Unit = {
		E10_WriteCSVDataIntoTheDatabase.setUp()
		E10_WriteCSVDataIntoTheDatabase.insertCoursesIntoDatabase()
		E10_WriteCSVDataIntoTheDatabase.insertStudentsIntoDatabase()
	}

	def tearDown() : Unit = {
		E10_WriteCSVDataIntoTheDatabase.tearDown()
	}

}