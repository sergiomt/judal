package org.judal.examples.scala.jdbc

import org.junit.Test
import org.junit.Assert.assertTrue

import org.scalatest.Suite

import org.judal.examples.scala.model.Student

/**
 * Load a Student from the database using his primary key to find him
 * Connect using the default Thread DataSource as provided by
 * EngineFactory.DefaultThreadDataSource.get()
 */
class E11_LoadRecordByPrimaryKeyFromDefaultDataSourceModifyAndStore extends Suite {

	@Test def demo() = {

		E11_LoadRecordByPrimaryKeyFromDefaultDataSourceModifyAndStore.setUp
		
		val s = new Student
		
		assertTrue (s.load(1))
		
		s.setDateOfBirth("1971-01-01")

		s.store()

		E11_LoadRecordByPrimaryKeyFromDefaultDataSourceModifyAndStore.tearDown
	}
	
}

object E11_LoadRecordByPrimaryKeyFromDefaultDataSourceModifyAndStore {

	def setUp() = {
		E10_WriteCSVDataIntoTheDatabase.setUp
		E10_WriteCSVDataIntoTheDatabase.insertStudentsIntoDatabase
	}

	def tearDown() = {
		E10_WriteCSVDataIntoTheDatabase.tearDown
	}
  
}