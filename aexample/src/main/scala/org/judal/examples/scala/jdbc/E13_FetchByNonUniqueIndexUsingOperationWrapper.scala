package org.judal.examples.scala.jdbc

import org.junit.Test

import org.judal.Using._

import org.scalatest.Suite

import org.judal.storage.scala.TableOperation

import org.judal.examples.scala.model.Student

/**
 * Use TableOperation wrapper to fetch students whose last name is "Kol"
 * then change the last name to "Col"
 * Data is read from the database using the DataSource provided by
 * EngineFactory.DefaultThreadDataSource.get()
 */
class E13_FetchByNonUniqueIndexUsingOperationWrapper extends Suite {

	@Test def demo() = {
		
		E13_FetchByNonUniqueIndexUsingOperationWrapper.setUp

		val s = new Student()
		
		var op : TableOperation[Student] = null
		using (op) {
		  op = new TableOperation(s)
			// Fetch students whose last name is "Kol" returning the results sorted by first_name in ascending order
			val students : Iterable[Student] = op.fetchAsc(s.fetchGroup, "last_name", "Kol", "first_name")
			for (t <- students) {
				// Change last name and update record in the database
				t.setLastName("Col")
				t.store()
			}
		}

		E13_FetchByNonUniqueIndexUsingOperationWrapper.tearDown
	}
	
}

object E13_FetchByNonUniqueIndexUsingOperationWrapper {

	def setUp() = {
		E10_WriteCSVDataIntoTheDatabase.setUp
	}

	def tearDown() = {
		E10_WriteCSVDataIntoTheDatabase.tearDown
	}
}
