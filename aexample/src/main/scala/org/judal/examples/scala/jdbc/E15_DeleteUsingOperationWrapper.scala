package org.judal.examples.scala.jdbc

import org.junit.Test

import org.scalatest.Suite

import org.judal.Using._

import org.judal.storage.scala.IndexableTableOperation

import org.judal.examples.scala.model.Student

/**
 * Delete a student given his internal primary key
 */
class E15_DeleteUsingOperationWrapper extends Suite {

	@Test def demo() = {
		
		E15_DeleteUsingOperationWrapper.setUp

		var op : IndexableTableOperation[Student] = null
		
		using (op) {
			op = new IndexableTableOperation(new Student)
		  op.delete(7)
		}

		E15_DeleteUsingOperationWrapper.tearDown
	}

}

object E15_DeleteUsingOperationWrapper {

	def setUp() = E10_WriteCSVDataIntoTheDatabase.setUp

	def tearDown() = E10_WriteCSVDataIntoTheDatabase.tearDown
  
}
