package org.judal.examples.scala.jdbc

import org.junit.Test

import org.judal.Using._

import org.scalatest.Suite

import org.judal.storage.Param
import org.judal.storage.query.Expression
import org.judal.storage.scala.IndexableTableOperation

import java.sql.Date
import java.sql.Types

import org.judal.examples.scala.model.Course

/**
 * Use an IndexableTableOperation to update a set of rows
 */
class E14_UpdateUsingOperationWrapper extends Suite {

	@Test def demo() = {
		
		E14_UpdateUsingOperationWrapper.setUp

		var op : IndexableTableOperation[Course] = null
		
		using(op) {
		  op = new IndexableTableOperation(new Course)

		  // Increase by 20% the price of courses staring on 2017-05-15
			op.update(
					Array[Param] ( new Param("price", Types.DECIMAL, 1, new Expression("price*1.2")) ),
					Array[Param] ( new Param("dt_start", Types.DATE, 2, new Date(117, 4, 15)) ) )
		}

		E14_UpdateUsingOperationWrapper.tearDown
	}
	
}

object E14_UpdateUsingOperationWrapper {

	def setUp() = E10_WriteCSVDataIntoTheDatabase.setUp

	def tearDown() = E10_WriteCSVDataIntoTheDatabase.tearDown
  
}