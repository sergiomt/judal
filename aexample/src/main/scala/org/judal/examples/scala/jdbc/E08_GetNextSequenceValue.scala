package org.judal.examples.scala.jdbc

import org.junit.Test

import org.scalatest.Suite

import org.junit.Assert.assertEquals

/**
 * Example of how to get the next value of a sequence
 */
class E08_GetNextSequenceValue extends Suite {

	@Test def demo() = {
		
		val dataSource = E02_CreateAdditionalDataSource.create()
		
		// First create the sequence with direct SQL
		dataSource.execute("CREATE SEQUENCE demo_sequence AS BIGINT START WITH 1 INCREMENT BY 1")
		
		val nextVal = dataSource.getSequence("demo_sequence").nextValue()
		
		assertEquals(1l, nextVal)

		dataSource.execute("DROP SEQUENCE demo_sequence")

		E02_CreateAdditionalDataSource.close(dataSource)
	}

}
