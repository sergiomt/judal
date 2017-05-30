package org.judal.examples.java.jdbc;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

import org.judal.jdbc.JDBCRelationalDataSource;

/**
 * Example of how to get the next value of a sequence
 */
public class E08_GetNextSequenceValue {

	@Test
	public void demo() throws Exception {
		
		JDBCRelationalDataSource dataSource = E02_CreateAdditionalDataSource.create();
		
		// First create the sequence with direct SQL
		dataSource.execute("CREATE SEQUENCE demo_sequence AS BIGINT START WITH 1 INCREMENT BY 1");
		
		long nextVal = dataSource.getSequence("demo_sequence").nextValue();
		
		assertEquals(1l, nextVal);

		dataSource.execute("DROP SEQUENCE demo_sequence");

		E02_CreateAdditionalDataSource.close(dataSource);
	}
	
}
