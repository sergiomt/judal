package org.judal.examples.java.jdbc;

import org.judal.jdbc.JDBCRelationalDataSource;
import org.junit.Test;

public class E08_GetNextSequenceValue {

	@Test
	public void demo() throws Exception {
		JDBCRelationalDataSource dataSource = E02_CreateAdditionalDataSource.create();
		
		dataSource.execute("CREATE SEQUENCE demo_sequence AS BIGINT START WITH 1 INCREMENT BY 1");
		
		long nextVal = dataSource.getSequence("demo_sequence").nextValue();
		
		dataSource.execute("DROP SEQUENCE demo_sequence");

		E02_CreateAdditionalDataSource.close(dataSource);
	}
	
}
