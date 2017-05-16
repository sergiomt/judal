package org.judal.examples.java.jdbc;

import org.junit.Test;

import org.judal.jdbc.JDBCRelationalDataSource;

public class E03_ExecuteDDLOnRelationalDataSource {

	@Test
	public void demo() throws Exception {
		JDBCRelationalDataSource dataSource = E02_CreateAdditionalDataSource.create();
		
		dataSource.execute("CREATE TABLE test_table (column1 INTEGER, column2 VARCHAR(100))");
		
		dataSource.execute("DROP TABLE test_table");
		
		E02_CreateAdditionalDataSource.close(dataSource);
	}
	
}
