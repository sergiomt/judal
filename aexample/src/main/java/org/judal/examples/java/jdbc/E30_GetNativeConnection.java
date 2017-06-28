package org.judal.examples.java.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Test;

import org.judal.storage.EngineFactory;
import org.judal.jdbc.JDBCRelationalDataSource;

public class E30_GetNativeConnection {

	@Test
	public void demo() throws Exception {
		
		setUp();

		JDBCRelationalDataSource dts = (JDBCRelationalDataSource) EngineFactory.getDefaultRelationalDataSource();

		Connection conn = dts.getConnection();		
		Statement stmt = conn.createStatement();
		ResultSet rset = stmt.executeQuery("SELECT * FROM student");
		
		// ...

		rset.close();
		stmt.close();
		conn.close();

		tearDown();
	}

	public static void setUp() throws Exception {
		E10_WriteCSVDataIntoTheDatabase.setUp();
	}

	public static void tearDown() throws Exception {
		E10_WriteCSVDataIntoTheDatabase.tearDown();
	}	

}
