package org.judal.examples.java.jdbc;

import java.sql.Types;

import org.junit.Test;
import org.junit.Before;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

import org.mockito.junit.MockitoJUnitRunner;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;

import org.judal.storage.Param;
import org.judal.jdbc.JDBCRelationalDataSource;

/**
 * Example of how to call a stored procedure or PostgreSQL function and get its return value
 */
@RunWith(MockitoJUnitRunner.class)
public class E25_CallStoredProcedure {

	private JDBCRelationalDataSource dts = mock(JDBCRelationalDataSource.class);

	private final Param[] oneParam = new Param[]{new Param("column_name", Types.VARCHAR, 1, "Param Value")};
	
	private final String procReturnValue = "Hello!";
	
	@Test
	public void demo() {
		
		Object retval = dts.call("your_procedure_name", oneParam);
		
		assertEquals(procReturnValue, retval);

	}

	@Before
	public void setUp() {
		
		// HSQL does not support stored procedures, so mock it
		when(dts.call("your_procedure_name", oneParam)).thenReturn(procReturnValue);		

	}
}
