package org.judal.examples.java.jdbc;

import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;

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

		@SuppressWarnings("unchecked")
		List<Map<String,Object>> resultset = (List<Map<String,Object>>) dts.call("return_resultset", oneParam);
		assertEquals(2, resultset.size());

	}

	@Before
	public void setUp() {
		// HSQL does not support stored procedures, so mock it

		Map<String,Object> row1 = new HashMap<>();
		row1.put("column_name1", "column_value1");
		row1.put("column_name2", "column_value2");
		row1.put("column_name3", "column_value3");
		Map<String,Object> row2 = new HashMap<>();
		row1.put("column_name1", "column_value4");
		row1.put("column_name2", "column_value5");
		row1.put("column_name3", "column_value6");

		List<Map<String,Object>> procResultSetValue = Arrays.asList(row1, row2);
				
		when(dts.call("your_procedure_name", oneParam)).thenReturn(procReturnValue);		
		when(dts.call("return_resultset", oneParam)).thenReturn(procResultSetValue);		

	}
}
