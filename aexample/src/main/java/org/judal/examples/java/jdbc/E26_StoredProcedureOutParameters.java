package org.judal.examples.java.jdbc;

import java.sql.Types;

import org.junit.Test;
import org.junit.Before;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;

import org.judal.storage.Param;
import org.judal.storage.Param.Direction;
import org.judal.jdbc.JDBCRelationalDataSource;

/**
 * Example of how to call a stored procedure with output parameters
 */
@RunWith(MockitoJUnitRunner.class)
public class E26_StoredProcedureOutParameters {

	private JDBCRelationalDataSource dts = mock(JDBCRelationalDataSource.class);

	private final String paramValue = "Hello!";

	// One input and one output parameter
	private final Param[] twoParams = new Param[] {
			new Param("column1_name", Types.VARCHAR, 1, Direction.IN, paramValue),
			new Param("column2_name", Types.VARCHAR, 2, Direction.OUT) };
	
	@Test
	public void demo() {

		Object retval = dts.call("your_procedure_name", twoParams);

		assertEquals(paramValue.length(), retval);
		assertEquals(paramValue, twoParams[1].getValue());

	}

	@Before
	public void  setUp() {

		// HSQL does not support stored procedures, so mock it
		when(dts.call("your_procedure_name", twoParams)).thenAnswer(new Answer<Integer>() {
			public Integer answer(InvocationOnMock invocation) throws Throwable {
				Param in = invocation.getArgument(1);
				((Param) invocation.getArgument(2)).setValue(in.getValue());
				return paramValue.length();
			}
		});
		
	}
}
