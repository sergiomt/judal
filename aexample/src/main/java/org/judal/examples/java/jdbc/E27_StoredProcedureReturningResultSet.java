package org.judal.examples.java.jdbc;

import java.sql.Types;

import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.HashMap;

import org.junit.Test;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;

import org.judal.storage.Param;
import org.judal.examples.java.model.map.Student;
import org.judal.jdbc.JDBCRelationalDataSource;

/**
 * Example of how to call a stored procedure which returns a ResultSet
 */
@RunWith(MockitoJUnitRunner.class)
public class E27_StoredProcedureReturningResultSet {

	private JDBCRelationalDataSource dts = mock(JDBCRelationalDataSource.class);

	private final Param[] oneParam = new Param[]{new Param("column_name", Types.VARCHAR, 1, "Param Value")};
	
	@Test
	@SuppressWarnings("unchecked")
	public void demo() {
		
		// ResultSet is returned as List of Map
		List<Map<String,Object>> resultSet = (List<Map<String,Object>>) dts.call("your_procedure_name", oneParam);
		
		int r = 0;
		for (Map<String,Object> row : resultSet) {
			
			Student s = new Student();
			s.putAll(row);
			
			if (r==0)
				assertEquals(1, s.getId());

			if (r==1)
				assertEquals("Wrigth", s.getLastName());

			r++;
		}
	}

	@Before
	public void setUp() throws Exception {

		E10_WriteCSVDataIntoTheDatabase.setUp();
		
		final List<Map<String,Object>> resultSet = new LinkedList<Map<String,Object>>();

		HashMap<String,Object> row1 = new HashMap<>();
		row1.put("id_student", 1);
		row1.put("first_name", "John");
		row1.put("last_name", "Smith");

		HashMap<String,Object> row2 = new HashMap<>();
		row2.put("id_student", 2);
		row2.put("first_name", "Marcus");
		row2.put("last_name", "Wrigth");

		HashMap<String,Object> row3 = new HashMap<>();
		row3.put("id_student", 3);
		row3.put("first_name", "Lorence");
		row3.put("last_name", "Pyzc");
		
		resultSet.add(row1);
		resultSet.add(row2);
		resultSet.add(row3);
		
		// HSQL does not support stored procedures, so mock it
		when(dts.call("your_procedure_name", oneParam)).thenReturn(resultSet);

	}

	@After
	public void tearDown() throws Exception {
		E10_WriteCSVDataIntoTheDatabase.tearDown();
	}

}