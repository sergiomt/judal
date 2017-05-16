package org.judal.examples.java.jdbc;

import org.junit.Test;
import org.judal.storage.Param;
import org.judal.storage.query.Expression;
import org.judal.storage.table.IndexableTableOperation;

import java.sql.Date;
import java.sql.Types;

import org.judal.examples.java.model.Course;

/**
 * Use an IndexableTableOperation to update a set of rows
 */
public class E14_UpdateUsingOperationWrapper {

	@Test
	@SuppressWarnings("deprecation")
	public void demo() throws Exception {
		
		setUp();

		try (IndexableTableOperation<Course> op = new IndexableTableOperation<>(new Course())) {
			// Increase by 20% the price of courses staring on 2017-05-15
			op.update(
					new Param[] { new Param("price", Types.DECIMAL, 1, new Expression("price*1.2"))},
					new Param[] { new Param("dt_start", Types.DATE, 2, new Date(117, 4, 15))});
		}

		tearDown();
	}

	public static void setUp() throws Exception {
		E10_WriteCSVDataIntoTheDatabase.setUp();
	}

	public static void tearDown() throws Exception {
		E10_WriteCSVDataIntoTheDatabase.tearDown();
	}
	
}
