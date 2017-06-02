package org.judal.examples.java.jdbc;

import org.junit.Test;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Types;

import org.judal.examples.java.model.Course;
import org.judal.storage.Param;
import org.judal.storage.relational.RelationalOperation;
import org.judal.storage.table.IndexableTableOperation;

/**
 * Insert a new row that does not previously exist in the database
 */
public class E16_InsertUsingOperationWrapper {

	@Test
	@SuppressWarnings("deprecation")
	public void demo() throws Exception {
		
		setUp();

		try (IndexableTableOperation<Course> op = new IndexableTableOperation<>(new Course())) {
			// Insert new course
			// Faster than Course.store() because it won't perform an update attempt
			// before inserting and also useful when the user has permissions to write
			// but not to read the database table holding the data.

			final Integer courseId = freeCourseId();
			
			assertFalse (op.exists(courseId));

			op.insert(new Param[] {					
					new Param("id_course", Types.INTEGER, 1, courseId),
					new Param("code", Types.VARCHAR, 2, "EX21"),
					new Param("dt_start", Types.DATE, 3, new Date(117, 5, 15)),
					new Param("dt_end", Types.DATE, 4, new Date(117, 6, 15)),
					new Param("price", Types.DECIMAL, 5, new BigDecimal("250")),
					new Param("nm_course", Types.VARCHAR, 6, "Extension 21")
			});
			
			assertTrue (op.exists(courseId));
		}

		tearDown();
	}

	public static Integer freeCourseId() throws Exception {
		// This is not a safe way of generating ids, it is just for demo purposes
		try (RelationalOperation<Course> op = new RelationalOperation<>(new Course())) {
			return op.maxInt("id_course", null) + 1;
		}
	}

	public static void setUp() throws Exception {
		E10_WriteCSVDataIntoTheDatabase.setUp();
		E10_WriteCSVDataIntoTheDatabase.insertCoursesIntoDatabase();

	}

	public static void tearDown() throws Exception {
		E10_WriteCSVDataIntoTheDatabase.tearDown();
	}

}
