package org.judal.examples.java.jdbc;

import org.junit.Test;
import org.judal.examples.java.model.Student;
import org.judal.storage.java.IndexableTableOperation;

/**
 * Delete a student given his internal primary key
 */
public class E15_DeleteUsingOperationWrapper {

	@Test
	public void demo() throws Exception {
		
		setUp();

		try (IndexableTableOperation<Student> op = new IndexableTableOperation<>(new Student())) {
			op.delete(new Integer(7));
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
