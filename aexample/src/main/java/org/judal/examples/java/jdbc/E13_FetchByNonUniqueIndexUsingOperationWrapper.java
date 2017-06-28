package org.judal.examples.java.jdbc;

import org.junit.Test;
import org.judal.examples.java.model.map.Student;
import org.judal.storage.java.TableOperation;
import org.judal.storage.table.RecordSet;

/**
 * Use TableOperation wrapper to fetch students whose last name is "Kol"
 * then change the last name to "Col"
 * Data is read from the database using the DataSource provided by
 * EngineFactory.DefaultThreadDataSource.get()
 */
public class E13_FetchByNonUniqueIndexUsingOperationWrapper {

	@Test
	public void demo() throws Exception {
		
		setUp();

		Student s = new Student();
		
		try (TableOperation<Student> op = new TableOperation<>(s)) {
			// Fetch students whose last name is "Kol" returning the results sorted by first_name in ascending order
			RecordSet<Student> students = op.fetchAsc(s.fetchGroup(), "last_name", "Kol", "first_name");
			for (Student t : students) {
				// Change last name and update record in the database
				t.setLastName("Col");
				t.store();
			}
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
