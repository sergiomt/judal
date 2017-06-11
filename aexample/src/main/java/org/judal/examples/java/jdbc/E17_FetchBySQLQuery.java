package org.judal.examples.java.jdbc;

import org.junit.Test;

import java.util.Calendar;

import org.judal.storage.java.RelationalQuery;
import org.judal.storage.table.RecordSet;

import org.judal.examples.java.model.Student;

/**
 * Use a SQL WHERE clause as filter of a relational query
 */
public class E17_FetchBySQLQuery {

	@SuppressWarnings("unused")
	@Test
	public void demo() throws Exception {
		
		setUp();

		// Use implicitly the default RelationalDataSource for the thread.
		try (RelationalQuery<Student> qry = new RelationalQuery<>(Student.class)) {
			
			// List the columns as in SELECT clause
			qry.setResult("id_student,first_name,last_name,date_of_birth");
			
			// Set the filter as it'd be written in a SQL WHERE clause
			qry.setFilter("last_name='Kol'");
			
			// Set ordering as in SQL ORDER BY clause
			qry.setOrdering("date_of_birth");
						
			RecordSet<Student> kols = qry.fetch();
			for (Student s : kols) {
				int id = s.getId();
				String firstName = s.getFirstName();
				String lastName = s.getLastName();
				Calendar dob = s.getDateOfBirth();
			}
		}

		tearDown();
	}

	public static void setUp() throws Exception {
		E10_WriteCSVDataIntoTheDatabase.setUp();
		E10_WriteCSVDataIntoTheDatabase.insertCoursesIntoDatabase();
		E10_WriteCSVDataIntoTheDatabase.insertStudentsIntoDatabase();
		E10_WriteCSVDataIntoTheDatabase.assignStudentsToCoursesIntoDatabase();
	}

	public static void tearDown() throws Exception {
		E10_WriteCSVDataIntoTheDatabase.tearDown();
	}
}
