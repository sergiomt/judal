package org.judal.examples.java.jdbc;

import org.junit.Test;

import java.util.Calendar;

import org.judal.storage.EngineFactory;
import org.judal.storage.query.AbstractQuery;
import org.judal.storage.relational.RelationalTable;
import org.judal.storage.table.RecordSet;
import org.judal.storage.relational.RelationalDataSource;

import org.judal.examples.java.model.Student;

public class E16_FetchBySQLQuery {

	@SuppressWarnings("unused")
	@Test
	public void demo() throws Exception {
		
		setUp();

		// The default DataSource for the thread could also be used implicitly.
		// Shown here how to retrieve it and inject into Student constructor
		// for illustrative purposes only.
		RelationalDataSource dts = (RelationalDataSource) EngineFactory.DefaultThreadDataSource.get();
		
		try (RelationalTable students = dts.openRelationalTable(new Student(dts))) {
			AbstractQuery qry = students.newQuery();
			qry.setFilter("last_name='Kol'");
			qry.setOrdering("date_of_birth");
			qry.setResult("id_student,first_name,last_name,date_of_birth");
			RecordSet<Student> kols = students.fetch(qry);
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
