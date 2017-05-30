package org.judal.examples.java.jdbc;

import org.junit.Test;

import java.sql.Date;
import java.util.Calendar;

import org.judal.storage.table.RecordSet;
import org.judal.storage.table.ColumnGroup;

import org.judal.storage.query.relational.RelationalQuery;

import static org.judal.storage.query.Operator.LIKE;
import static org.judal.storage.query.Operator.GT;

import org.judal.examples.java.model.Student;

public class E18_FetchByRelationalQuery {

	@SuppressWarnings({ "unused", "deprecation" })
	@Test
	public void demo() throws Exception {
		
		setUp();
		
		try (RelationalQuery<Student> qry = new RelationalQuery<>(Student.class)) {
			qry.and("last_name", LIKE, "S%").and("date_of_birth", GT, new Date(80, 00, 01));
			qry.setResult(new ColumnGroup("id_student","first_name","last_name","date_of_birth"));
			RecordSet<Student> s80 = qry.fetch();
			for (Student s : s80) {
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
