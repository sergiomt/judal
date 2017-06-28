package org.judal.examples.java.jdbc;

import java.util.GregorianCalendar;

import org.junit.Test;
import org.judal.examples.java.model.map.Student;
import org.judal.storage.java.RelationalQuery;
import org.judal.storage.table.RecordSet;

import static org.judal.storage.query.Operator.BETWEEN;

/**
 * Fetch students whose date of birth is between 01/01/1980 and 31/12/1989
 */
public class E19_FetchFilteringBetweenDates {

	@SuppressWarnings({ "unused", "unchecked" })
	@Test
	public void demo() throws Exception {
		
		setUp();
		
		try (RelationalQuery<Student> qry = new RelationalQuery<>(Student.class)) {
			// Fetch students born between 01/01/1980 and 31/12/1989 ordering results from youngest to oldest
			qry.setResult(new Student().fetchGroup().getMembers());
			qry.and("date_of_birth", BETWEEN,
					new GregorianCalendar(1980,0,1),
					new GregorianCalendar(1989,11,31));
			qry.setOrdering("date_of_birth DESC");
			RecordSet<Student> s80 = qry.fetch();
			for (Student s : s80) {
				int id = s.getId();
				String firstName = s.getFirstName();
				String lastName = s.getLastName();
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
