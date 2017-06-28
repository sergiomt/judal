package org.judal.examples.java.jdbc;

import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Types;
import java.util.Calendar;

import org.judal.storage.table.RecordSet;
import org.judal.examples.java.model.array.Course;
import org.judal.examples.java.model.map.Student;
import org.judal.storage.Param;
import org.judal.storage.java.RelationalQuery;
import org.judal.storage.query.Predicate;
import org.judal.storage.table.ColumnGroup;

import static org.judal.storage.query.Operator.LIKE;
import static org.judal.storage.query.Operator.GT;
import static org.judal.storage.query.Connective.AND;

/**
 * Fetch records using query builders
 */
public class E18_FetchByRelationalQuery {

	@SuppressWarnings({ "unused", "deprecation" })
	@Test
	public void demo() throws Exception {
		
		setUp();
		
		// SELECT id_student, first_name , last_name, date_of_birth FROM student WHERE last_name LIKE 'S%' AND date_of_birth > '1980-01-01'
		try (RelationalQuery<Student> qry = new RelationalQuery<>(Student.class)) {
			qry.and("last_name", LIKE, "S%").and("date_of_birth", GT, new Date(80, 0, 1));
			qry.setResult(new ColumnGroup("id_student","first_name","last_name","date_of_birth"));
			RecordSet<Student> s80 = qry.fetch();
			for (Student s : s80) {
				int id = s.getId();
				String firstName = s.getFirstName();
				String lastName = s.getLastName();
				Calendar dob = s.getDateOfBirth();
			}
		}

		// SELECT * FROM course WHERE dt_created > '2017-05-01' AND price > 1000
		try (RelationalQuery<Course> qry = new RelationalQuery<>(Course.class)) {
			Predicate prd = qry.newPredicate(AND, GT, new Param[] {
				new Param("dt_created", 1, new Date(117, 4, 1)),
				new Param("price", Types.DECIMAL, 2, new BigDecimal("1000")) });
			qry.setFilter(prd);
			qry.setResult("*");
			RecordSet<Course> c17 = qry.fetch();
			for (Course c : c17) {
				int id = c.getId();
				String cCode = c.getCode();
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
