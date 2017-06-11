package org.judal.examples.java.jdbc;

import org.junit.Test;

import org.judal.metadata.NameAlias;

import org.judal.storage.table.RecordSet;
import org.judal.storage.java.RelationalQuery;
import org.judal.storage.query.Expression;
import org.judal.storage.query.Predicate;

import static org.judal.storage.query.Operator.EQ;
import static org.judal.storage.query.Operator.EXISTS;

import org.judal.examples.java.model.Student;
import org.judal.examples.java.model.StudentCourse;

public class E22_ExistsSubqueryUsingPredicates {

	@SuppressWarnings("unused")
	@Test
	public void demo() throws Exception {

		setUp();

		//  SELECT * FROM student s WHERE EXISTS (SELECT id_student FROM student_x_course x WHERE s.id_student = x.id_student)

		final String STUDENT_ALIAS = "s";
		final NameAlias STUDENT_X_COURSE = new NameAlias(StudentCourse.TABLE_NAME, "x");
		
		try (RelationalQuery<Student> qry = new RelationalQuery<>(Student.class, STUDENT_ALIAS)) {
			qry.setResult("*");
			
			Predicate sid = qry.newPredicate().and("s.id_student", EQ, new Expression("x.id_student"));
			qry.and("id_student", EXISTS, STUDENT_X_COURSE, sid);
			
			RecordSet<Student> students = qry.fetch();
			for (Student s : students) {
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
