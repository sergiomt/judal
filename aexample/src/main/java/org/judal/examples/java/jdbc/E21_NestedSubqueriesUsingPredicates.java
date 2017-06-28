package org.judal.examples.java.jdbc;

import java.math.BigDecimal;

import org.junit.Test;
import org.judal.examples.java.model.map.Course;
import org.judal.examples.java.model.map.Student;
import org.judal.examples.java.model.map.StudentCourse;
import org.judal.storage.java.RelationalQuery;
import org.judal.storage.query.Predicate;
import org.judal.storage.table.ColumnGroup;
import org.judal.storage.table.RecordSet;

import static org.judal.storage.query.Operator.IN;
import static org.judal.storage.query.Operator.LT;

public class E21_NestedSubqueriesUsingPredicates {

	@SuppressWarnings("unused")
	@Test
	public void demo() throws Exception {

		setUp();

		//  SELECT * FROM student WHERE id_student IN
		// (SELECT id_student FROM student_x_course WHERE id_course IN
		// (SELECT id_course FROM course WHERE price < 1000))

		try (RelationalQuery<Student> qry = new RelationalQuery<>(Student.class)) {
			Predicate lt1k = qry.newPredicate().and("price", LT, new BigDecimal("1000")); 
			Predicate sxc = qry.newPredicate().and("id_course", IN, Course.TABLE_NAME, "id_course", lt1k); 
			qry.and("id_student", IN, StudentCourse.TABLE_NAME, "id_student", sxc);
			qry.setResult(new ColumnGroup("id_student"));
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