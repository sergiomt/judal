package org.judal.examples.java.jdbc;

import org.junit.Test;

import java.util.Calendar;

import org.judal.storage.EngineFactory;
import org.judal.storage.table.IndexableView;
import org.judal.storage.table.RecordSet;
import org.judal.storage.relational.RelationalDataSource;

import org.judal.examples.java.model.Student;
import org.judal.examples.java.model.StudentCourse;

import static org.judal.metadata.JoinType.INNER;
import static org.judal.metadata.NameAlias.AS;

import static com.knowgate.tuples.Pair.P$;

public class E20_InnerJoinRelationalQuery {

	@SuppressWarnings({ "unused", "unchecked" })
	@Test
	public void demo() throws Exception {

		setUp();

		Student s = new Student();
		StudentCourse c = new StudentCourse();

		RelationalDataSource dts = EngineFactory.getDefaultRelationalDataSource();

		try (IndexableView v = dts.openJoinView(INNER, s, AS(s,"s"), AS(c,"c"), P$("id_student","id_student"))) {
			RecordSet<Student> s80 = v.fetch(s.fetchGroup(), c.getTableName()+".id_course", new Integer(7));
			for (Student t : s80) {
				int id = t.getId();
				String firstName = t.getFirstName();
				String lastName = t.getLastName();
				Calendar dob = t.getDateOfBirth();
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
