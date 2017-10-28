package org.judal.examples.java.jdbc;

import org.junit.Test;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;

import javax.jdo.JDOException;

import org.judal.storage.Param;
import org.judal.storage.EngineFactory;
import org.judal.storage.table.IndexableView;
import org.judal.storage.table.RecordSet;
import org.judal.storage.relational.RelationalDataSource;

import org.judal.metadata.ColumnDef;

import org.judal.jdbc.RDBMS;
import org.judal.jdbc.metadata.SQLViewDef;
import org.judal.jdbc.JDBCRelationalDataSource;
import org.judal.jdbc.JDBCMetadataObjectFactory;

import static org.judal.metadata.JoinType.INNER;
import static org.judal.metadata.NameAlias.AS;

import org.judal.examples.java.model.map.Student;
import org.judal.examples.java.model.map.StudentCourse;
import org.judal.examples.java.model.map.StudentPerCourse;

import static com.knowgate.tuples.Pair.P$;
import static com.knowgate.dateutils.GregorianCalendarLocalisation.addMonths;

/**
 * Example of how to create an inner join and query it
 * and how to perform a query on an already existing view
 * with the same join
 */
public class E20_InnerJoinRelationalQuery {

	@SuppressWarnings({ "unused", "unchecked" })
	@Test
	public void demo() throws Exception {

		setUp();

		Student s = new Student();
		StudentCourse c = new StudentCourse();

		RelationalDataSource dts = EngineFactory.getDefaultRelationalDataSource();

		// Create the inner join on the fly to query students from course 7
		try (IndexableView v = dts.openJoinView(INNER, s, AS(s,"s"), AS(c,"c"), P$("id_student","id_student"))) {
			RecordSet<Student> s80 = v.fetch(s.fetchGroup(), "c.id_course", new Integer(7));
			for (Student t : s80) {
				int id = t.getId();
				String firstName = t.getFirstName();
				String lastName = t.getLastName();
				Calendar dob = t.getDateOfBirth();
			}
		}
		
		// Another way
		// Create a view in the database and query it for courses created less than a month ago
		
		createStudentCourseView((JDBCRelationalDataSource) dts);
		
		StudentPerCourse spc = new StudentPerCourse();
		
		try (IndexableView v = dts.openIndexedView(spc)) {
			RecordSet<StudentPerCourse> sc80 = v.fetch(spc.fetchGroup(), Integer.MAX_VALUE, 0,
					new Param("dt_created",1, addMonths(-1, new Date())));
			for (StudentPerCourse t : sc80) {
				String lastName = t.getLastName();
				Calendar dob = t.getDateOfBirth();
				String courseCode = t.getCourseCode();
				BigDecimal coursePrice = t.getPrice();	
			}
		}
		
		tearDown();
	}

	public static void createStudentCourseView(JDBCRelationalDataSource dts) throws SQLException, NoSuchMethodException, JDOException {
		final String createViewSql = "CREATE VIEW student_per_course AS SELECT s.id_student,s.first_name,s.last_name,s.date_of_birth,c.id_course,c.code,c.nm_course,c.price FROM student s INNER JOIN student_x_course x ON s.id_student=x.id_student INNER JOIN course c ON x.id_course=c.id_course";
		SQLViewDef vdef = JDBCMetadataObjectFactory.newViewDef(RDBMS.valueOf(dts.getRdbmsId()), "student_per_course", createViewSql);
		for (ColumnDef cdef : new StudentPerCourse().getTableDef().getColumns())
			vdef.addColumnMetadata(cdef);
		dts.getMetaData().addView(vdef);
		dts.execute(createViewSql);
	}

	public static void setUp() throws Exception {
		E10_WriteCSVDataIntoTheDatabase.setUp();
		E10_WriteCSVDataIntoTheDatabase.insertCoursesIntoDatabase();
		E10_WriteCSVDataIntoTheDatabase.insertStudentsIntoDatabase();
		E10_WriteCSVDataIntoTheDatabase.assignStudentsToCoursesIntoDatabase();
	}

	public static void tearDown() throws Exception {
		((JDBCRelationalDataSource) EngineFactory.getDefaultRelationalDataSource()).execute("DROP VIEW student_per_course");
		
		E10_WriteCSVDataIntoTheDatabase.tearDown();
	}
}
