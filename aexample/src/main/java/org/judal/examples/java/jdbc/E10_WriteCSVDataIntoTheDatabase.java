package org.judal.examples.java.jdbc;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.ParseException;
import java.util.List;

import javax.jdo.JDOException;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

import com.knowgate.io.IOUtils;

import org.judal.storage.EngineFactory;
import org.judal.storage.relational.RelationalDataSource;
import org.judal.storage.relational.RelationalView;
import org.judal.storage.table.impl.SingleLongColumnRecord;
import org.judal.jdbc.JDBCRelationalDataSource;

import org.judal.examples.Resources;
import org.judal.examples.java.model.map.Course;
import org.judal.examples.java.model.map.Student;
import org.judal.examples.java.model.map.StudentCourse;

/**
 * Insert data from a comma delimited file into the database
 * using Default Relational DataSource kept at StorageContext
 */
public class E10_WriteCSVDataIntoTheDatabase {
	
	@Test
	public void demo() throws Exception {
		
		setUp();
		
		insertStudentsIntoDatabase();
		
		insertCoursesIntoDatabase();
		
		assignStudentsToCoursesIntoDatabase();

		tearDown();
	}

	public static void insertStudentsIntoDatabase() throws IOException, ParseException {
		List<String> lines = IOUtils.readLines(Resources.getResourceAsStream("students.csv"));

		// Write new students into the database
		for (String line : lines) {
			String[] fields = line.split(";");
			Student s = new Student();
			s.setId(Integer.parseInt(fields[0]));
			s.setFirstName(fields[1]);
			s.setLastName(fields[2]);
			s.setDateOfBirth(fields[3]);
			// This call use EngineFactory.DefaultThreadDataSource under the hood
			s.store();
		}
		
		assertEquals(lines.size(), countRows(Student.TABLE_NAME));
	}

	public static void insertCoursesIntoDatabase() throws IOException, ParseException {
		List<String> lines = IOUtils.readLines(Resources.getResourceAsStream("courses.csv"));

		// Write new students into the database
		for (String line : lines) {
			String[] fields = line.split(";");
			Course c = new Course();
			c.setId(Integer.parseInt(fields[0]));
			c.setCode(fields[1]);
			c.setCourseName(fields[2]);
			c.setStartDate(fields[3]);
			c.setEndDate(fields[4]);
			c.setPrice(new BigDecimal(fields[5]));
			c.setDescription(fields[6]);
			// This call use EngineFactory.DefaultThreadDataSource under the hood
			c.store();
		}
		
		
		assertEquals(lines.size(), countRows(Course.TABLE_NAME));
	}
	
	public static void assignStudentsToCoursesIntoDatabase() throws IOException {
		List<String> lines = IOUtils.readLines(Resources.getResourceAsStream("studentcourse.csv"));

		// Write new students into the database
		for (String line : lines) {
			String[] fields = line.split(",");
			StudentCourse sc = new StudentCourse();
			sc.setStudentId(Integer.parseInt(fields[0]));
			sc.setCourseId(Integer.parseInt(fields[1]));
			// This call use EngineFactory.DefaultThreadDataSource under the hood
			sc.store();
		}		
		assertEquals(lines.size(), countRows(StudentCourse.TABLE_NAME));
	}

	public static int countRows(final String TableName) throws JDOException {
		SingleLongColumnRecord count = new SingleLongColumnRecord(TableName, "c");
		RelationalDataSource dts = EngineFactory.getDefaultRelationalDataSource();
		try (RelationalView courses = dts.openRelationalView(count)) {
			return courses.count(null).intValue();
		}
	}
	
	public static JDBCRelationalDataSource setUp() throws Exception {
		JDBCRelationalDataSource dataSource = E01_CreateDefaultRelationalDataSource.create();
		E04_CreateTablesFromJDOXML.createSchemaObjects(dataSource);
		dataSource.execute("CREATE SEQUENCE seq_student AS BIGINT START WITH 1 INCREMENT BY 1");
		dataSource.execute("CREATE SEQUENCE seq_course AS BIGINT START WITH 1 INCREMENT BY 1");
		return dataSource;
	}

	public static void tearDown() throws Exception {
		E04_CreateTablesFromJDOXML.dropSchemaObjects(EngineFactory.getDefaultRelationalDataSource());
		E01_CreateDefaultRelationalDataSource.close();
	}

}