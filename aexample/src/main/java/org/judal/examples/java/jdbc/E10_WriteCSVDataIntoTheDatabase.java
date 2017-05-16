package org.judal.examples.java.jdbc;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.ParseException;
import java.util.List;

import org.junit.Test;

import com.knowgate.io.IOUtils;

import org.judal.storage.EngineFactory;
import org.judal.storage.relational.RelationalDataSource;
import org.judal.jdbc.JDBCRelationalDataSource;

import org.judal.examples.Resources;
import org.judal.examples.java.model.Course;
import org.judal.examples.java.model.Student;
import org.judal.examples.java.model.StudentCourse;


/**
 * Insert data from a comma delimited file into the database
 * using Default Relational DataSource kept at StorageContext
 */
@SuppressWarnings("unchecked")
public class E10_WriteCSVDataIntoTheDatabase {
	
	@Test
	public void demo() throws Exception {
		
		setUp();
		
		insertStudentsIntoDatabase();
		
		insertCoursesIntoDatabase();

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
	}
	
	public static void assignStudentsToCoursesIntoDatabase() throws IOException {
		List<String> lines = IOUtils.readLines(Resources.getResourceAsStream("studentcourses.csv"));

		// Write new students into the database
		for (String line : lines) {
			String[] fields = line.split(";");
			StudentCourse sc = new StudentCourse();
			sc.setStudentId(Integer.parseInt(fields[0]));
			sc.setCourseId(Integer.parseInt(fields[1]));
			// This call use EngineFactory.DefaultThreadDataSource under the hood
			sc.store();
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
		E04_CreateTablesFromJDOXML.dropSchemaObjects((RelationalDataSource) EngineFactory.DefaultThreadDataSource.get());
		E01_CreateDefaultRelationalDataSource.close();
	}

}