package org.judal.examples.java.jdbc;

import java.text.ParseException;

import org.judal.examples.java.model.Student;
import org.judal.jdbc.JDBCRelationalDataSource;
import org.judal.storage.table.Table;
import org.junit.Test;

/**
 * Open a table from a given DataSource write a couple Records to it reload one
 * of the records and modify it last delete the second record
 */
public class E12_OpenTableAndWriteRecords {

	@Test
	public void demo() throws Exception {

		JDBCRelationalDataSource dataSource = setUp();

		insertIntoStudentsTable(dataSource);

		tearDown(dataSource);
	}

	public static void insertIntoStudentsTable(JDBCRelationalDataSource dataSource) throws ParseException {
		// If the model instance is not created for the
		// default Thread DataSource then the DataSource
		// to be used must be provided in the constructor.
		Student s = new Student(dataSource);

		// Tables are opened from a TableDataSource.
		// Tables implement Autocloseable interface.
		// Use them always in a try-with-resources.

		try (Table students = dataSource.openTable(s)) {

			s.setId(1);
			s.setFirstName("John");
			s.setLastName("Smith");
			students.store(s);

			// An object instance can be reused after cleared
			s.clear();

			s.setId(2);
			s.setFirstName("Sandra");
			s.setLastName("Blake");
			students.store(s);

			// Reload Student 1 into s and change his DOB
			boolean found = students.load(new Integer(1), s);
			s.setDateOfBirth("1977-07-07");
			students.store(s);

			// Delete Student 2
			students.delete(new Integer(2));

			// load() will return false and leave target Student
			// unmodified when no record is found with the given
			// primary key
			boolean notfound = students.load(new Integer(3), s);

		}	
	}
	
	public static JDBCRelationalDataSource setUp() throws Exception {
		JDBCRelationalDataSource dataSource = E02_CreateAdditionalDataSource.create();
		E04_CreateTablesFromJDOXML.createSchemaObjects(dataSource);
		return dataSource;
	}

	public static void tearDown(JDBCRelationalDataSource dataSource) throws Exception {
		E04_CreateTablesFromJDOXML.dropSchemaObjects(dataSource);
		E02_CreateAdditionalDataSource.close(dataSource);
	}

}
