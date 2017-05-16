package org.judal.examples.java.jdbc;

import org.junit.Test;
import org.judal.examples.java.model.Student;

/**
 * Load a Student from the database using his primary key to find him
 * Connect using the default Thread DataSource as provided by
 * EngineFactory.DefaultThreadDataSource.get()
 */
public class E11_LoadRecordByPrimaryKeyFromDefaultDataSourceModifyAndStore {

	@Test
	public void demo() throws Exception {

		setUp();
		
		Student s = new Student();
		
		s.load(new Integer(1));
		
		s.setDateOfBirth("1971-01-01");

		s.store();

		tearDown();
	}

	public static void setUp() throws Exception {
		E10_WriteCSVDataIntoTheDatabase.setUp();
		E10_WriteCSVDataIntoTheDatabase.insertStudentsIntoDatabase();
	}

	public static void tearDown() throws Exception {
		E10_WriteCSVDataIntoTheDatabase.tearDown();
	}
	
}
