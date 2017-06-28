package org.judal.examples.java.jdbc;

import org.junit.Test;

import java.util.Iterator;

import org.judal.examples.java.model.map.Student;
import org.judal.storage.EngineFactory;
import org.judal.storage.relational.RelationalDataSource;
import org.judal.storage.table.View;

public class E29_IterateOpenResultSet {

	@Test
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void demo() throws Exception {
		
		setUp();

		RelationalDataSource dts = EngineFactory.getDefaultRelationalDataSource();
		
		try (View tbl = dts.openView(new Student())) {
			Iterator cursor = tbl.iterator();
			while (cursor.hasNext()) {
				Student s = (Student) cursor.next();
			}
			tbl.close(cursor);
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
