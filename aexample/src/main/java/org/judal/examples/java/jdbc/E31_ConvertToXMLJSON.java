package org.judal.examples.java.jdbc;

import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.judal.examples.java.model.Course;
import org.judal.storage.query.relational.RelationalQuery;
import org.judal.storage.table.RecordSet;

public class E31_ConvertToXMLJSON {

	@SuppressWarnings("unused")
	@Test
	public void demo() throws Exception {
		
		setUp();

		Course c = new Course();
		
		c.load(1);

		Map<String,String> attribs = new HashMap<String,String>();
		attribs.put("timestamp", new Date().toString());
		
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-DD");
		
		String courseAsXML = c.toXML("  ", attribs, dateFormat, null, null);

		try (RelationalQuery<Course> qry = new RelationalQuery<>(Course.class)) {
			qry.setResult("*");
			qry.setFilter("1=1");
						
			RecordSet<Course> courses = qry.fetch();
			
			String coursesAsXML = courses.toXML("  ", dateFormat, null, null);
			
			String coursesAsJSON = courses.toJSON();
		}
		
		tearDown();
	}
	
	public static void setUp() throws Exception {
		E10_WriteCSVDataIntoTheDatabase.setUp();
		E10_WriteCSVDataIntoTheDatabase.insertCoursesIntoDatabase();
	}

	public static void tearDown() throws Exception {
		E10_WriteCSVDataIntoTheDatabase.tearDown();
	}	
	
}
