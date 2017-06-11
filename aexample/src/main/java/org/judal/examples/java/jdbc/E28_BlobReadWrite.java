package org.judal.examples.java.jdbc;

import org.junit.Test;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertArrayEquals;

import java.io.InputStream;

import org.judal.examples.Resources;
import org.judal.examples.java.model.Student;
import org.judal.storage.java.TableOperation;

/**
 * Example of how to write and read a LONGVARBINARY field
 */
public class E28_BlobReadWrite {

	@Test
	public void demo() throws Exception {
		
		final int photoSize = 4832;
		byte[] photoBytes = new byte[photoSize];
		
		setUp();

		// Blobs are written and read using byte arrays as any other column
		try (TableOperation<Student> op = new TableOperation<>(new Student())) {
			Student s1 = op.load(new Integer(1));
			
			try (InputStream photo = Resources.getResourceAsStream("photo.jpg")) {
				assertNotNull(photo);
				photo.read(photoBytes);				
			}
			s1.put("photo", photoBytes);
			s1.store();			
		}

		try (TableOperation<Student> op = new TableOperation<>(new Student())) {
			Student s1 = op.load(new Integer(1));
			assertArrayEquals(photoBytes, s1.getBytes("photo"));
		}

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