package org.judal.examples.java.jdbc;

import java.math.BigDecimal;

import javax.transaction.Status;
import javax.transaction.TransactionManager;

import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;

import org.judal.storage.Param;
import org.judal.storage.EngineFactory;
import org.judal.storage.table.IndexableView;

import org.judal.examples.java.model.map.Course;
import org.judal.examples.java.model.map.Student;
import org.judal.jdbc.JDBCRelationalDataSource;

/**
 * Example of how to commit or rollback a transaction
 */
public class E32_Transaction1 {

	@Test
	public void demo() throws Exception {
		
		setUp();

		JDBCRelationalDataSource dts = (JDBCRelationalDataSource) EngineFactory.getDefaultRelationalDataSource();
		dts.setDefaultAutoCommit(false);

		TransactionManager txm = dts.getTransactionManager();
		
		Course c = new Course(dts);
		c.setId(100);
		c.setCode("CU01");
		c.setCourseName("Applied electromechanics");
		c.setPrice(new BigDecimal("2134"));
		c.setStartDate("2017-08-09");
		c.setStartDate("2017-10-11");

		Student s = new Student(dts);
		s.setId(100);
		s.setFirstName("Jhon");
		s.setLastName("McMillan");
		s.setDateOfBirth("1971-02-03");

		txm.begin();
		
		assertEquals(Status.STATUS_ACTIVE, txm.getStatus());
		
		c.store(dts);				
		s.store(dts);
		
		txm.rollback();
		
		try (IndexableView  v = dts.openIndexedView(c)) {
			assertFalse(v.exists(new Param("code", 1, "CU01"), null));
		}
		try (IndexableView  v = dts.openIndexedView(s)) {
			assertFalse(v.exists(new Param("last_name", 1, "McMillan"), null));			
		}
		
		txm.begin();

		assertEquals(Status.STATUS_ACTIVE, txm.getStatus());
		
		c.store(dts);				
		s.store(dts);
		
		txm.commit();
		
		try (IndexableView  v = dts.openIndexedView(c)) {
			assertTrue(v.exists(new Param("code", 1, "CU01"), null));
		}
		try (IndexableView  v = dts.openIndexedView(s)) {
			assertTrue(v.exists(new Param("last_name", 1, "McMillan"), null));			
		}

		tearDown();
	}

	public static void setUp() throws Exception {
		E10_WriteCSVDataIntoTheDatabase.setUp();
		E10_WriteCSVDataIntoTheDatabase.insertCoursesIntoDatabase();
		E10_WriteCSVDataIntoTheDatabase.insertStudentsIntoDatabase();
	}

	public static void tearDown() throws Exception {
		E10_WriteCSVDataIntoTheDatabase.tearDown();
	}	

}