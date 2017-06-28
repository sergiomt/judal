package org.judal.examples.java.jdbc;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import javax.transaction.Status;
import javax.transaction.TransactionManager;

import org.junit.Test;
import org.junit.Before;
import org.junit.After;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;

import static org.judal.examples.Resources.getResourceAsStream;
import static org.judal.storage.DataSource.AUTOCOMMIT;
import static org.judal.storage.DataSource.DEFAULT_MAXPOOLSIZE;
import static org.judal.storage.DataSource.DEFAULT_POOLSIZE;
import static org.judal.storage.DataSource.DEFAULT_USE_DATABASE_METADATA;
import static org.judal.storage.DataSource.DRIVER;
import static org.judal.storage.DataSource.MAXPOOLSIZE;
import static org.judal.storage.DataSource.PASSWORD;
import static org.judal.storage.DataSource.POOLSIZE;
import static org.judal.storage.DataSource.SCHEMA;
import static org.judal.storage.DataSource.URI;
import static org.judal.storage.DataSource.USER;
import static org.judal.storage.DataSource.USE_DATABASE_METADATA;

import org.judal.storage.Param;
import org.judal.storage.DataSource;
import org.judal.storage.Engine;
import org.judal.storage.table.IndexableView;
import static org.judal.transaction.DataSourceTransactionManager.Transact;

import org.judal.metadata.SchemaMetaData;
import org.judal.metadata.bind.JdoXmlMetadata;

import org.judal.jdbc.JDBCEngine;
import org.judal.jdbc.JDBCRelationalDataSource;

import org.judal.examples.java.model.map.Course;
import org.judal.examples.java.model.map.Student;

/**
 * Example of how to coordinate a transaction between two data sources
 */
public class E33_Transaction2 {

	private JDBCRelationalDataSource dataSource1;
	private JDBCRelationalDataSource dataSource2;
	
	@Test
	public void demo() throws Exception {
		
		TransactionManager txm = dataSource1.getTransactionManager();
		
		Student s = new Student(dataSource1);
		s.setId(100);
		s.setFirstName("Jhon");
		s.setLastName("McMillan");
		s.setDateOfBirth("1971-02-03");

		Course c = new Course(dataSource2);
		c.setId(100);
		c.setCode("CU01");
		c.setCourseName("Applied electromechanics");
		c.setPrice(new BigDecimal("2134"));
		c.setStartDate("2017-08-09");
		c.setStartDate("2017-10-11");

		txm.begin();
		
		assertEquals(Status.STATUS_ACTIVE, txm.getStatus());
		
		s.store(dataSource1);
		c.store(dataSource2);				
		
		txm.rollback();
		
		try (IndexableView  v = dataSource1.openIndexedView(s)) {
			assertFalse(v.exists(new Param("last_name", 1, "McMillan"), null));			
		}
		try (IndexableView  v = dataSource2.openIndexedView(c)) {
			assertFalse(v.exists(new Param("code", 1, "CU01"), null));
		}
		
		txm.begin();

		assertEquals(Status.STATUS_ACTIVE, txm.getStatus());
		
		s.store(dataSource1);
		c.store(dataSource2);				
		
		txm.commit();
		
		try (IndexableView  v = dataSource1.openIndexedView(s)) {
			assertTrue(v.exists(new Param("last_name", 1, "McMillan"), null));			
		}
		try (IndexableView  v = dataSource2.openIndexedView(c)) {
			assertTrue(v.exists(new Param("code", 1, "CU01"), null));
		}

	}

	@Before
	public void setUp() throws Exception {
		JdoXmlMetadata xmlMeta;
		SchemaMetaData metadata;
		Map<String,Object> options = new HashMap<>();
		options.put(DataSource.CATALOG, "PUBLIC");
		options.put(DataSource.SCHEMA, "PUBLIC");

		Engine<JDBCRelationalDataSource> jdbc = new JDBCEngine();
		
		dataSource1 = jdbc.getDataSource(dataSourceProperties("demo1"), Transact);

		xmlMeta = new JdoXmlMetadata (dataSource1);		
		metadata = xmlMeta.readMetadata(getResourceAsStream("jdometadata.xml"));
		dataSource1.createTable(metadata.getTable(Student.TABLE_NAME), options);
		dataSource1.execute("CREATE SEQUENCE seq_student AS BIGINT START WITH 1 INCREMENT BY 1");		
		
		dataSource2 = jdbc.getDataSource(dataSourceProperties("demo2"), Transact);
		xmlMeta = new JdoXmlMetadata (dataSource2);		
		metadata = xmlMeta.readMetadata(getResourceAsStream("jdometadata.xml"));
		dataSource2.createTable(metadata.getTable(Course.TABLE_NAME), options);		
		dataSource2.execute("CREATE SEQUENCE seq_course AS BIGINT START WITH 1 INCREMENT BY 1");

	}

	@After
	public void tearDown() throws Exception {
		dataSource2.dropTable(Course.TABLE_NAME, false);
		dataSource2.close();
		dataSource1.dropTable(Student.TABLE_NAME, false);
		dataSource1.close();
	}	

	public static Map<String, String> dataSourceProperties(final String dbName) {
		
		Map<String, String> properties = new HashMap<>();
		properties.put(DRIVER, "org.hsqldb.jdbc.JDBCDriver");
		properties.put(URI, "jdbc:hsqldb:mem:" + dbName);
		properties.put(USER, "sa");
		properties.put(PASSWORD, "");
		properties.put(POOLSIZE, DEFAULT_POOLSIZE);
		properties.put(MAXPOOLSIZE, DEFAULT_MAXPOOLSIZE);
		properties.put(SCHEMA, "PUBLIC");
		properties.put(USE_DATABASE_METADATA, DEFAULT_USE_DATABASE_METADATA);
		properties.put(AUTOCOMMIT, "false");

		return properties;
	}
	
}