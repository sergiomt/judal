package org.judal.examples.scala.jdbc

import java.math.BigDecimal

import javax.transaction.Status
import javax.transaction.TransactionManager

import org.junit.Test
import org.junit.Before
import org.junit.After

import org.junit.Assert.assertTrue
import org.junit.Assert.assertFalse
import org.junit.Assert.assertEquals

import org.judal.examples.Resources.getResourceAsStream
import org.judal.storage.DataSource.AUTOCOMMIT
import org.judal.storage.DataSource.DEFAULT_MAXPOOLSIZE
import org.judal.storage.DataSource.DEFAULT_POOLSIZE
import org.judal.storage.DataSource.DEFAULT_USE_DATABASE_METADATA
import org.judal.storage.DataSource.DRIVER
import org.judal.storage.DataSource.MAXPOOLSIZE
import org.judal.storage.DataSource.PASSWORD
import org.judal.storage.DataSource.POOLSIZE
import org.judal.storage.DataSource.SCHEMA
import org.judal.storage.DataSource.URI
import org.judal.storage.DataSource.USER
import org.judal.storage.DataSource.USE_DATABASE_METADATA

import org.judal.storage.Param
import org.judal.storage.DataSource
import org.judal.storage.Engine
import org.judal.storage.table.IndexableView
import org.judal.transaction.DataSourceTransactionManager.Transact

import org.judal.metadata.SchemaMetaData
import org.judal.metadata.bind.JdoXmlMetadata

import org.judal.jdbc.JDBCEngine
import org.judal.jdbc.JDBCRelationalDataSource

import org.judal.examples.java.model.map.Course
import org.judal.examples.java.model.map.Student

import org.judal.Using._

import scala.collection.JavaConverters._

/**
 * Example of how to coordinate a transaction between two data sources
 */
class E33_Transaction2 {

	var dataSource1: JDBCRelationalDataSource  = null
	var dataSource2: JDBCRelationalDataSource = null
	
	@Test
	def demo() : Unit = {
		var v : IndexableView = null
		val txm = dataSource1.getTransactionManager
		
		val s = new Student(dataSource1)
		s.setId(100)
		s.setFirstName("Jhon")
		s.setLastName("McMillan")
		s.setDateOfBirth("1971-02-03")

		val c = new Course(dataSource2)
		c.setId(100)
		c.setCode("CU01")
		c.setCourseName("Applied electromechanics")
		c.setPrice(new BigDecimal("2134"))
		c.setStartDate("2017-08-09")
		c.setStartDate("2017-10-11")

		txm.begin
		
		assertEquals(Status.STATUS_ACTIVE, txm.getStatus)
		
		s.store(dataSource1)
		c.store(dataSource2)				
		
		txm.rollback
		
		using (v) {
		  v = dataSource1.openIndexedView(s)
			assertFalse(v.exists(Seq(new Param("last_name", 1, "McMillan")):_*))
		}
		using (v) {
		  v = dataSource2.openIndexedView(c)
			assertFalse(v.exists(Seq(new Param("code", 1, "CU01")):_*))
		}
		
		txm.begin

		assertEquals(Status.STATUS_ACTIVE, txm.getStatus)
		
		s.store(dataSource1)
		c.store(dataSource2)				
		
		txm.commit
		
		using (v) {
		  v = dataSource1.openIndexedView(s)
			assertTrue(v.exists(Seq(new Param("last_name", 1, "McMillan")):_*))			
		}
		using (v) {
		  v = dataSource2.openIndexedView(c)
			assertTrue(v.exists(Seq(new Param("code", 1, "CU01")):_*))
		}

	}

	@Before
	def setUp() : Unit = {
		var xmlMeta: JdoXmlMetadata = null
		var metadata: SchemaMetaData  = null
		val options = Map[String,AnyRef](DataSource.CATALOG -> "PUBLIC", DataSource.SCHEMA -> "PUBLIC").asJava

		val jdbc = new JDBCEngine
		
		dataSource1 = jdbc.getDataSource(dataSourceProperties("demo1"), Transact)

		xmlMeta = new JdoXmlMetadata (dataSource1)		
		metadata = xmlMeta.readMetadata(getResourceAsStream("jdometadata.xml"))
		dataSource1.createTable(metadata.getTable(Student.TABLE_NAME), options)
		dataSource1.execute("CREATE SEQUENCE seq_student AS BIGINT START WITH 1 INCREMENT BY 1")		
		
		dataSource2 = jdbc.getDataSource(dataSourceProperties("demo2"), Transact)
		xmlMeta = new JdoXmlMetadata (dataSource2)		
		metadata = xmlMeta.readMetadata(getResourceAsStream("jdometadata.xml"))
		dataSource2.createTable(metadata.getTable(Course.TABLE_NAME), options)		
		dataSource2.execute("CREATE SEQUENCE seq_course AS BIGINT START WITH 1 INCREMENT BY 1")

	}

	@After
	def tearDown() : Unit = {
		dataSource2.dropTable(Course.TABLE_NAME, false)
		dataSource2.close
		dataSource1.dropTable(Student.TABLE_NAME, false)
		dataSource1.close
	}	

	def dataSourceProperties(dbName: String) = Map (
		
		DRIVER -> "org.hsqldb.jdbc.JDBCDriver",
		URI -> ("jdbc:hsqldb:mem:" + dbName),
		USER -> "sa",
		PASSWORD -> "",
		POOLSIZE -> DEFAULT_POOLSIZE,
		MAXPOOLSIZE -> DEFAULT_MAXPOOLSIZE,
		SCHEMA -> "PUBLIC",
		USE_DATABASE_METADATA -> DEFAULT_USE_DATABASE_METADATA,
		AUTOCOMMIT -> "false").asJava
	
}