package org.judal.examples.scala.jdbc

import org.junit.Test
import org.junit.Assert.assertTrue

import org.scalatest.Suite

import org.judal.Using._

import org.judal.jdbc.JDBCRelationalDataSource
import org.judal.storage.table.Table

import org.judal.examples.scala.model.Student

/**
 * Open a table from a given DataSource write a couple Records to it reload one
 * of the records and modify it last delete the second record
 */
class E12_OpenTableAndWriteRecords extends Suite {

	@Test def demo() = {

		val dataSource = E12_OpenTableAndWriteRecords.setUp

		E12_OpenTableAndWriteRecords.insertIntoStudentsTable (dataSource)

		E12_OpenTableAndWriteRecords.tearDown (dataSource)
	}

}

object E12_OpenTableAndWriteRecords {

	def insertIntoStudentsTable(dataSource: JDBCRelationalDataSource) = {
		// If the model instance is not created for the
		// default Thread DataSource then the DataSource
		// to be used must be provided in the constructor.
		val s = new Student(dataSource)

		// Tables are opened from a TableDataSource.
		// Tables implement Autocloseable interface.
		// Use them always in a try-with-resources.

		var students : Table = null

		using (students) {

		  students = dataSource.openTable(s)
		  
			s.setId(1)
			s.setFirstName("John")
			s.setLastName("Smith")
			students.store(s)

			// An object instance can be reused after cleared
			s.clear()

			s.setId(2)
			s.setFirstName("Sandra")
			s.setLastName("Blake")
			students.store(s)

			// Reload Student 1 into s and change his DOB
			val found = students.load(1, s)
			s.setDateOfBirth("1977-07-07")
			students.store(s)

			// Delete Student 2
			students.delete(2)

			// load() will return false and leave target Student
			// unmodified when no record is found with the given
			// primary key
			val notfound = students.load(3, s)

		}	
	}
	
	def setUp() = {
		val dataSource = E02_CreateAdditionalDataSource.create()
		E04_CreateTablesFromJDOXML.createSchemaObjects(dataSource)
		dataSource
	}

	def tearDown(dataSource: JDBCRelationalDataSource) = {
		E04_CreateTablesFromJDOXML.dropSchemaObjects(dataSource)
		E02_CreateAdditionalDataSource.close(dataSource)
	}

}