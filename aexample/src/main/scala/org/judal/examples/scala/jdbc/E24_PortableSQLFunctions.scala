package org.judal.examples.scala.jdbc

import org.junit.Test

import org.judal.storage.EngineFactory
import org.judal.storage.table.RecordSet
import org.judal.storage.table.impl.SingleStringColumnRecord

import org.judal.jdbc.JDBCDataSource
import org.judal.jdbc.metadata.SQLFunctions

import org.judal.storage.scala.RelationalQuery

import org.judal.examples.scala.model.Student

import org.judal.Using._

/**
 * Use SQL functions in a way which is portable across RDBMS
 */
class E24_PortableSQLFunctions {

	@Test
	def demo() : Unit = {
		
		E24_PortableSQLFunctions.setUp()

		// For HSQL this will execute:
		// SELECT UPPER(CONCAT(ISNULL(last_name,''),',',ISNULL(first_name,''))) AS full_name FROM student ORDER BY full_name
		
		// For PostgreSQL will execute:
		// SELECT UPPER(COALESCE(last_name,'') || ',' || COALESCE(first_name,'')) AS full_name FROM student ORDER BY full_name
		
		val f = EngineFactory.getDefaultRelationalDataSource().asInstanceOf[JDBCDataSource].Functions
		
		val lastFirstName = Array[String]("last_name", "first_name")
		
		val columnAlias = "full_name"
		
		val name = new SingleStringColumnRecord(Student.TABLE_NAME, columnAlias)
		var qry : RelationalQuery[SingleStringColumnRecord]= null
		using(qry) {
		  qry = new RelationalQuery[SingleStringColumnRecord](name)
			qry.setResultClass(classOf[SingleStringColumnRecord])
			qry.setResult(f.UPPER + "(" + f.strCat(lastFirstName, ',')+") AS " + columnAlias)
			qry.setOrdering(columnAlias)
			
			for (rec <- qry.fetch) {
				val n = rec.getString(columnAlias)
			}
		}
		
		E24_PortableSQLFunctions.tearDown()
	}

}


object E24_PortableSQLFunctions {
  
	def setUp() = {
		E10_WriteCSVDataIntoTheDatabase.setUp()
		E10_WriteCSVDataIntoTheDatabase.insertStudentsIntoDatabase()
		E10_WriteCSVDataIntoTheDatabase.insertCoursesIntoDatabase()
		E10_WriteCSVDataIntoTheDatabase.assignStudentsToCoursesIntoDatabase()
	}

	def tearDown() = {
		E10_WriteCSVDataIntoTheDatabase.tearDown()
	}

}