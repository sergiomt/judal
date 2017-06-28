package org.judal.examples.java.jdbc;

import org.junit.Test;

import org.judal.storage.EngineFactory;
import org.judal.storage.java.RelationalQuery;
import org.judal.storage.table.RecordSet;
import org.judal.storage.table.impl.SingleStringColumnRecord;
import org.judal.examples.java.model.map.Student;
import org.judal.jdbc.JDBCDataSource;
import org.judal.jdbc.metadata.SQLFunctions;

/**
 * Use SQL functions in a way which is portable across RDBMS
 */
public class E24_PortableSQLFunctions {

	@SuppressWarnings("unused")
	@Test
	public void demo() throws Exception {
		
		setUp();

		// For HSQL this will execute:
		// SELECT UPPER(CONCAT(ISNULL(last_name,''),',',ISNULL(first_name,''))) AS full_name FROM student ORDER BY full_name
		
		// For PostgreSQL will execute:
		// SELECT UPPER(COALESCE(last_name,'') || ',' || COALESCE(first_name,'')) AS full_name FROM student ORDER BY full_name
		
		final SQLFunctions f = ((JDBCDataSource) EngineFactory.getDefaultRelationalDataSource()).Functions;
		
		final String[] lastFirstName = new String[] { "last_name", "first_name" };
		
		final String columnAlias = "full_name";
		
		SingleStringColumnRecord name = new SingleStringColumnRecord(Student.TABLE_NAME, columnAlias);
		
		try (RelationalQuery<SingleStringColumnRecord> qry = new RelationalQuery<>(name)) {
			qry.setResultClass(SingleStringColumnRecord.class);
			qry.setResult(f.UPPER + "(" + f.strCat(lastFirstName, ',')+") AS "+columnAlias);
			qry.setOrdering(columnAlias);
			
			RecordSet<SingleStringColumnRecord> names = qry.fetch();

			for (SingleStringColumnRecord rec : names) {
				String n = rec.getString(columnAlias);
			}
		}
		tearDown();
	}

	public static void setUp() throws Exception {
		E10_WriteCSVDataIntoTheDatabase.setUp();
		E10_WriteCSVDataIntoTheDatabase.insertStudentsIntoDatabase();
		E10_WriteCSVDataIntoTheDatabase.insertCoursesIntoDatabase();
		E10_WriteCSVDataIntoTheDatabase.assignStudentsToCoursesIntoDatabase();
	}

	public static void tearDown() throws Exception {
		E10_WriteCSVDataIntoTheDatabase.tearDown();
	}
}
