package org.judal.examples.java.jdbc;

import org.junit.Test;

import org.judal.storage.EngineFactory;
import org.judal.storage.query.Predicate;
import org.judal.storage.relational.RelationalDataSource;
import org.judal.storage.relational.RelationalView;
import org.judal.storage.table.impl.SingleBigDecimalColumnRecord;
import org.judal.storage.table.impl.SingleDateColumnRecord;
import org.judal.storage.table.impl.SingleLongColumnRecord;

import static org.judal.storage.query.Operator.ISNOTNULL;
import static org.judal.storage.query.Operator.LIKE;

import java.math.BigDecimal;
import java.util.Date;

import org.judal.examples.java.model.Course;
import org.judal.examples.java.model.Student;
import org.judal.examples.java.jdbc.E10_WriteCSVDataIntoTheDatabase;

public class E23_AggregateFunctions {

	@SuppressWarnings("unused")
	@Test
	public void demo() throws Exception {
		
		setUp();
		final String COLUMN_NAME = "a";
		RelationalDataSource dts = (RelationalDataSource) EngineFactory.DefaultThreadDataSource.get();
		
		SingleLongColumnRecord count = new SingleLongColumnRecord(Student.TABLE_NAME, COLUMN_NAME);
		try (RelationalView students = dts.openRelationalView(count)) {
			Predicate like_S = students.newPredicate().and("last_name", LIKE, "S%");
			long l = students.count(like_S);
		}

		SingleDateColumnRecord youngest = new SingleDateColumnRecord(Student.TABLE_NAME, COLUMN_NAME);
		try (RelationalView students = dts.openRelationalView(youngest)) {
			Predicate like_S = students.newPredicate().and("last_name", LIKE, "S%");
			Date d = (Date) students.max("date_of_birth", like_S);
		}
		
		SingleBigDecimalColumnRecord sum = new SingleBigDecimalColumnRecord(Course.TABLE_NAME, COLUMN_NAME);
		try (RelationalView courses = dts.openRelationalView(sum)) {
			Predicate all = courses.newPredicate().and("price", ISNOTNULL);
			BigDecimal total = (BigDecimal) courses.sum("price", all);
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
