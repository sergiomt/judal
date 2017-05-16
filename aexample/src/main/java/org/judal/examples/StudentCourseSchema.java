package org.judal.examples;

import java.util.Map;
import java.util.HashMap;

import java.sql.Types;

import org.judal.metadata.ColumnDef;
import org.judal.metadata.ForeignKeyDef;
import org.judal.metadata.IndexDef;
import org.judal.metadata.NonUniqueIndexDef;
import org.judal.metadata.SchemaMetaData;
import org.judal.metadata.TableDef;

import org.judal.storage.relational.RelationalDataSource;

public class StudentCourseSchema {

	public static SchemaMetaData generateSchemaMetaData(RelationalDataSource dataSource) {

		final boolean NULL = true;

		Map<String, Object> options = new HashMap<>();

		TableDef studentDef = dataSource.createTableDef("student", options);
		studentDef.addPrimaryKeyColumn(null, "id_student", Types.INTEGER);
		studentDef.addColumnMetadata(null, "dt_created", Types.TIMESTAMP, !NULL);
		studentDef.addColumnMetadata(null, "first_name", Types.VARCHAR, 100, !NULL);
		studentDef.addColumnMetadata(null, "last_name", Types.VARCHAR, 100, !NULL);
		studentDef.addColumnMetadata(null, "date_of_birth", Types.DATE, NULL);
		studentDef.addColumnMetadata(null, "photo", Types.BLOB, NULL);
		
		TableDef courseDef = dataSource.createTableDef("course", options);
		courseDef.addPrimaryKeyColumn(null, "id_course", Types.INTEGER);
		courseDef.addColumnMetadata(null, "code", Types.INTEGER, 4, !NULL, IndexDef.Type.ONE_TO_ONE);
		courseDef.addColumnMetadata(null, "nm_course", Types.VARCHAR, 100, !NULL);
		courseDef.addColumnMetadata(null, "dt_created", Types.TIMESTAMP, !NULL);
		courseDef.addColumnMetadata(null, "dt_start", Types.TIMESTAMP, NULL);
		courseDef.addColumnMetadata(null, "dt_end", Types.TIMESTAMP, NULL);
		courseDef.addColumnMetadata(null, "price", Types.DECIMAL, 8, 2, NULL);
		courseDef.addColumnMetadata(null, "description", Types.CLOB, NULL);

		courseDef.addIndexMetadata(new NonUniqueIndexDef(
				courseDef.getName(),
				"u1_course",
				courseDef.getColumnByName("code"),
				IndexDef.Type.ONE_TO_MANY));
		
		TableDef studentXcourseDef = dataSource.createTableDef("student_x_course", options);
		studentXcourseDef.addPrimaryKeyColumn(null, "id_student", Types.INTEGER);
		studentXcourseDef.addPrimaryKeyColumn(null, "id_course", Types.INTEGER);
		
		ForeignKeyDef fk1 = new ForeignKeyDef();
		ColumnDef id_student = studentXcourseDef.getColumnByName("id_student");
		id_student.setTarget("id_student");
		fk1.setTable(studentDef.getName());
		fk1.setName("f1_student_x_course");		
		fk1.addColumn(id_student);
		studentXcourseDef.addForeignKeyMetadata(fk1);
		
		ForeignKeyDef fk2 = new ForeignKeyDef();
		ColumnDef id_course = studentXcourseDef.getColumnByName("id_course");
		id_course.setTarget("id_course");
		fk2.setTable(studentDef.getName());
		fk2.setName("f2_student_x_course");		
		fk2.addColumn(id_course);
		studentXcourseDef.addForeignKeyMetadata(fk2);
		
		SchemaMetaData schemaMetadata = new SchemaMetaData();
		schemaMetadata.setSchema("PUBLIC");
		schemaMetadata.addPackage("default");

		schemaMetadata.addTable(studentDef, "default");
		schemaMetadata.addTable(courseDef, "default");
		schemaMetadata.addTable(studentXcourseDef, "default");

		return schemaMetadata;
	}
	
}
