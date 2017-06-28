package org.judal.examples.java.model.pojo;

import javax.jdo.JDOException;

import org.judal.storage.EngineFactory;
import org.judal.storage.java.PojoRecord;
import org.judal.storage.relational.RelationalDataSource;

/**
 * Extend PojoRecord in order to create model classes manageable by JUDAL.
 * Add your getters and setters for database fields.
 */
public class StudentCourse extends PojoRecord {

	private static final long serialVersionUID = 1L;
	
	public static final String TABLE_NAME = "student_x_course";

	private int id_course;
	private int id_student;
	
	public StudentCourse() throws JDOException {
		this(EngineFactory.getDefaultRelationalDataSource());
	}

	public StudentCourse(RelationalDataSource dataSource) throws JDOException {
		super(dataSource, TABLE_NAME);
	}

	public int getCourseId() {
		return id_course;
	}

	public void setCourseId(final int id) {
		id_course = id;
	}

	public int getStudentId() {
		return id_student;
	}

	public void setStudentId(final int id) {
		id_student = id;
	}
	
}
