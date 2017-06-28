package org.judal.examples.java.model.map;

import javax.jdo.JDOException;

import org.judal.storage.EngineFactory;
import org.judal.storage.java.MapRecord;
import org.judal.storage.relational.RelationalDataSource;

/**
 * Extend MapRecord in order to create model classes manageable by JUDAL.
 * Add your getters and setters for database fields.
 */
public class StudentCourse extends MapRecord {

	private static final long serialVersionUID = 1L;
	
	public static final String TABLE_NAME = "student_x_course";
	
	public StudentCourse() throws JDOException {
		this(EngineFactory.getDefaultRelationalDataSource());
	}

	public StudentCourse(RelationalDataSource dataSource) throws JDOException {
		super(dataSource, TABLE_NAME);
	}

	public int getCourseId() {
		return getInt("id_course");
	}

	public void setCourseId(final int id) {
		put("id_course", id);
	}

	public int getStudentId() {
		return getInt("id_student");
	}

	public void setStudentId(final int id) {
		put("id_student", id);
	}
	
}
