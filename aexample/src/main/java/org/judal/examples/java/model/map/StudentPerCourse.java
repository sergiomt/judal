package org.judal.examples.java.model.map;

import java.math.BigDecimal;
import java.util.Calendar;

import javax.jdo.JDOException;

import org.judal.storage.EngineFactory;
import org.judal.storage.java.MapRecord;
import org.judal.storage.relational.RelationalDataSource;

/**
 * Record for a view joining students and courses
 */
public class StudentPerCourse extends MapRecord {

	private static final long serialVersionUID = 1L;

	public static final String VIEW_NAME = "student_per_course";
	
	public static final String[] columnNames = "id_student,first_name,last_name,date_of_birth,id_course,code,nm_course,price".split(",");

	public StudentPerCourse() throws JDOException {
		this(EngineFactory.getDefaultRelationalDataSource());
	}

	public StudentPerCourse(RelationalDataSource dataSource) throws JDOException {
		super(dataSource, VIEW_NAME, columnNames);
	}

	public int getStudentId() {
		return getInt("id_student");
	}

	public String getFirstName() {
		return getString("first_name");
	}

	public String getLastName() {
		return getString("last_name");
	}

	public Calendar getDateOfBirth() {
		return getCalendar("date_of_birth");
	}

	public int getCourseId() {
		return getInt("id_course");
	}
	
	public String getCourseCode() {
		return getString("code");
	}
	
	public String getCourseName() {
		return getString("nm_course");
	}

	public BigDecimal getPrice() {
		return getDecimal("price");
	}
}