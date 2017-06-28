package org.judal.examples.java.model.array;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;

import javax.jdo.JDOException;

import org.judal.storage.EngineFactory;
import org.judal.storage.java.ArrayRecord;
import org.judal.storage.relational.RelationalDataSource;

/**
 * Extend ArrayRecord in order to create model classes manageable by JUDAL.
 * Add your getters and setters for database fields.
 */
public class Course extends ArrayRecord {

	private static final long serialVersionUID = 1L;
	
	public static final String TABLE_NAME = "course";
	
	public Course() throws JDOException {
		this(EngineFactory.getDefaultRelationalDataSource());
	}

	public Course(RelationalDataSource dataSource) throws JDOException {
		super(dataSource, TABLE_NAME);
	}

	public int getId() {
		return getInt("id_course");
	}

	public void setId(final int id) {
		put("id_course", id);
	}

	public String getCode() {
		return getString("code");
	}

	public void setCode(final String courseCode) {
		put("code", courseCode);
	}

	public String getCourseName() {
		return getString("nm_course");
	}

	public void setCourseName(final String courseName) {
		put("nm_course", courseName);
	}

	public Calendar getStartDate() {
		return getCalendar("dt_start");
	}

	public void setStartDate(final Calendar dtStart) {
		put("dt_start", dtStart);
	}
	
	public void setStartDate(final String yyyyMMdd) throws ParseException {
		SimpleDateFormat dtFormat = new SimpleDateFormat("yyyy-MM-dd");
		Calendar cal = new GregorianCalendar();
		cal.setTime(dtFormat.parse(yyyyMMdd));
		setStartDate(cal);
	}

	public Calendar getEndDate() {
		return getCalendar("dt_end");
	}

	public void setEndDate(final Calendar dtEnd) {
		put("dt_end", dtEnd);
	}
	
	public void setEndDate(final String yyyyMMdd) throws ParseException {
		SimpleDateFormat dtFormat = new SimpleDateFormat("yyyy-MM-dd");
		Calendar cal = new GregorianCalendar();
		cal.setTime(dtFormat.parse(yyyyMMdd));
		setEndDate(cal);
	}

	public BigDecimal getPrice() {
		return getDecimal("price");
	}

	public void setPrice(final BigDecimal price) {
		put("price", price);
	}
	
	public String getDescription() {
		return getString("description");
	}

	public void setDescription(final String description) {
		put("description", description);
	}

}
