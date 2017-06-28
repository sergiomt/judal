package org.judal.examples.java.model.pojo;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import javax.jdo.JDOException;

import org.judal.storage.EngineFactory;
import org.judal.storage.java.PojoRecord;
import org.judal.storage.relational.RelationalDataSource;

/**
 * Extend PojoRecord in order to create model classes manageable by JUDAL.
 * Add your getters and setters for database fields.
 */
public class Course extends PojoRecord {

	private static final long serialVersionUID = 1L;
	
	public static final String TABLE_NAME = "course";

	private int id_course;
	private String code;
	private String nm_course;
	@SuppressWarnings("unused")
	private Date dt_created;
	private Calendar dt_start;
	private Calendar dt_end;
	private BigDecimal price;
	private String description;

	public Course() throws JDOException {
		this(EngineFactory.getDefaultRelationalDataSource());
	}

	public Course(RelationalDataSource dataSource) throws JDOException {
		super(dataSource, TABLE_NAME);
	}

	public int getId() {
		return id_course;
	}

	public void setId(final int id) {
		id_course = id;
	}

	public String getCode() {
		return code;
	}

	public void setCode(final String courseCode) {
		code = courseCode;
	}

	public String getCourseName() {
		return nm_course;
	}

	public void setCourseName(final String courseName) {
		nm_course = courseName;
	}

	public Calendar getStartDate() {
		return dt_start;
	}

	public void setStartDate(final Calendar dtStart) {
		dt_start = dtStart;
	}
	
	public void setStartDate(final String yyyyMMdd) throws ParseException {
		SimpleDateFormat dtFormat = new SimpleDateFormat("yyyy-MM-dd");
		Calendar cal = new GregorianCalendar();
		cal.setTime(dtFormat.parse(yyyyMMdd));
		setStartDate(cal);
	}

	public Calendar getEndDate() {
		return dt_end;
	}

	public void setEndDate(final Calendar dtEnd) {
		dt_end = dtEnd;
	}
	
	public void setEndDate(final String yyyyMMdd) throws ParseException {
		SimpleDateFormat dtFormat = new SimpleDateFormat("yyyy-MM-dd");
		Calendar cal = new GregorianCalendar();
		cal.setTime(dtFormat.parse(yyyyMMdd));
		setEndDate(cal);
	}

	public BigDecimal getPrice() {
		return price;
	}

	public void setPrice(final BigDecimal coursePrice) {
		this.price = coursePrice;
	}
	
	public String getDescription() {
		return description;
	}

	public void setDescription(final String courseDesc) {
		description = courseDesc;
	}

}
