package org.judal.examples.java.model.pojo;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import javax.jdo.JDOException;

import org.judal.storage.DataSource;
import org.judal.storage.EngineFactory;
import org.judal.storage.java.PojoRecord;
import org.judal.storage.relational.RelationalDataSource;

/**
 * Extend PojoRecord in order to create model classes manageable by JUDAL.
 * Add your getters and setters for database fields.
 */
public class Student extends PojoRecord {

	private static final long serialVersionUID = 1L;

	public static final String TABLE_NAME = "student";
	
	private int id_student;
	private String first_name;
	private String last_name;
	@SuppressWarnings("unused")
	private Date dt_created;
	private Calendar date_of_birth;
	private byte[] photo;
	
	public Student() throws JDOException {
		this(EngineFactory.getDefaultRelationalDataSource());
	}

	public Student(RelationalDataSource dataSource) throws JDOException {
		super(dataSource, TABLE_NAME);
	}

	@Override
	public void store(DataSource dts) throws JDOException {
		// Generate the student Id. from a sequence if it is not provided
		if (isNull("id_student"))
			setId ((int) dts.getSequence("seq_student").nextValue());
		super.store(dts);
	}
	
	public int getId() {
		return id_student;
	}

	public void setId(final int id) {
		id_student = id;
	}

	public String getFirstName() {
		return first_name;
	}

	public void setFirstName(final String firstName) {
		first_name = firstName;
	}

	public String getLastName() {
		return last_name;
	}

	public void setLastName(final String lastName) {
		last_name = lastName;
	}

	public Calendar getDateOfBirth() {
		return date_of_birth;
	}

	public void setDateOfBirth(final Calendar dob) {
		date_of_birth = dob;
	}
	
	public void setDateOfBirth(final String yyyyMMdd) throws ParseException {
		SimpleDateFormat dobFormat = new SimpleDateFormat("yyyy-MM-dd");
		Calendar cal = new GregorianCalendar();
		cal.setTime(dobFormat.parse(yyyyMMdd));
		setDateOfBirth(cal);
	}

	public byte[] getPhoto() {
		return photo;
	}

	public void setPhoto(final byte[] photoData) {
		photo = photoData;
	}

}
