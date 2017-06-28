package org.judal.examples.java.model.array;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;

import javax.jdo.JDOException;

import org.judal.storage.DataSource;
import org.judal.storage.EngineFactory;
import org.judal.storage.java.ArrayRecord;
import org.judal.storage.relational.RelationalDataSource;

/**
 * Extend ArrayRecord in order to create model classes manageable by JUDAL.
 * Add your getters and setters for database fields.
 */
public class Student extends ArrayRecord {

	private static final long serialVersionUID = 1L;

	public static final String TABLE_NAME = "student";
	
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
		return getInt("id_student");
	}

	public void setId(final int id) {
		put("id_student", id);
	}

	public String getFirstName() {
		return getString("first_name");
	}

	public void setFirstName(final String firstName) {
		put("first_name", firstName);
	}

	public String getLastName() {
		return getString("last_name");
	}

	public void setLastName(final String lastName) {
		put("last_name", lastName);
	}

	public Calendar getDateOfBirth() {
		return getCalendar("date_of_birth");
	}

	public void setDateOfBirth(final Calendar dob) {
		put("date_of_birth", dob);
	}
	
	public void setDateOfBirth(final String yyyyMMdd) throws ParseException {
		SimpleDateFormat dobFormat = new SimpleDateFormat("yyyy-MM-dd");
		Calendar cal = new GregorianCalendar();
		cal.setTime(dobFormat.parse(yyyyMMdd));
		setDateOfBirth(cal);
	}

	public byte[] getPhoto() {
		return getBytes("photo");
	}

	public void setPhoto(final byte[] photoData) {
		put("photo", photoData);
	}

}
