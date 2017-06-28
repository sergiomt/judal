package org.judal.examples.java.model.array;

import javax.jdo.JDOException;

import org.judal.storage.EngineFactory;
import org.judal.storage.java.ArrayRecord;
import org.judal.storage.relational.RelationalDataSource;

/**
 * Extend ArrayRecord in order to create model classes manageable by JUDAL.
 * Add your getters and setters for database fields.
 */
public class StudentDocument extends ArrayRecord {

	private static final long serialVersionUID = 1L;
	
	public static final String TABLE_NAME = "student_document";
	
	public StudentDocument() throws JDOException {
		this(EngineFactory.getDefaultRelationalDataSource());
	}

	public StudentDocument(RelationalDataSource dataSource) throws JDOException {
		super(dataSource, TABLE_NAME);
	}

}
