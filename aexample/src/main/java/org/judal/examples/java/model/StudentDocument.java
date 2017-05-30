package org.judal.examples.java.model;

import javax.jdo.JDOException;

import org.judal.storage.EngineFactory;
import org.judal.storage.java.MapRecord;
import org.judal.storage.relational.RelationalDataSource;

public class StudentDocument extends MapRecord {

	private static final long serialVersionUID = 1L;
	
	public static final String TABLE_NAME = "student_document";
	
	public StudentDocument() throws JDOException {
		this(EngineFactory.getDefaultRelationalDataSource());
	}

	public StudentDocument(RelationalDataSource dataSource) throws JDOException {
		super(dataSource, TABLE_NAME);
	}

}
