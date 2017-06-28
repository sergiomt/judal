package org.judal.examples.java.model.pojo;

import java.util.Date;

import javax.jdo.JDOException;

import org.judal.storage.EngineFactory;
import org.judal.storage.java.PojoRecord;
import org.judal.storage.relational.RelationalDataSource;

/**
 * Extend PojoRecord in order to create model classes manageable by JUDAL.
 * Add your getters and setters for database fields.
 */
public class StudentDocument extends PojoRecord {

	private static final long serialVersionUID = 1L;
	
	public static final String TABLE_NAME = "student_document";

	private int id_document;
	private int id_student;
	private Date dt_created;
	private String tl_document;
	private String tp_document;
	private String path_document;
	
	public StudentDocument() throws JDOException {
		this(EngineFactory.getDefaultRelationalDataSource());
	}

	public StudentDocument(RelationalDataSource dataSource) throws JDOException {
		super(dataSource, TABLE_NAME);
	}

	public int getStudentId() {
		return id_student;
	}

	public void setStudentId(final int id) {
		id_student = id;
	}
	
	public int getDocumentId() {
		return id_document;
	}

	public void setDocumentId(final int id) {
		id_document = id;
	}

	public String getTitle() {
		return tl_document;
	}

	public void setTitle(final String title) {
		tl_document = title;
	}

	public String getDocType() {
		return tp_document;
	}

	public void setDocType(final String docType) {
		tp_document = docType;
	}

	public String getPathDoc() {
		return path_document;
	}

	public void setPathDoc(final String pathDoc) {
		path_document = pathDoc;
	}
}
