package org.judal.benchmark.java.model;

import javax.jdo.JDOException;

import org.judal.benchmark.java.MediumRecordData;
import org.judal.storage.EngineFactory;
import org.judal.storage.java.MapRecord;

public class MediumRecordMap extends MapRecord {

	public MediumRecordMap() throws JDOException {
		super(EngineFactory.getDefaultRelationalDataSource(), MediumRecordData.TABLE_NAME);
	}

	private static final long serialVersionUID = 1L;


}