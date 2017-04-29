package org.judal.storage.java.test;

import java.util.Date;

import org.judal.storage.table.Record;

public interface TestRecord2 extends Record {

	String getCode();

	void setCode(String n);

	Date getCreated();

	void setCreated(long timeMilis);

	String getName();

	void setName(String n);

	String getDescription();

	void setDescription(String d);
	
}
