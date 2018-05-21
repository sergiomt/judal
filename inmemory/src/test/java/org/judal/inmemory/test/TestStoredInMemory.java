package org.judal.inmemory.test;

import org.judal.inmemory.InMemoryRecord;
import javax.jdo.JDOException;

@SuppressWarnings("serial")
public class TestStoredInMemory extends InMemoryRecord {

	public TestStoredInMemory(String tableName) throws JDOException {
		super(tableName, "column1", "column2", "column3");
	}

}
