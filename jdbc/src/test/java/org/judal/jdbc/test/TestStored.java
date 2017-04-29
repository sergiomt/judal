package org.judal.jdbc.test;

import javax.jdo.JDOException;

import org.judal.storage.java.ArrayRecord;
import org.judal.storage.table.TableDataSource;

@SuppressWarnings("serial")
public class TestStored extends ArrayRecord {

	public TestStored(TableDataSource dataSource, String tableName) throws JDOException {
		super(dataSource, tableName);
	}

}
