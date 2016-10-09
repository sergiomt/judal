package org.judal.jdbc.test;

import javax.jdo.JDOException;
import org.judal.storage.TableDataSource;
import org.judal.storage.java.ArrayRecord;

@SuppressWarnings("serial")
public class TestStored extends ArrayRecord {

	public TestStored(TableDataSource dataSource, String tableName) throws JDOException {
		super(dataSource, tableName);
	}

}
