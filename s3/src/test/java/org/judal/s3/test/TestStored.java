package org.judal.s3.test;

import org.judal.s3.S3Record;
import javax.jdo.JDOException;

@SuppressWarnings("serial")
public class TestStored extends S3Record {

	public TestStored(String tableName) throws JDOException {
		super(tableName, "column1", "column2", "column3");
	}

}
