package org.judal.bdbj.test;

import org.judal.bdbj.DBJBucketDataSource;
import org.judal.bdbj.DBJStored;

import javax.jdo.JDOException;

public class TestStored extends DBJStored {

	private static final long serialVersionUID = 1L;

	public TestStored(String bucketName) throws JDOException {
		super(DBJBucketDataSource.getKeyValueDef(bucketName));
	}

}
