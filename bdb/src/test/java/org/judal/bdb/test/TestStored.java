package org.judal.bdb.test;

import org.judal.bdb.DBBucketDataSource;
import org.judal.bdb.DBStored;
import javax.jdo.JDOException;

public class TestStored extends DBStored {

	private static final long serialVersionUID = 1L;

	public TestStored(String bucketName) throws JDOException {
		super(DBBucketDataSource.getKeyValueDef(bucketName));
	}

}
