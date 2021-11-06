package org.judal.bdbj.test;

import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.AfterClass;

import java.io.IOException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import javax.jdo.JDOException;
import javax.transaction.SystemException;

import org.judal.bdbj.DBJBucketDataSource;
import org.judal.bdbj.DBJDataSource;
import org.judal.bdbj.DBJEngine;
import org.judal.storage.java.test.AbstractBucketTest;
import org.judal.storage.keyvalue.Stored;
import org.judal.serialization.BytesConverter;

public class TestBDBJAsBucket extends AbstractBucketTest {

	private static Map<String,String> properties;
	private static DBJDataSource dts;

	@BeforeClass
	public static void init() throws ClassNotFoundException, JDOException, IOException {
		properties = new TestBDBJ().getTestProperties();
		System.out.println(System.getProperty("java.library.path"));
		DBJEngine dbe = new DBJEngine();
		dts = dbe.getDataSource(properties);
	}

	@AfterClass
	public static void cleanup() throws JDOException {
		if (dts!=null) dts.close();
	}

	@Test
	public void test01Bucket() throws JDOException, IOException, SystemException {
		super.test01Bucket();
	}
	
	@Override
	public DBJBucketDataSource getBucketDataSource() throws JDOException {
		return (DBJBucketDataSource) dts;
	}

	@Override
	public Stored getByteStored1() throws JDOException {
		Stored obj = new TestStored(bucketName);
		obj.setKey(pk);
		obj.setValue(new byte[]{1,2,3,4,5,6,7,8,9});
		return obj;
	}

	@Override
	public Stored getObjectStored2() throws JDOException {
		Stored obj = new TestStored(bucketName);
		obj.setKey(pk);
		HashMap<String,String> map = new HashMap<String,String>();
		map.put("1", "one");
		map.put("2", "two");
		map.put("3", "three");
		obj.setValue(BytesConverter.toBytes(map,Types.JAVA_OBJECT));
		return obj;
	}

	@Override
	public Stored getRetrieved() throws JDOException {
		return new TestStored(bucketName);
	}
}
