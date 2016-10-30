package org.judal.s3.test;

import org.junit.Test;
import org.junit.Ignore;
import org.junit.BeforeClass;
import org.junit.AfterClass;

import java.io.IOException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import org.judal.storage.DataSource;
import javax.jdo.JDOException;
import javax.transaction.SystemException;

import org.judal.storage.Stored;
import org.judal.storage.java.test.AbstractBucketTest;
import org.judal.serialization.BytesConverter;

import org.judal.s3.S3Engine;
import org.judal.s3.S3Record;
import org.judal.s3.S3DataSource;

public class TestS3AsBucket extends AbstractBucketTest {

	private static Map<String,String> properties;
	private static S3DataSource dts;
	private static String defaultBucketName;
	
	@BeforeClass
	public static void init() throws ClassNotFoundException, JDOException, IOException {
		properties = new TestS3().getTestProperties();
		System.out.println("Properties");
		for (String k : properties.keySet())
			System.out.println(k+"="+properties.get(k));
		S3Engine s3 = new S3Engine();
		dts = s3.getDataSource(properties);
		defaultBucketName = bucketName;
		bucketName = properties.getOrDefault(DataSource.URI, "judaltest1");
	}

	@AfterClass
	public static void cleanup() throws JDOException {
		if (dts!=null) dts.close();
		bucketName = defaultBucketName;
	}

	@Test
	public void test01Bucket() throws JDOException, IOException, SystemException {
		super.test01Bucket();
	}
	
	@Override
	public S3DataSource getBucketDataSource() throws JDOException {
		return dts;
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
		S3Record obj = new TestStored(bucketName);
		obj.setKey(pk);
		HashMap<String,String> map = new HashMap<String,String>();
		map.put("1", "one");
		map.put("2", "two");
		map.put("3", "three");
		obj.put("column1", "value1");
		obj.put("column2", "value2");
		obj.put("column3", "value3");
		obj.setValue(BytesConverter.toBytes(map,Types.JAVA_OBJECT));
		return obj;
	}

	@Override
	public Stored getRetrieved() throws JDOException {
		return new TestStored(bucketName);
	}
	
}
