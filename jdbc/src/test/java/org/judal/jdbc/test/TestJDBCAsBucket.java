package org.judal.jdbc.test;

import org.junit.Test;
import org.junit.Ignore;
import org.junit.BeforeClass;
import org.junit.AfterClass;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import org.judal.jdbc.JDBCBucketDataSource;
import org.judal.jdbc.JDBCEngine;
import org.judal.jdbc.JDBCTableDataSource;
import org.judal.storage.TableDataSource;
import org.judal.storage.DataSource;
import javax.jdo.JDOException;
import javax.transaction.SystemException;

import org.judal.storage.Stored;
import org.judal.storage.java.test.AbstractBucketTest;
import org.judal.serialization.BytesConverter;

public class TestJDBCAsBucket extends AbstractBucketTest {

	private static Map<String,String> properties;
	private static TableDataSource dts;

	@BeforeClass
	public static void init() throws ClassNotFoundException, JDOException, IOException {
		properties = new TestJDBC().getTestProperties();
		JDBCEngine jdbc = new JDBCEngine();
		dts = jdbc.getDataSource(properties);
	}

	@AfterClass
	public static void cleanup() throws JDOException {
		if (dts!=null) dts.close();
	}

	@Test
	public void test00Driver() throws ClassNotFoundException, SQLException  {
		Class.forName(properties.get(DataSource.DRIVER));
		Connection conn = DriverManager.getConnection(properties.get(DataSource.URI), properties.get(DataSource.USER), properties.get(DataSource.PASSWORD));
		System.out.println("\""+conn.getMetaData().getDatabaseProductName()+"\"");
		conn.close();
	}
	
	@Test
	public void test01Bucket() throws JDOException, IOException, SystemException {
		super.test01Bucket();
	}
	
	@Override
	public JDBCBucketDataSource getBucketDataSource() throws JDOException {
		return (JDBCBucketDataSource) dts;
	}

	@Override
	public Stored getByteStored1() throws JDOException {
		Stored obj = new TestStored(dts, bucketName);
		obj.setKey(pk);
		obj.setValue(new byte[]{1,2,3,4,5,6,7,8,9});
		return obj;
	}

	@Override
	public Stored getObjectStored2() throws JDOException {
		Stored obj = new TestStored(dts, bucketName);
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
		return new TestStored(dts, bucketName);
	}
	
}
