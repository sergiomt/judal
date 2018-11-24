package org.judal.hbase.test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.jdo.JDOException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.judal.hbase.HBEngine;
import org.judal.hbase.HBTableDataSource;
import org.judal.storage.DataSource;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Ignore;
import static org.junit.Assert.assertNotNull;

public class TestHBaseConnectivity {

	private Map<String,String> properties;
	private HBTableDataSource dts;

	// @Before
	public void setUp() throws ClassNotFoundException, JDOException, IOException {
		properties = new TestHBase().getTestProperties();
		System.out.println("config="+properties.get(DataSource.CONFIG));
		HBEngine eng = new HBEngine();
		dts = eng.getDataSource(properties);
	}

	// @After
	public void tearDown() {
		if (null!=dts)
			dts.close();
	}

	@Test
	public void testVoid() {
	}

	@Ignore
	public void testClusterConnection() throws IOException {
		try (ClusterConnection conn = (ClusterConnection) ConnectionFactory.createConnection(dts.getConfig())) {
			Admin hadmin = conn.getAdmin();
			System.out.println("got Admin");
			assertNotNull(hadmin);
		}
	}

	@Ignore
	public void testListTableDescriptors() throws IOException {
		try (ClusterConnection conn = (ClusterConnection) ConnectionFactory.createConnection(dts.getConfig())) {
			Admin hadmin = conn.getAdmin();
			System.out.println("got Admin");
			List<TableDescriptor> descriptors = hadmin.listTableDescriptors();
		}
	}
}
