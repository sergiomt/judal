package org.judal.cassandra.test;

import java.io.IOException;
import java.util.Map;

import javax.jdo.JDOException;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Ignore;

import org.judal.cassandra.CSEngine;
import org.judal.cassandra.KeySpace;

import org.judal.storage.java.test.MapRecord1;
import org.judal.storage.java.test.MapRecord2;
import org.judal.storage.java.test.AbstractTableTest;

public class TestCassandraAsTable extends AbstractTableTest {

	public TestCassandraAsTable() {
		super(MapRecord1.class, MapRecord2.class);
	}

	private static Map<String,String> properties;
	private static KeySpace dts;

	@BeforeClass
	public static void init() throws ClassNotFoundException, JDOException, IOException {
		System.out.println("Before TestBDBAsTable");
		properties = new TestCassandra().getTestProperties();
		CSEngine eng = new CSEngine();
		dts = eng.getDataSource(properties);
	}

	@AfterClass
	public static void cleanup() throws JDOException {
		if (dts!=null) {
			dts.close();
		}
	}
	
	@Override
	public KeySpace getTableDataSource() throws JDOException {
		return dts;
	}

	@Ignore
	public void test00Pks() throws JDOException, IOException, InstantiationException, IllegalAccessException {
		super.test00Pks();
	}

	@Ignore
	public void test01Table() throws JDOException, IOException, InstantiationException, IllegalAccessException, SystemException {		
		super.test01Table();
	}

	@Ignore
	public void test02Transaction() throws JDOException, IOException, SecurityException, IllegalStateException, NotSupportedException, SystemException, RollbackException, HeuristicMixedException, HeuristicRollbackException, InstantiationException, IllegalAccessException {
		super.test02Transaction();
	}

	@Ignore
	public void test03Recordset() throws JDOException, IOException, SecurityException, IllegalStateException, NotSupportedException, SystemException, RollbackException, HeuristicMixedException, HeuristicRollbackException, InstantiationException, IllegalAccessException {
		super.test03Recordset();
	}

	@Test
	public void test05Metadata() throws JDOException, IOException, InstantiationException, IllegalAccessException {
		super.test05Metadata("org/judal/cassandra/test", "metadata.xml");
	}
	
}
