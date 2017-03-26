package org.judal.bdb.test;

import java.io.File;
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

import com.knowgate.io.FileUtils;

import org.junit.Ignore;
import org.judal.storage.DataSource;
import org.judal.storage.java.test.AbstractTableTest;
import org.judal.storage.java.test.MapRecord1;
import org.judal.storage.java.test.MapRecord2;
import org.judal.metadata.SchemaMetaData;
import org.judal.transaction.DataSourceTransactionManager;

import org.judal.bdb.DBTableDataSource;

public class TestBDBAsTable extends AbstractTableTest {

	public TestBDBAsTable() {
		super(MapRecord1.class, MapRecord2.class);
	}

	private static Map<String,String> properties;
	private DBTableDataSource dts;
	private static SchemaMetaData metaData;

	@BeforeClass
	public static void init() throws ClassNotFoundException, JDOException, IOException {
		System.out.println("Before TestBDBAsTable");
		properties = new TestBDB().getTestProperties();
		metaData = new SchemaMetaData();
		metaData.addTable(MapRecord1.getTableDef(null));
		metaData.addTable(MapRecord2.getTableDef(null));
	}

	@AfterClass
	public static void cleanup() throws JDOException, IOException {
		File env = new File(properties.get(DataSource.DBENV));
		if (env.exists())
			FileUtils.deleteDirectory(env);
	}

	@Override
	public DBTableDataSource getTableDataSource() throws JDOException {
		return dts;
	}

	@Ignore
	public void test00Pks() throws JDOException, IOException, InstantiationException, IllegalAccessException {
		File directory = new File(properties.get(DataSource.DBENV));
		if (directory.exists())
			FileUtils.deleteDirectory(directory);
		FileUtils.forceMkdir(directory);
		dts = new DBTableDataSource(properties, null, metaData);
		super.test00Pks();
		if (dts!=null) {
			dts.close();
			dts=null;
		}
	}

	@Ignore
	public void test01Table() throws JDOException, IOException, InstantiationException, IllegalAccessException, SystemException {		
		File directory = new File(properties.get(DataSource.DBENV));
		if (directory.exists())
			FileUtils.deleteDirectory(directory);
		FileUtils.forceMkdir(directory);
		dts = new DBTableDataSource(properties, null, metaData);
		super.test01Table();
		if (dts!=null) {
			dts.close();
			dts=null;
		}
	}

	@Test
	public void test02Transaction() throws JDOException, IOException, SecurityException, IllegalStateException, NotSupportedException, SystemException, RollbackException, HeuristicMixedException, HeuristicRollbackException, InstantiationException, IllegalAccessException {
		File directory = new File(properties.get(DataSource.DBENV));
		if (directory.exists())
			FileUtils.deleteDirectory(directory);
		FileUtils.forceMkdir(directory);
		dts = new DBTableDataSource(properties, DataSourceTransactionManager.Transact, metaData);
		super.test02Transaction();
		if (dts!=null) {
			dts.close();
			dts=null;
		}
	}

	@Ignore
	public void test03Recordset() throws JDOException, IOException, SecurityException, IllegalStateException, NotSupportedException, SystemException, RollbackException, HeuristicMixedException, HeuristicRollbackException, InstantiationException, IllegalAccessException {
		File directory = new File(properties.get(DataSource.DBENV));
		if (directory.exists())
			FileUtils.deleteDirectory(directory);
		FileUtils.forceMkdir(directory);
		dts = new DBTableDataSource(properties, null, metaData);
		super.test03Recordset();
		if (dts!=null) {
			dts.close();
			dts=null;
		}
	}

	@Ignore
	public void test05Metadata() throws JDOException, IOException, InstantiationException, IllegalAccessException {
		File directory = new File(properties.get(DataSource.DBENV));
		if (directory.exists())
			FileUtils.deleteDirectory(directory);
		FileUtils.forceMkdir(directory);
		dts = new DBTableDataSource(properties, null, metaData);
		super.test05Metadata("org/judal/bdb/test", "metadata.xml");
		if (dts!=null) {
			dts.close();
			dts=null;
		}
	}
	
}
