package org.judal.jdbc.test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Map;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;

import org.judal.jdbc.JDBCEngine;
import org.judal.jdbc.JDBCTableDataSource;
import org.judal.jdbc.metadata.SQLFunctions;

import javax.jdo.JDOException;

import org.judal.storage.java.test.AbstractTableTest;
import org.judal.storage.java.test.ArrayRecord1;
import org.judal.storage.java.test.ArrayRecord2;
import org.judal.storage.java.test.MapRecord1;
import org.judal.storage.table.ColumnGroup;
import org.judal.storage.table.Record;
import org.judal.storage.table.RecordSet;
import org.judal.storage.table.Table;
import org.judal.storage.table.TableDataSource;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Ignore;
import static org.junit.Assert.assertTrue;

public class TestJDBCAsTable extends AbstractTableTest {

	public TestJDBCAsTable() {
		super(ArrayRecord1.class, ArrayRecord2.class);
	}

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
	
	@Override
	public JDBCTableDataSource getTableDataSource() throws JDOException {
		return (JDBCTableDataSource) dts;
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

	@Ignore
	public void test04IndexedRecordset() throws JDOException, IOException, SecurityException, IllegalStateException, NotSupportedException, SystemException, RollbackException, HeuristicMixedException, HeuristicRollbackException, InstantiationException, IllegalAccessException {
		super.test03Recordset();
	}

	@Ignore
	public void test06SQLFunctions() throws JDOException, IOException, SecurityException, IllegalStateException, NotSupportedException, SystemException, RollbackException, HeuristicMixedException, HeuristicRollbackException, InstantiationException, IllegalAccessException {
		SQLFunctions f = ((JDBCTableDataSource) dts).Functions;
		createTable1(dts);
		createRecords1(dts);
		Record rec = MapRecord1.class.newInstance();
		ColumnGroup grp = new ColumnGroup(rec);
		grp.addMember(f.strCat(new String[]{"name","description"},' ')+" AS DESCNAME");
		grp.addMember(f.ISNULL+"(location,'no location') AS NULLOCAT");
		Table tbl = dts.openTable(rec);
		RecordSet<MapRecord1> rst = tbl.fetch(grp, "amount", new BigDecimal("0"));
		for (Record res : rst) {
			assertTrue(!res.apply("DESCNAME").equals("Paul Browm") || res.apply("NULLOCAT").equals("There"));
			assertTrue(!res.apply("DESCNAME").equals("Peter Scott") || res.apply("NULLOCAT").equals("Overthere"));
			assertTrue(!res.apply("DESCNAME").equals("Adam Nichols") || res.apply("NULLOCAT").equals("Round the Corner"));
		}
		tbl.close();
		dts.dropTable(MapRecord1.tableName, false);
	}
	
}
