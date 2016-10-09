package org.judal.storage.test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.sql.Timestamp;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;

import org.judal.metadata.ColumnDef;
import org.judal.metadata.SchemaMetaData;
import org.judal.metadata.TableDef;
import org.judal.metadata.bind.JdoPackageMetadata;
import org.judal.storage.IndexableView;
import org.judal.storage.Param;
import org.judal.storage.Record;
import org.judal.storage.RecordSet;

import javax.jdo.FetchGroup;
import javax.jdo.JDOException;
import org.judal.storage.Table;
import org.judal.storage.TableDataSource;
import org.judal.storage.View;
import org.judal.storage.query.AbstractQuery;
import org.judal.storage.query.Connective;
import org.judal.storage.query.Operator;
import org.judal.storage.query.Predicate;

public abstract class AbstractTableTest {
	
	public Class<? extends TestRecord1> recordClass1;
	public Class<? extends TestRecord2> recordClass2;

	public AbstractTableTest(Class<? extends TestRecord1> recordClass1, Class<? extends TestRecord2> recordClass2) {
		this.recordClass1 = recordClass1;
		this.recordClass2 = recordClass2;
	}

	public String statusString(int status) {
		switch (status) {
		case Status.STATUS_NO_TRANSACTION:
			return "no transaction";
		case Status.STATUS_ACTIVE:
			return "active";
		case Status.STATUS_PREPARING:
			return "preparing";
		case Status.STATUS_PREPARED:
			return "prepared";
		case Status.STATUS_COMMITTING:
			return "committing";
		case Status.STATUS_COMMITTED:
			return "commited";
		case Status.STATUS_MARKED_ROLLBACK:
			return "marked rollback";
		case Status.STATUS_ROLLEDBACK:
			return "rolledback";
		case Status.STATUS_ROLLING_BACK:
			return "rolling back";
		default:
			return "unknown";
		}
	}

	public abstract TableDataSource getTableDataSource() throws JDOException;
	
	public void createTable1(TableDataSource ds) throws JDOException {
		System.out.println("Creating table "+ArrayRecord1.getTableDef(ds).getName());
		ds.createTable(ArrayRecord1.getTableDef(ds), null);
	}

	public void createTable2(TableDataSource ds) throws JDOException {
		System.out.println("Creating table "+ArrayRecord2.getTableDef(ds).getName());
		ds.createTable(ArrayRecord2.getTableDef(ds), null);
	}

	public void createRecords1(TableDataSource ds) throws JDOException, InstantiationException, IllegalAccessException {
		Table tb1 = null;
		TestRecord1 rec1, rec2, rec3, rec4, rec5;
		System.out.println("Creating records");

		rec1 = recordClass1.newInstance();
		rec1.setId(new Integer(1));
		rec1.setCreated(System.currentTimeMillis());
		rec1.setName("John Smith");
		rec1.setLocation("Here");
		rec1.setImage(new byte[]{1,2,3,4,5,6,7,8,9});
		rec1.setAmount(new BigDecimal("334267.87"));
		
		rec2 = recordClass1.newInstance();
		rec2.setId(new Integer(2));
		rec2.setCreated(System.currentTimeMillis());
		rec2.setName("Paul Browm");
		rec2.setLocation("There");
		rec2.setAmount(new BigDecimal("0"));
		
		rec3 = recordClass1.newInstance();
		rec3.setId(new Integer(3));
		rec3.setCreated(System.currentTimeMillis());
		rec3.setName("Peter Scott");
		rec3.setLocation("Overthere");
		rec3.setAmount(new BigDecimal("0"));

		rec4 = recordClass1.newInstance();
		rec4.setId(new Integer(4));
		rec4.setCreated(System.currentTimeMillis());
		rec4.setName("Adam Nichols");
		rec4.setLocation("Round the Corner");
		rec4.setAmount(new BigDecimal("0"));

		rec5 = recordClass1.newInstance();
		rec5.setId(new Integer(5));
		rec5.setCreated(System.currentTimeMillis());
		rec5.setName("Martin Ady");
		rec5.setLocation("Back of Beyond");
		rec5.setAmount(new BigDecimal("-101"));

		System.out.println("Opening table "+ArrayRecord1.tableName);

		tb1 = ds.openIndexedTable(recordClass1.newInstance());
		tb1.store(rec1);
		tb1.store(rec2);
		tb1.store(rec3);
		tb1.store(rec4);
		tb1.store(rec5);
		System.out.println("Closing table "+ArrayRecord1.tableName);
		tb1.close();
		
		System.out.println("Records created");
		
		tb1 = ds.openIndexedTable(recordClass1.newInstance());
		rec1 = recordClass1.newInstance();
		System.out.println("check exists 1");
		assertTrue(tb1.exists(new Param[]{new Param("id", 1, new Integer(1))}));
		System.out.println("check load 1");
		assertTrue(tb1.load(new Integer(1), rec1));
		System.out.println("check name 1 is John Smith");
		assertEquals("John Smith", rec1.getName());
		rec2 = recordClass1.newInstance();
		System.out.println("check exists 2");
		assertTrue(tb1.exists(new Param[]{new Param("id", 1, new Integer(2))}));
		System.out.println("check load 2");
		assertTrue(tb1.load(new Integer(2), rec1));
		System.out.println("check name 2 is Paul Browm");
		assertEquals("Paul Browm", rec1.getName());
		tb1.close();
	}

	public void test00Pks() throws JDOException, IOException, InstantiationException, IllegalAccessException {
		TableDataSource ds = getTableDataSource();
		assertEquals("id", ArrayRecord1.getTableDef(ds).getPrimaryKeyMetadata().getColumn());
		assertEquals("code", ArrayRecord2.getTableDef(ds).getPrimaryKeyMetadata().getColumn());
		assertEquals("id", MapRecord1.getTableDef(ds).getPrimaryKeyMetadata().getColumn());
		assertEquals("code", MapRecord2.getTableDef(ds).getPrimaryKeyMetadata().getColumn());
	}

	public void test01Table() throws JDOException, IOException, InstantiationException, IllegalAccessException, SystemException {
		System.out.println("Begin test01Table()");

		TableDataSource ds = getTableDataSource();

		System.out.println("Transaction status after getting table data source = "+ statusString(ds.getTransactionManager().getStatus()));

		ArrayRecord1.dataSource = ds;
		ArrayRecord2.dataSource = ds;
		MapRecord1.dataSource = ds;
		MapRecord2.dataSource = ds;

		try {
			createTable1(ds);
			TestRecord1 rec = recordClass1.newInstance();
			rec.put("id", new Integer(1));
			rec.put("created", new Timestamp(System.currentTimeMillis()));
			rec.put("name", "John Smith");
			rec.put("location", "");
			rec.put("image", new byte[]{1,2,3,4,5,6,7,8,9});
			rec.put("amount", new BigDecimal("334267.87"));
			assertNull(rec.get("description"));
			assertTrue(rec.isNull("description"));
			assertTrue(rec.isEmpty("location"));
			Table tb = ds.openTable(rec);
			tb.store(rec);
			tb.close();
			
			TestRecord1 ret = recordClass1.newInstance();
			tb = ds.openTable(ret);
			boolean loaded = tb.load(new Integer(1), ret);
			tb.close();
			assertTrue(loaded);
			assertEquals(rec.getInt("id"), ret.getInt("id"));
			assertEquals(rec.getString("name"), ret.getString("name"));
			assertNull(ret.get("description"));
			assertTrue(ret.isNull("description"));
			assertEquals("", ret.getString("location"));
			assertTrue(ret.isEmpty("location"));
			assertArrayEquals(new byte[]{1,2,3,4,5,6,7,8,9}, ret.getBytes("image"));
			assertEquals(new BigDecimal("334267.87").toString(), ret.getDecimal("amount").toString());
		} finally {
			System.out.println("Transaction before dropping table = "+ statusString(ds.getTransactionManager().getStatus()));
			if (ds!=null) ds.dropTable(ArrayRecord1.tableName, false);
		}
		assertFalse(ds.exists(ArrayRecord1.tableName, "U"));
		System.out.println("End test01Table()");
	}

	public void test02Transaction() throws JDOException, IOException, NotSupportedException, SystemException, SecurityException, IllegalStateException, RollbackException, HeuristicMixedException, HeuristicRollbackException, InstantiationException, IllegalAccessException {

		System.out.println("Begin test02Transaction()");
		
		TableDataSource ds = getTableDataSource();
		
		ArrayRecord1.dataSource = ds;
		ArrayRecord2.dataSource = ds;
		MapRecord1.dataSource = ds;
		MapRecord2.dataSource = ds;

		Table tb1 = null;
		Table tb2 = null;
		boolean created1 = false;
		boolean created2 = false;
		try {
			createTable1(ds);
			created1 = true;
			createTable2(ds);
			created2 = true;

			TestRecord1 rec1 = recordClass1.newInstance();
			rec1.put("id", new Integer(1));
			rec1.put("created", new Timestamp(System.currentTimeMillis()));
			rec1.put("name", "John Smith");
			rec1.put("location", "");
			rec1.put("image", new byte[]{1,2,3,4,5,6,7,8,9});
			rec1.put("amount", new BigDecimal("334267.87"));
			
			TestRecord2 rec2 = recordClass2.newInstance();
			rec2.put("code", "1234");
			rec2.put("name", "John Smith");
			rec2.put("created", new Timestamp(System.currentTimeMillis()));
			rec2.put("description", "... ... ...");
			
			System.out.println("Transaction status before begin (1) = "+ statusString(getTableDataSource().getTransactionManager().getStatus()));
			getTableDataSource().getTransactionManager().begin();
			assertEquals(Status.STATUS_ACTIVE, ds.getTransactionManager().getStatus());
			tb1 = ds.openTable(rec1);
			tb2 = ds.openTable(rec2);
			tb1.store(rec1);
			tb2.store(rec2);
			System.out.println("Transaction status before commit = "+ statusString(ds.getTransactionManager().getStatus()));
			ds.getTransactionManager().commit();
			System.out.println("Transaction status after commit = "+ statusString(ds.getTransactionManager().getStatus()));
			tb2.close();
			System.out.println("After close table 2");
			tb1.close();
			System.out.println("After close table 1");
			
			tb1 = ds.openTable(rec1);
			System.out.println("After re-open table 1");
			assertTrue(tb1.exists(new Param("id",1,1)));
			assertTrue(tb1.load(new Integer(1), rec1));
			tb1.close();
			System.out.println("After close table 1");

			ds.truncateTable(ArrayRecord1.tableName, false);

			System.out.println("After truncate table 1");

			tb1 = ds.openTable(rec1);
			System.out.println("After re-open table 1");
			assertFalse(tb1.exists(new Param("id",1,1)));
			assertFalse(tb1.load(new Integer(1), rec1));
			tb1.close();
			System.out.println("After close table 1");
			
			tb2 = ds.openTable(rec2);
			System.out.println("After re-open table 2");
			assertTrue(tb2.exists(new Param("code",1,"1234")));
			assertTrue(tb2.load("1234", rec2));
			tb2.close();
			System.out.println("After close table 2");

			ds.truncateTable(ArrayRecord2.tableName, false);

			tb2 = ds.openTable(rec2);
			assertFalse(tb2.exists(new Param("code",1,"1234")));
			assertFalse(tb2.load("1234", rec2));
			tb2.close();
			
			rec1 = new ArrayRecord1();
			rec1.put("id", new Integer(1));
			rec1.put("created", new Timestamp(System.currentTimeMillis()));
			rec1.put("name", "John Smith");
			rec1.put("location", "");
			rec1.put("image", new byte[]{1,2,3,4,5,6,7,8,9});
			rec1.put("amount", new BigDecimal("334267.87"));
			
			rec2 = new ArrayRecord2();
			rec2.put("code", "1234");
			rec2.put("name", "John Smith");
			rec2.put("created", new Timestamp(System.currentTimeMillis()));
			rec2.put("description", "... ... ...");

			System.out.println("Transaction status before begin (2) = "+ statusString(getTableDataSource().getTransactionManager().getStatus()));
			ds.getTransactionManager().begin();
			System.out.println("Transaction status after begin (2) = "+ statusString(getTableDataSource().getTransactionManager().getStatus()));
			assertEquals(Status.STATUS_ACTIVE, ds.getTransactionManager().getStatus());
			tb1 = ds.openTable(rec1);
			tb2 = ds.openTable(rec2);
			tb1.store(rec1);
			tb2.store(rec2);
			System.out.println("Transaction status before rollback (2) = "+ statusString(ds.getTransactionManager().getStatus()));
			ds.getTransactionManager().rollback();
			System.out.println("Transaction status after rollback (2) = "+ statusString(ds.getTransactionManager().getStatus()));
			tb2.close();
			System.out.println("Closed 2");
			tb1.close();
			System.out.println("Closed 1");

			System.out.println("Reopen "+ ArrayRecord1.tableName);
			System.out.println("Transaction status before open table "+rec1.getTableName()+" = "+ statusString(getTableDataSource().getTransactionManager().getStatus()));
			tb1 = ds.openTable(rec1);
			assertFalse(tb1.exists(new Param("id",1,1)));
			assertFalse(tb1.load(new Integer(1), rec1));
			System.out.println("Reloaded  1");
			System.out.println("Close "+ ArrayRecord1.tableName);
			tb1.close();
			System.out.println("Before truncate"+ ArrayRecord1.tableName);
			ds.truncateTable(ArrayRecord1.tableName, false);
			System.out.println("After truncate"+ ArrayRecord1.tableName);
			
			System.out.println("Reopen "+ ArrayRecord2.tableName);
			tb2 = ds.openTable(rec2);
			assertFalse(tb2.exists(new Param("code",1,"1234")));
			assertFalse(tb2.load("1234", rec2));
			System.out.println("Reloaded  2");
			System.out.println("Close "+ ArrayRecord1.tableName);
			tb2.close();
			System.out.println("Before truncate"+ ArrayRecord1.tableName);
			ds.truncateTable(ArrayRecord2.tableName, false);
			System.out.println("After truncate"+ ArrayRecord2.tableName);

		} finally {
			if (ds!=null) {
				System.out.println("Transaction status while finalizing = "+ statusString(ds.getTransactionManager().getStatus()));
				try {
					if (ds.getTransactionManager().getStatus()!=Status.STATUS_NO_TRANSACTION &&
						ds.getTransactionManager().getStatus()!=Status.STATUS_COMMITTED &&
						ds.getTransactionManager().getStatus()!=Status.STATUS_ROLLEDBACK) {
						ds.getTransactionManager().getTransaction().rollback();
						System.out.println("Transaction status after finallyzing rollback() = "+ statusString(ds.getTransactionManager().getStatus()));
					}
				} finally { }
					if (created2) {
						System.out.println("Dropping table "+ArrayRecord2.tableName);
						ds.dropTable(ArrayRecord2.tableName, false);
					}
					if (created1) {
						System.out.println("Dropping table "+ArrayRecord1.tableName);
						ds.dropTable(ArrayRecord1.tableName, false);
					}
			}
		}
		assertFalse(ds.exists(ArrayRecord1.tableName, "U"));
		assertFalse(ds.exists(ArrayRecord2.tableName, "U"));
		System.out.println("End test02Transaction()");
	}

	public void test03Recordset() throws JDOException, IOException, SecurityException, IllegalStateException, NotSupportedException, SystemException, RollbackException, HeuristicMixedException, HeuristicRollbackException, InstantiationException, IllegalAccessException {
		System.out.println("Begin test03Recordset()");

		TableDataSource ds = getTableDataSource();
		
		ArrayRecord1.dataSource = ds;
		ArrayRecord2.dataSource = ds;
		MapRecord1.dataSource = ds;
		MapRecord2.dataSource = ds;

		View tb1 = null;
		Integer one = new Integer(1);

		try {
			
			createTable1(ds);

			createRecords1(ds);

			tb1 = ds.openIndexedView(recordClass1.newInstance());

			assertTrue(tb1.exists(one));

			assertTrue(tb1.load(one, recordClass1.newInstance()));

			FetchGroup group1 = new ArrayRecord1().fetchGroup();

			RecordSet<? extends Record> recs1 = tb1.fetch(group1, "id", one, 1, 0);

			System.out.println("Checking result set size 1 vs "+recs1.size());

			assertEquals(1, recs1.size());
			
			for (Record rec : recs1)
				assertEquals("John Smith", rec.get("name"));
			
			System.out.println("Checking skip 0 maxrows 3");
			BigDecimal zero = new BigDecimal("0");
			recs1 = tb1.fetch(group1, "amount", zero, 3, 0);
			System.out.println("Checking result set size 3 vs "+recs1.size());
			assertEquals(3, recs1.size());
			System.out.println("Checking skip 0 maxrows 2");
			recs1 = tb1.fetch(group1, "amount", zero, 2, 0);
			System.out.println("Checking result set size 2 vs "+recs1.size());
			assertEquals(2, recs1.size());
			System.out.println("Checking skip 1 maxrows 3");
			recs1 = tb1.fetch(group1, "amount", zero, 2, 1);
			System.out.println("Checking result set size 2 vs "+recs1.size());
			assertEquals(2, recs1.size());
			System.out.println("Checking skip 2 maxrows 3");
			recs1 = tb1.fetch(group1, "amount", zero, 3, 2);
			System.out.println("Checking result set size 1 vs "+recs1.size());
			assertEquals(1, recs1.size());

			System.out.println("Closing table "+tb1.name());
			
			tb1.close();

		} catch (Exception xcpt) {

			System.out.println(xcpt.getClass().getName()+" "+xcpt.getMessage());
			System.out.println(com.knowgate.debug.StackTraceUtil.getStackTrace(xcpt));

		} finally {
			if (ds!=null) {
				System.out.println("Dropping table "+ArrayRecord1.tableName);
				ds.dropTable(ArrayRecord1.tableName, false);
			}
		}
		assertFalse(ds.exists(ArrayRecord1.tableName, "U"));
		assertFalse(ds.exists(ArrayRecord2.tableName, "U"));
		System.out.println("End test03Recordset()");
	}

	public void test04IndexedRecordset() throws JDOException, IOException, SecurityException, IllegalStateException, NotSupportedException, SystemException, RollbackException, HeuristicMixedException, HeuristicRollbackException, InstantiationException, IllegalAccessException, UnsupportedOperationException, NoSuchMethodException, IllegalArgumentException, InvocationTargetException {
		System.out.println("Begin test04IndexedRecordset()");

		TableDataSource ds = getTableDataSource();
		ArrayRecord1.dataSource = ds;
		ArrayRecord2.dataSource = ds;
		IndexableView tb1 = null;
		RecordSet<? extends Record> recs1;
		try {
			createTable1(ds);

			createRecords1(ds);
			
			tb1 = ds.openIndexedView(recordClass1.newInstance());
			AbstractQuery qry1 = tb1.newQuery();
			Predicate amount0 = qry1.newPredicate(Connective.AND).add("amount", Operator.LT, new BigDecimal("0"));
			qry1.setFilter(amount0);
			
			recs1 = tb1.fetch(qry1);
			assertEquals(1, recs1.size());
									
			Predicate paulorpeter = qry1.newPredicate(Connective.OR);
			paulorpeter.add("name",Operator.LIKE,"Paul%");
			paulorpeter.add("name",Operator.LIKE,"Peter%");			
			Predicate amount0andpaulorpeter = qry1.newPredicate(Connective.AND).add("amount",Operator.EQ,new BigDecimal("0")).add(paulorpeter);
			qry1.setFilter(amount0andpaulorpeter);
			recs1 = tb1.fetch(qry1);
			assertEquals(2, recs1.size());
			
			recs1 = tb1.fetch(new ArrayRecord1().fetchGroup(), "amount", new BigDecimal(-1), new BigDecimal(0));
			assertEquals(3, recs1.size());
			recs1 = tb1.fetch(new ArrayRecord1().fetchGroup(), "amount", new BigDecimal(2), new BigDecimal(500000));
			assertEquals(1, recs1.size());

			tb1.close();

		} finally {
			if (ds!=null)
				ds.dropTable(ArrayRecord1.tableName, false);
		}
		assertFalse(ds.exists(ArrayRecord1.tableName, "U"));
		System.out.println("End test04IndexedRecordset()");
	}

	public void test05Metadata(String packagePath, String fileName) throws JDOException, IOException {

		JdoPackageMetadata packMeta = new JdoPackageMetadata(packagePath, fileName);
		SchemaMetaData metadata = packMeta.readMetadata(packMeta.openStream());
		
		TableDef xmlDef1 = metadata.getTable(ArrayRecord1.tableName);
		TableDef xmlDef2 = metadata.getTable(ArrayRecord2.tableName);

		TableDataSource ds = getTableDataSource();
		
		TableDef hardwiredDef1 = ArrayRecord1.getTableDef(ds);
		TableDef hardwiredDef2 = ArrayRecord2.getTableDef(ds);
		
		assertEquals(hardwiredDef1.getNumberOfColumns(), xmlDef1.getNumberOfColumns());
		assertEquals(hardwiredDef2.getNumberOfColumns(), xmlDef2.getNumberOfColumns());

		for (ColumnDef hdef : hardwiredDef1.getColumns()) {
			ColumnDef xdef = xmlDef1.getColumnByName(hdef.getName());
			assertEquals(hdef.isPrimaryKey(), xdef.isPrimaryKey());
			assertEquals(hdef.getName(), xdef.getName());
			assertEquals(hdef.getJDBCType(), xdef.getJDBCType());
			assertEquals(hdef.getAllowsNull(), xdef.getAllowsNull());
		}

		for (ColumnDef hdef : hardwiredDef2.getColumns()) {
			ColumnDef xdef = xmlDef2.getColumnByName(hdef.getName());
			assertEquals(hdef.isPrimaryKey(), xdef.isPrimaryKey());
			assertEquals(hdef.getName(), xdef.getName());
			assertEquals(hdef.getJDBCType(), xdef.getJDBCType());
			assertEquals(hdef.getAllowsNull(), xdef.getAllowsNull());
		}

		ds.close();
	}
}
