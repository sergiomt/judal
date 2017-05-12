package org.judal.jdbc.test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.jdo.JDOException;
import javax.jdo.JDOUserException;

import org.judal.jdbc.JDBCEngine;
import org.judal.jdbc.JDBCTableDataSource;
import org.judal.jdbc.RDBMS;
import org.judal.storage.java.ArrayRecord;
import org.judal.storage.java.MapRecord;
import org.judal.storage.java.postgresql.PostgreSQLFieldHelper;
import org.judal.storage.table.Table;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Ignore;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;

public class TestFieldHelper {

	public TestFieldHelper() {
	}

	private static Map<String,String> properties;
	private static JDBCTableDataSource dts;

	@BeforeClass
	public static void init() throws ClassNotFoundException, JDOException, IOException, SQLException {
		properties = new TestJDBC().getTestProperties();
		JDBCEngine jdbc = new JDBCEngine();
		dts = jdbc.getDataSource(properties);
		if (dts.getDatabaseProductId().equals(RDBMS.POSTGRESQL)) {
			try {
				dts.execute("CREATE EXTENSION hstore");
			} catch (JDOException ignore) { }			
			try {
				dts.execute("CREATE TABLE unittest_table_pg (id INTEGER NOT NULL, stor HSTORE NULL, inta INTEGER[] NULL, stra TEXT[] NULL, tmsa TIMESTAMP[] NULL, dati INTERVAL NULL, CONSTRAINT pk_unittest_table_pg PRIMARY KEY (id))");
			} catch (Exception e) {
				System.out.println(e.getClass().getName()+" "+e.getMessage());
			}	
		} else {
			throw new JDOUserException("This test does not support "+dts.getDatabaseProductId()+" RDBMS");
		}
		dts.close();
		// Force metadata re-read after CREATE TABLE
		dts = jdbc.getDataSource(properties);		
	}

	@AfterClass
	public static void cleanup() throws JDOException {
		if (dts!=null) {
			try {
				dts.execute("DROP TABLE unittest_table_pg");
			} catch (JDOException ignore) { }			
			dts.close();
		}
	}
	
	public JDBCTableDataSource getTableDataSource() throws JDOException {
		return (JDBCTableDataSource) dts;
	}

	@Ignore
	public void test01() throws JDOException, SQLException, ClassCastException, ClassNotFoundException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		HashMap<String,String> s1 = new HashMap<>();
		s1.put("name", "John");
		s1.put("surname", "Smith");
		ArrayRecord a1, a2;
		MapRecord m1, m2;
		Table tbl = null;
		if (dts.getDatabaseProductId()==RDBMS.POSTGRESQL) {
			PostgreSQLFieldHelper fh = new PostgreSQLFieldHelper();
			
			a1 = new ArrayRecord(dts, "unittest_table_pg", fh);

			a2 = new ArrayRecord(dts, "unittest_table_pg", fh);
			m1 = new MapRecord(dts, "unittest_table_pg", fh);
			m2 = new MapRecord(dts, "unittest_table_pg", fh);
			
			a1.put("id", 1);
			a1.put("stor", s1);
			a1.put("inta", new Integer[]{1,2,3});
			a1.put("stra", new String[]{"A","B","C","D"});
			a1.put("tmsa", new Date[]{new Date(), new  Date()});
			m1.put("id", 2);
			m1.put("stor", s1);
			m1.put("inta", new Integer[]{4,5,6});
			m1.put("stra", new String[]{"E","F","G","H"});
			m1.put("tmsa", new Date[]{new Date(), new  Date()});

			System.out.println("openTable("+a1.getTableName()+")");
			tbl = dts.openTable(a1);
			
			tbl.store(a1);
			tbl.load(new Integer(1), a2);
			assertEquals(a1.getInt("id"), a2.getInt("id"));
			assertArrayEquals(a1.getIntegerArray("inta"), a2.getIntegerArray("inta"));
			assertArrayEquals(a1.getStringArray("stra"), a2.getStringArray("stra"));
			assertEquals("John", a2.getMap("stor").get("name"));
			assertEquals("Smith", a2.getMap("stor").get("surname"));
			assertEquals(new Date().getYear(), a2.getDateArray("tmsa")[0].getYear());

			tbl.store(m1);
			tbl.load(new Integer(2), m2);
			assertEquals(m1.getInt("id"), m2.getInt("id"));
			assertArrayEquals(m1.getIntegerArray("inta"), m2.getIntegerArray("inta"));
			assertArrayEquals(m1.getStringArray("stra"), m2.getStringArray("stra"));
			assertEquals("John", m2.getMap("stor").get("name"));
			assertEquals("Smith", m2.getMap("stor").get("surname"));
			assertEquals(new Date().getYear(), m2.getDateArray("tmsa")[0].getYear());
			
			System.out.println("close("+a1.getTableName()+")");
			tbl.close();
		}
	}

}
