package org.judal.ramqueue.test;

import com.knowgate.debug.Chronometer;
import org.judal.storage.Env;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Ignore;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Map;

import javax.jdo.JDOException;

import org.judal.storage.EngineFactory;
import org.judal.storage.Param;
import org.judal.storage.Param.Direction;
import org.judal.storage.table.Table;
import org.judal.storage.table.TableDataSource;
import org.judal.ramqueue.RAMQueueProducer;

import static org.junit.Assert.assertTrue;

public class RAMQueueTest {

	private static Map<String,String> properties;
	
	@BeforeClass
	public static void init() throws IOException, ClassNotFoundException {
		InputStream inStrm = RAMQueueTest.class.getResourceAsStream("datasource.properties");
		properties = Env.getDataSourceProperties(inStrm, "test");
		Class.forName(properties.get("driver"));
		EngineFactory.registerEngine("JDBC", "org.judal.jdbc.JDBCEngine");
	}

	public void createTable1(TableDataSource ds) throws JDOException {
		System.out.println("Creating table "+ArrayRecord1.getTableDef(ds).getName());
		ds.createTable(ArrayRecord1.getTableDef(ds), null);
	}

	public ArrayRecord1[] records() {
		ArrayRecord1 rec1, rec2, rec3;

		rec1 = new ArrayRecord1();
		rec1.setId(new Integer(1));
		rec1.setCreated(System.currentTimeMillis());
		rec1.setName("John Smith");
		rec1.setLocation("Here");
		rec1.setImage(new byte[]{1,2,3,4,5,6,7,8,9});
		rec1.setAmount(new BigDecimal("334267.87"));
		
		rec2 = new ArrayRecord1();
		rec2.setId(new Integer(2));
		rec2.setCreated(System.currentTimeMillis());
		rec2.setName("Paul Browm");
		rec2.setLocation("There");
		rec2.setAmount(new BigDecimal("0"));
		
		rec3 = new ArrayRecord1();
		rec3.setId(new Integer(3));
		rec3.setCreated(System.currentTimeMillis());
		rec3.setName("Peter Scott");
		rec3.setLocation("Overthere");
		rec3.setAmount(new BigDecimal("0"));

		return new ArrayRecord1[]{rec1,rec2,rec3};
	}

	@Ignore
	public void test01RamQueueStore() throws JDOException, IllegalStateException, InstantiationException, IllegalAccessException, InterruptedException {

		TableDataSource dts = (TableDataSource) EngineFactory.getEngine("JDBC").getDataSource(properties);
		createTable1(dts);
		dts.close();
		
		RAMQueueProducer producer = new RAMQueueProducer("JDBC", properties);
		
		producer.store(records());
		
		Chronometer c = new Chronometer();
		c.start();

		dts = (TableDataSource) EngineFactory.getEngine("JDBC").getDataSource(properties);
		Table tbl = dts.openTable(new ArrayRecord1());
		while (!tbl.exists(new Integer(1)) || !tbl.exists(new Integer(2)) || !tbl.exists(new Integer(3))) {
			Thread.sleep(500);
			assertTrue(c.elapsed()<3000l);
		}
		tbl.close();
		
		dts.dropTable(ArrayRecord1.tableName, false);
		dts.close();
		
		producer.close();		
	}

	@Ignore
	public void test01RamQueueInsert() throws JDOException, IllegalStateException, InstantiationException, IllegalAccessException, InterruptedException {
		TableDataSource dts = (TableDataSource) EngineFactory.getEngine("JDBC").getDataSource(properties);
		createTable1(dts);
		dts.close();
		
		RAMQueueProducer producer = new RAMQueueProducer("JDBC", properties);
		
		ArrayRecord1 rec1 = new ArrayRecord1();
		
		Param params1[] = new Param[] {
				new Param("id", 1, 1),
				new Param("created", 2, new Timestamp(System.currentTimeMillis())),
				new Param("name", 3, "John Smith"),
				new Param("description", 4, ""),
				new Param("location", 5, "Here"),
				new Param("image", Types.LONGVARBINARY, 6, Direction.IN, null),
				new Param("amount", Types.DECIMAL, 7, new BigDecimal("334267.87"))
		};

		ArrayRecord1 rec2 = new ArrayRecord1();
		
		Param params2[] = new Param[] {
				new Param("id", 1, 2),
				new Param("created", 2, new Timestamp(System.currentTimeMillis())),
				new Param("name", 3, "Mark Brown"),
				new Param("description", 4, "..."),
				new Param("location", 5, "There"),
				new Param("image", Types.LONGVARBINARY, 6, Direction.IN, null),
				new Param("amount", Types.DECIMAL, 7, new BigDecimal("8215.88"))
		};

		producer.insert(rec1, params1);
		producer.insert(rec2, params2);
		
		Chronometer c = new Chronometer();
		c.start();

		dts = (TableDataSource) EngineFactory.getEngine("JDBC").getDataSource(properties);
		Table tbl = dts.openTable(new ArrayRecord1());
		while (!tbl.exists(new Integer(1)) || !tbl.exists(new Integer(2))) {
			Thread.sleep(500);
			assertTrue(c.elapsed()<3000l);
		}
		tbl.close();
		dts.dropTable(ArrayRecord1.tableName, false);
		dts.close();
		
		producer.close();		
	}
	
}
